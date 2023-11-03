import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from typing import Optional

from async_helpers import CAMERAS, Camera, Dispatcher, Sightings
from async_protocol import Parser, Serializer, SocketHandler
from heartbeat import Heartbeat

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="ERROR",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.128.0.2", 9090
sightings = Sightings()


async def handler(reader: StreamReader, writer: StreamWriter):
    client_uuid = str(uuid.uuid4())
    logging.info(
        f"Connected to client @ {writer.get_extra_info('peername')}, referred to as {client_uuid}"
    )
    parser = Parser()
    serializer = Serializer()
    sock_handler = SocketHandler(reader, writer)

    cam_client: Optional[Camera] = None
    disp_client: Optional[Dispatcher] = None
    heartbeat_requested: bool = False

    while 1:
        try:
            msg_code = await parser.parse_message_type(reader)

            if msg_code == 32:  # Plate
                plate, timestamp = await parser.parse_plate_data(reader)
                logging.info(f"Message : Sighting @ {plate}/{timestamp}.")
                if cam_client is None:
                    raise RuntimeError("Client unknown")
                road, mile, speed_limit = cam_client.road, cam_client.mile, cam_client.limit
                await sightings.get_tickets(road, plate, timestamp, mile, speed_limit)
                await sightings.add_sighting(road, plate, timestamp, mile)

            elif msg_code == 64:  # Want Heartbeat
                interval = await parser.parse_wantheartbeat_data(reader)
                logging.info(f"Message : WantHeartBeat @ {interval} deciseconds.")
                if heartbeat_requested:
                    raise RuntimeError("Heartbeat already requested")
                if interval > 0:
                    await Heartbeat(reader, writer, interval / 10).send_heartbeat()
                heartbeat_requested = True

            elif msg_code == 128:
                road, mile, limit = await parser.parse_iamcamera_data(reader)
                logging.info(f"Message : Camera @ {road}/{mile}/{limit}")
                if cam_client is not None:
                    raise RuntimeError("Client has already identified itself")
                cam_client = Camera(road, mile, limit)
                CAMERAS[road].append(cam_client)

            elif msg_code == 129:
                roads = await parser.parse_iamdispatcher_data(reader)
                logging.info(f"Message : Dispatcher @ {roads}")
                if disp_client is not None:
                    raise RuntimeError("Client has already identified itself")
                disp_client = Dispatcher(writer, roads)
                await disp_client.dispatch()

            else:
                raise RuntimeError("Unexpected msg_type")

        except (ConnectionResetError, OSError, asyncio.exceptions.IncompleteReadError) as err:
            logging.error(err)
            await sock_handler.close("Connection Reset by client", client_uuid)
            return
        except RuntimeError as err:
            logging.error(err)
            error_msg = await serializer.serialize_error_data(msg=str(err))
            await sock_handler.write(error_msg.decode())
            await sock_handler.close("Connection Reset by client", client_uuid)
            return


async def main():
    server = await asyncio.start_server(handler, IP, PORT)
    logging.info(f"Started Camera Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")

import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from typing import Optional

from async_helpers import Camera, Dispatcher, Sightings, Heartbeat
from async_protocol import Parser, Serializer, SocketHandler

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.128.0.2", 9090
sightings = Sightings()


async def handler(reader: StreamReader, writer: StreamWriter):
    client_uuid = str(uuid.uuid4()).split("-")[0]
    logging.info(
        f"Connected to client @ {writer.get_extra_info('peername')}, referred to as {client_uuid}"
    )
    parser = Parser()
    serializer = Serializer()
    sock_handler = SocketHandler(reader, writer)

    cam_client: Optional[Camera] = None
    disp_client: Optional[Dispatcher] = None
    heartbeat_requested: bool = False
    client_known: bool = False

    while 1:
        try:
            try:
                msg_code = await parser.parse_message_type(reader)
            except asyncio.exceptions.IncompleteReadError:
                logging.error(f"Connection Reset by client : {client_uuid}")
                await sock_handler.close(client_uuid)
                break

            if msg_code == 32:  # Plate
                plate, timestamp = await parser.parse_plate_data(reader)
                logging.debug(f"Message : {client_uuid} : Sighting @ {plate}/{timestamp}.")
                if cam_client is None:
                    raise RuntimeError("Unknown client")
                road, mile, speed_limit = cam_client.road, cam_client.mile, cam_client.limit
                await sightings.get_tickets(road, plate, timestamp, mile, speed_limit)
                await sightings.add_sighting(road, plate, timestamp, mile)

            elif msg_code == 64:  # Want Heartbeat
                interval = await parser.parse_wantheartbeat_data(reader)
                logging.info(f"Message : {client_uuid} : WantHeartBeat @ {interval} deciseconds.")
                if heartbeat_requested:
                    raise RuntimeError("Heartbeat already requested")
                if interval > 0:
                    heartbeat_requested = True
                    await Heartbeat(reader, writer, interval / 10).send_heartbeat()

            elif msg_code == 128:
                road, mile, limit = await parser.parse_iamcamera_data(reader)
                logging.info(f"Message : {client_uuid} : Camera @ {road}/{mile}/{limit}")
                if client_known:
                    raise RuntimeError("Client has already identified itself")
                cam_client = Camera(road, mile, limit)
                client_known = True

            elif msg_code == 129:
                roads = await parser.parse_iamdispatcher_data(reader)
                logging.info(f"Message : {client_uuid} : Dispatcher @ {roads} @ {client_uuid}")
                if client_known:
                    raise RuntimeError("Client has already identified itself")
                disp_client = Dispatcher(writer, roads)
                client_known = True
                asyncio.create_task(disp_client.dispatch())
                logging.info(f"Started dispatching tickets from Dispatcher : {client_uuid}")

            else:
                raise RuntimeError(f"Unexpected msg_type : {msg_code}")

        except RuntimeError as err:
            logging.error(err)
            error_msg = await serializer.serialize_error_data(msg=str(err))
            await sock_handler.write(error_msg.decode())
            await sock_handler.close(client_uuid)
            return
        except (ConnectionResetError, OSError, asyncio.exceptions.IncompleteReadError) as err:
            logging.error(err)
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

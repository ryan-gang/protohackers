import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from typing import Optional

from async_helpers import CAMERAS, DISPATCHERS, Camera, Dispatcher, Sightings
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
    logging.info(f"Connected to client @ {writer.get_extra_info('peername')}")
    client_uuid = str(uuid.uuid4())
    parser = Parser()
    serializer = Serializer()
    sock_handler = SocketHandler(reader, writer)

    client_type_known = heartbeat_requested = False
    cam_client: Optional[Camera] = None
    disp_client: Optional[Dispatcher] = None
    heartbeat_requested: bool = False
    while 1:
        try:
            try:
                msg_code = await parser.parse_message_type(reader)
            except ConnectionRefusedError:
                logging.error("Client Disconnected.")
                return

            if msg_code == 20:  # Plate
                _ = await parser.parse_plate_data(reader)
                plate, timestamp = _
                if not client_type_known or cam_client is None:
                    raise RuntimeError("Client unknown")
                road, mile, speed_limit = cam_client.road, cam_client.mile, cam_client.limit
                sightings.get_tickets(road, plate, timestamp, mile, speed_limit)
                sightings.add_sighting(road, plate, timestamp, mile)

            elif msg_code == 40:  # Want Heartbeat
                _ = await parser.parse_wantheartbeat_data(reader)
                interval = _
                logging.info(f"Message : WantHeartBeat @ {interval/10} seconds.")
                if heartbeat_requested:
                    raise RuntimeError("Heartbeat already requested")
                if interval > 0:
                    await Heartbeat(reader, writer, interval // 10).send_heartbeat()
                heartbeat_requested = True

            elif msg_code == 80:
                _ = await parser.parse_iamcamera_data(reader)
                road, mile, limit = _
                logging.debug(f"Message : Camera @ {road}/{mile}/{limit}")
                if client_type_known:
                    raise RuntimeError("Client has already identified itself")
                cam_client = Camera(road, mile, limit)
                CAMERAS[road].append(cam_client)
                client_type_known = True

            elif msg_code == 81:
                _ = await parser.parse_iamdispatcher_data(reader)
                roads = _
                logging.debug(f"Message : Dispatcher @ {roads}")
                if client_type_known:
                    raise RuntimeError("Client has already identified itself")
                disp_client = Dispatcher(writer, roads)
                for road in roads:
                    DISPATCHERS[road].append(disp_client)
                client_type_known = True
                await disp_client.dispatch()

            else:
                raise RuntimeError("Unexpected msg_type")
            logging.debug(f"Req : {msg_code} : Data : {_}")

        except (ConnectionResetError, OSError) as err:
            logging.error(err)
            await sock_handler.close("Connection Reset by client", client_uuid)
            return
        except RuntimeError as err:
            logging.error(err)
            err = await serializer.serialize_error_data(msg="Unknown message type")
            await sock_handler.write(err.decode())
            await sock_handler.close("Connection Reset by client", client_uuid)
            return


async def main():
    server = await asyncio.start_server(handler, IP, PORT)
    logging.info(f"Started MITM Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")

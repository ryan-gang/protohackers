import logging
import socket
import sys
import threading
import time
import uuid

from errors import ProtocolError
from heartbeat import heartbeat_deregister_client, heartbeat_register_client, heartbeat_thread
from helpers import CAMERAS, DISPATCHERS, Camera, Dispatcher, Sightings, ticket_dispatcher_thread
from protocol import Parser, Serializer, SocketHandler

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.138.0.2", 9090
parser = Parser()
serializer = Serializer()
sock_handler = SocketHandler()
sightings = Sightings()


def handler(conn: socket.socket, addr: socket.AddressFamily, client_uuid: str):
    logging.info(f"Connected to client @ {addr}")
    client_type_known = heartbeat_requested = False
    cam_client = disp_client = None
    while 1:
        try:
            msg_type, data = sock_handler.read_data(conn)
            logging.debug(f"Req : {msg_type} : {data} as hex : {data.hex()}")

            if msg_type == "20":
                plate, timestamp, _ = parser.parse_plate_data(data)
                if not client_type_known or cam_client is None:
                    raise RuntimeError("Client unknown")
                road, mile = cam_client.road, cam_client.mile
                speed_limit = cam_client.limit
                sightings.get_tickets(road, plate, timestamp, mile, speed_limit)
                sightings.add_sighting(road, plate, timestamp, mile)
            elif msg_type == "40":
                interval, _ = parser.parse_wantheartbeat_data(data)
                logging.info(f"Message : WantHeartBeat @ {interval//10} seconds.")
                if heartbeat_requested:
                    raise RuntimeError("Heartbeat already requested")
                if interval > 0:
                    heartbeat_register_client(client_uuid, conn, interval // 10)
                heartbeat_requested = True

            elif msg_type == "80":
                road, mile, limit, _ = parser.parse_iamcamera_data(data)
                logging.debug(f"Message : Camera @ {road}/{mile}/{limit}")
                if client_type_known:
                    raise RuntimeError("Client has already identified itself")
                cam_client = Camera(conn, road, mile, limit)
                CAMERAS[road].append(cam_client)
                client_type_known = True

            elif msg_type == "81":
                roads, _ = parser.parse_iamdispatcher_data(data)
                logging.debug(f"Message : Dispatcher @ {roads}")
                if client_type_known:
                    raise RuntimeError("Client has already identified itself")
                disp_client = Dispatcher(conn, roads)
                for road in roads:
                    DISPATCHERS[road].append(disp_client)
                client_type_known = True

            else:
                raise RuntimeError("Unknown message type")
        except (ConnectionResetError, OSError) as err:
            logging.error(err)
            heartbeat_deregister_client(client_uuid)
            conn.close()
            return
        except RuntimeError as err:
            logging.error(err)
            heartbeat_deregister_client(client_uuid)
            err = serializer.serialize_error_data(msg="Unknown message type")
            sock_handler.send_data(conn, err)
            conn.close()
            return
        except ProtocolError as err:
            logging.error(err)
            conn.close()
            return
        time.sleep(1)  # Give other threads a chance.


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}")
    threading.Thread(target=heartbeat_thread, daemon=True).start()
    threading.Thread(target=ticket_dispatcher_thread, daemon=True).start()
    while True:
        conn, addr = server_socket.accept()
        # conn.settimeout(10)  # Set timeout for connection to 10 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handler, args=(conn, addr, uuid.uuid4())).start()


if __name__ == "__main__":
    main()

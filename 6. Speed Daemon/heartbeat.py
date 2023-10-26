import logging
import socket
import sys
from threading import Lock
import time

from protocol import Serializer, SocketHandler

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


heartbeat_clients: dict[str, "Heartbeat"] = {}  # client_uuid -> Heartbeat object
heartbeat_clients_lock = Lock()


class Heartbeat(object):
    def __init__(self, conn: socket.socket, interval: int):
        self.serializer = Serializer()
        self.sock_handler = SocketHandler()
        self.msg = self.generate_heartbeat()
        self.conn = conn
        self.interval = interval  # second
        self.elapsed = 0

    def send_heartbeat(self):
        self.sock_handler.send_data(self.conn, self.msg)

    def generate_heartbeat(self) -> bytes:
        return self.serializer.serialize_heartbeat_data()

    def ticktock(self, uuid: str, sleep_interval: float = 0.5):
        # logging.debug(f"Ticktock : {self.elapsed}")
        self.elapsed += sleep_interval
        if self.elapsed == self.interval:
            self.elapsed = 0
            logging.info(f"Sending heartbeat to : {uuid}")
            self.send_heartbeat()


def heartbeat_register_client(client_uuid: str, conn: socket.socket, interval: int):
    # interval is in deciseconds
    logging.info(f"Registering new client : {client_uuid} for Heartbeat.")
    heartbeat_clients[client_uuid] = Heartbeat(conn, interval)


def heartbeat_deregister_client(client_uuid: str):
    logging.info(f"Deregistering client : {client_uuid} from Heartbeat.")
    if client_uuid in heartbeat_clients:
        heartbeat_clients.pop(client_uuid)


def heartbeat_thread():
    sleep_interval = 0.5  # 5 decisecond

    while 1:
        with heartbeat_clients_lock:
            for client in heartbeat_clients:
                # logging.debug(f"Ticking client : {client}")
                heartbeat_clients[client].ticktock(uuid=client)
        time.sleep(sleep_interval)

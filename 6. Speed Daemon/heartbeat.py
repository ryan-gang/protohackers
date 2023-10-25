import logging
import socket
import sys
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


heartbeat_clients: dict[socket.socket, "Heartbeat"] = {}


class Heartbeat(object):
    def __init__(self, client: socket.socket, interval: int):
        self.serializer = Serializer()
        self.sock_handler = SocketHandler()
        self.msg = self.generate_heartbeat().decode()
        self.client = client
        self.interval = interval  # second
        self.elapsed = 0

    def send_heartbeat(self):
        self.sock_handler.send_data(self.client, self.msg)

    def generate_heartbeat(self) -> bytes:
        return self.serializer.serialize_heartbeat_data()

    def ticktock(self, sleep_interval: float = 0.5):
        # logging.debug(f"Ticktock : {self.elapsed}")
        self.elapsed += sleep_interval
        if self.elapsed == self.interval:
            self.elapsed = 0
            self.send_heartbeat()


def heartbeat_register_client(client: socket.socket, interval: int):
    # interval is in deciseconds
    logging.info("Registering new client for Heartbeat.")
    heartbeat_clients[client] = Heartbeat(client, interval)


def heartbeat_deregister_client(client: socket.socket):
    logging.info("Deregistering client from Heartbeat.")
    if client in heartbeat_clients:
        heartbeat_clients.pop(client)


def heartbeat_thread():
    sleep_interval = 0.5  # 5 decisecond

    while 1:
        for client in heartbeat_clients:
            # logging.debug(f"Ticking client : {client}")
            heartbeat_clients[client].ticktock()
            time.sleep(sleep_interval)

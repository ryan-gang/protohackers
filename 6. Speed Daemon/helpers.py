import logging
import socket
import sys

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Camera(object):
    type = "Camera"
    road = None
    mile = None
    limit = None

    def __init__(self, road: int, mile: int, limit: int):
        self.road = road
        self.mile = mile
        self.limit = limit

    def __str__(self):
        return f"Camera@{self.road}-{self.mile}"


class Dispatcher(object):
    type = "Dispatcher"
    num_roads = None
    roads = []

    def __init__(self, num_roads: int, roads: list[int]):
        self.num_roads = num_roads
        self.roads = roads

    def __str__(self):
        return f"Dispatcher@{id(self)}"


def recv_data(conn: socket.socket):
    request, size = "", 32
    while True:
        message = conn.recv(size)
        request += message.decode()
        if not message:
            raise ConnectionResetError("Client Disconnected.")
        if request.endswith("\n"):
            logging.debug(f"Request : {request.strip()}")
            return request


def send_data(conn: socket.socket, response: str):
    try:
        logging.debug(f"Response : {response.strip()}")
        conn.send(response.encode())
        # logging.info(f"Sent {len(response)} bytes.")
    except Exception as E:
        logging.error(E)

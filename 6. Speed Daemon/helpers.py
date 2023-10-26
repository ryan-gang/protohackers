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

    def __init__(self, conn: socket.socket, road: int, mile: int, limit: int):
        self.conn = conn
        self.road = road
        self.mile = mile
        self.limit = limit

    def __str__(self):
        return f"Camera@{self.road}-{self.mile}"


class Dispatcher(object):
    type = "Dispatcher"

    def __init__(self, conn: socket.socket, roads: list[int]):
        self.conn = conn
        self.num_roads = len(roads)
        self.roads = roads

    def __str__(self):
        return f"Dispatcher@{id(self)}"

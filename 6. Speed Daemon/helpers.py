import logging
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

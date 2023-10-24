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

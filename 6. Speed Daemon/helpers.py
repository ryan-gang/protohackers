import bisect
import logging
import socket
import sys
import time
from collections import defaultdict
from threading import Lock

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

CAMERAS: dict[int, list["Camera"]] = defaultdict(list)  # Road -> [Camera]
DISPATCHERS: dict[int, list["Dispatcher"]] = defaultdict(list)  # Road -> [Dispatcher]
SIGHTINGS: dict[int, dict[str, list[tuple[int, int]]]] = defaultdict(
    lambda: defaultdict(list)
)  # Road -> {Plate -> [Time, Mile]}
TICKETS: set["Ticket"] = set()
TICKETS_SERVED = dict[str, list[int]]  # plate -> [day]
tickets_lock = Lock()


parser = Parser()
serializer = Serializer()
sock_handler = SocketHandler()


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

    def dispatch_ticket(self, ticket: "Ticket"):
        p, r, m1, t1, m2, t2, s = (
            ticket.plate,
            ticket.road,
            ticket.mile1,
            ticket.timestamp1,
            ticket.mile2,
            ticket.timestamp2,
            ticket.speed,
        )
        ticket_object = serializer.serialize_ticket_data(p, r, m1, t1, m2, t2, s)
        sock_handler.send_data(self.conn, ticket_object.decode())


class Ticket(object):
    def __init__(
        self,
        plate: str,
        road: int,
        mile1: int,
        timestamp1: int,
        mile2: int,
        timestamp2: int,
        speed: int,
    ) -> None:
        self.plate = plate
        self.road = road
        self.mile1 = mile1
        self.timestamp1 = timestamp1
        self.mile2 = mile2
        self.timestamp2 = timestamp2
        self.speed = speed


class Sightings(object):
    def __init__(self):
        pass

    def __str__(self):
        return f"Dispatcher@{id(self)}"

    def add_sighting(self, road: int, plate: str, timestamp: int, mile: int):
        # Add sighting to datastore only after checking for possible tickets.
        entry = tuple((timestamp, mile))
        bisect.insort(SIGHTINGS[road][plate], entry, key=lambda item: item[0])

    def _get_closest_sightings(
        self, road: int, plate: str, timestamp: int
    ) -> list[tuple[int, int]]:
        idx = bisect.bisect(SIGHTINGS[road][plate], timestamp, key=lambda item: item[0])
        entries: list[tuple[int, int]] = []
        if idx > 0:
            entries.append(SIGHTINGS[road][plate][idx - 1])
        if idx < len(SIGHTINGS[road][plate]) - 1:
            entries.append(SIGHTINGS[road][plate][idx + 1])
        return entries

    def _compute_speed(self, timestamp1: int, mile1: int, timestamp2: int, mile2: int):
        dist = mile2 - mile1  # miles
        time = (timestamp2 - timestamp1) / 60 / 60  # hour
        speed = dist // time  # mph
        return speed

    def get_tickets(self, road: int, plate: str, timestamp: int, mile: int, speed_limit: int):
        entries = self._get_closest_sightings(road, plate, timestamp)
        for sighting in entries:
            _timestamp, _mile = sighting
            if _timestamp < timestamp:
                timestamp1, mile1, timestamp2, mile2 = _timestamp, _mile, timestamp, mile
            else:
                timestamp1, mile1, timestamp2, mile2 = timestamp, mile, _timestamp, _mile

            _speed = self._compute_speed(timestamp1, mile1, timestamp2, mile2)
            if _speed > speed_limit:
                speed = int(_speed * 100)
                tix = Ticket(plate, road, _mile, _timestamp, mile, timestamp, speed)
                TICKETS.add(tix)


def ticket_dispatcher_thread():
    sleep_interval = 2  # 2 seconds

    while 1:
        served: set[Ticket] = set()
        with tickets_lock:
            for ticket in TICKETS:
                road = ticket.road
                if road in DISPATCHERS:
                    dispatcher = DISPATCHERS[road][0]
                    dispatcher.dispatch_ticket(ticket)
                    served.add(ticket)
                    # TICKETS_SERVED[ticket.plate].append(ticket.timestamp1)
                    logging.debug(f"Dispatching ticket : {ticket}")
        for served_ticket in served:
            TICKETS.remove(served_ticket)
        time.sleep(sleep_interval)

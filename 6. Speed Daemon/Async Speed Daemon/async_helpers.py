import asyncio
import bisect
import logging
import sys
from asyncio import StreamReader, StreamWriter
from collections import defaultdict

from async_protocol import Serializer, SocketHandler

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

SIGHTINGS: dict[int, dict[str, list[tuple[int, int]]]] = defaultdict(
    lambda: defaultdict(list)
)  # Road -> {Plate -> [Time, Mile]}
TICKETS: set["Ticket"] = set()
TICKETS_SERVED: dict[str, set[int]] = defaultdict(set)  # plate -> [day]


serializer = Serializer()


class Camera(object):
    type = "Camera"

    def __init__(self, road: int, mile: int, limit: int):
        self.road = road
        self.mile = mile
        self.limit = limit

    def __str__(self):
        return f"Camera@{self.road}-{self.mile}"


class Dispatcher(object):
    type = "Dispatcher"

    def __init__(self, writer: StreamWriter, roads: list[int]):
        self.writer = writer
        self.num_roads = len(roads)
        self.roads = roads

    def __str__(self):
        return f"Dispatcher@{id(self)}"

    async def dispatch_ticket(self, ticket: "Ticket"):
        p, r, m1, t1, m2, t2, s = (
            ticket.plate,
            ticket.road,
            ticket.mile1,
            ticket.timestamp1,
            ticket.mile2,
            ticket.timestamp2,
            ticket.speed,
        )
        ticket_object = await serializer.serialize_ticket_data(p, r, m1, t1, m2, t2, s)
        logging.debug(f"Dispatching ticket : {await ticket.print_ticket()}")
        self.writer.write(ticket_object)
        await self.writer.drain()
        logging.debug(f"Sent {len(ticket_object)} bytes.")
        return

    async def dispatch(self):
        while 1:
            served: set[Ticket] = set()
            for ticket in TICKETS:
                start_day, end_day, road = (
                    await ticket.get_start_day(),
                    await ticket.get_end_day(),
                    ticket.road,
                )
                if (
                    start_day in TICKETS_SERVED[ticket.plate]
                    or end_day in TICKETS_SERVED[ticket.plate]
                ):
                    served.add(ticket)
                    continue
                if road in self.roads:
                    served.add(ticket)
                    TICKETS_SERVED[ticket.plate].add(start_day)
                    TICKETS_SERVED[ticket.plate].add(end_day)
                    await self.dispatch_ticket(ticket)
                    served.add(ticket)

            for ticket in served:
                TICKETS.remove(ticket)

            await asyncio.sleep(0)  # Optimal wait times


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

    async def print_ticket(self):
        return (
            f"Ticket for {self.plate} on {self.road} between {self.mile1} @ {self.timestamp1} and"
            f" {self.mile2} @ {self.timestamp2} on {await self.get_start_day()} &"
            f" {await self.get_end_day()}"
        )

    async def get_start_day(self) -> int:
        return self.timestamp1 // 86400

    async def get_end_day(self) -> int:
        return self.timestamp2 // 86400


class Sightings(object):
    async def add_sighting(self, road: int, plate: str, timestamp: int, mile: int):
        # Add sighting to datastore only after checking for possible tickets.
        entry = tuple((timestamp, mile))
        logging.debug(f"Add sighting for {plate} @ {timestamp} on road {road}:{mile}")
        bisect.insort(SIGHTINGS[road][plate], entry, key=lambda item: item[0])

    async def _get_closest_sightings(
        self, road: int, plate: str, timestamp: int
    ) -> list[tuple[int, int]]:
        logging.debug(f"Looking for closest sightings for {plate},{timestamp} on {road}")
        idx = bisect.bisect(SIGHTINGS[road][plate], timestamp, key=lambda item: item[0])
        entries: list[tuple[int, int]] = []
        arr = SIGHTINGS[road][plate]
        if idx >= 0 and idx < len(arr):
            entries.append(arr[idx])
        if idx > 0:
            entries.append(arr[idx - 1])
        if idx < len(arr) - 1:
            entries.append(arr[idx + 1])
        logging.debug(f"Found sightings : {entries}")
        return entries

    async def _compute_speed(self, timestamp1: int, mile1: int, timestamp2: int, mile2: int):
        dist = mile2 - mile1  # miles
        time = (timestamp2 - timestamp1) / 60 / 60  # hour
        speed = dist / time  # mph
        return abs(round(speed, 2))

    async def get_tickets(self, road: int, plate: str, timestamp: int, mile: int, speed_limit: int):
        entries = await self._get_closest_sightings(road, plate, timestamp)
        for sighting in entries:
            _timestamp, _mile = sighting
            if _timestamp < timestamp:
                timestamp1, mile1, timestamp2, mile2 = _timestamp, _mile, timestamp, mile
            else:
                timestamp1, mile1, timestamp2, mile2 = timestamp, mile, _timestamp, _mile

            _speed = await self._compute_speed(timestamp1, mile1, timestamp2, mile2)
            if _speed > speed_limit:
                speed = int(_speed * 100)

                tix = Ticket(plate, road, mile1, timestamp1, mile2, timestamp2, speed)
                logging.debug(f"New potential ticket found : {await tix.print_ticket()}")
                TICKETS.add(tix)


class Heartbeat(object):
    def __init__(self, reader: StreamReader, writer: StreamWriter, interval: float):
        self.sock_handler = SocketHandler(reader, writer)
        self.interval = interval  # second
        self.elapsed = 0

    async def heartbeat_task(self):
        msg = await serializer.serialize_heartbeat_data()
        while not self.sock_handler.reader.at_eof():
            await asyncio.sleep(self.interval)
            try:
                await self.sock_handler.write(msg.decode("utf-8"), log=False)
            except (RuntimeError, ConnectionResetError):
                await self.sock_handler.close("Heartbeat client")
                return

    async def send_heartbeat(self) -> None:
        if self.interval:
            asyncio.create_task(self.heartbeat_task())

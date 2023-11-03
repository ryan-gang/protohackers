import asyncio
import logging
import sys
from asyncio import StreamReader, StreamWriter

from async_protocol import Serializer, SocketHandler

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Heartbeat(object):
    def __init__(self, reader: StreamReader, writer: StreamWriter, interval: float):
        self.serializer = Serializer()
        self.sock_handler = SocketHandler(reader, writer)
        self.interval = interval  # second
        self.elapsed = 0

    async def send_heartbeat(self):
        msg = await self.serializer.serialize_heartbeat_data()
        while 1:
            await self.sock_handler.write(msg.decode("utf-8"))
            await asyncio.sleep(self.interval)

import logging
import sys
from asyncio import StreamReader, StreamWriter
import json
from typing import Any

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Job(object):
    def __init__(
        self, id: int, job_data: str, priority: int, queue: str, status: int
    ) -> None:
        self.id = id
        self.job_data = job_data
        self.priority = priority
        self.queue = queue
        self.status = status

    def __repr__(self) -> str:
        return (
            f"Job details : \nid : {self.id}\njob : {self.job_data}\npriority :"
            f" {self.priority}\nqueue : {self.queue}\nstatus : {self.status}\n"
        )


class Reader(object):
    def __init__(self, reader: StreamReader) -> None:
        self.reader = reader

    async def readline(self) -> str:
        data = await self.reader.readuntil(separator=b"\n")
        if not data:
            raise RuntimeError("Connection closed by client")
        decoded = data.decode("utf-8").strip()
        return decoded

    async def read(self) -> str:
        line = bytearray()
        while True:
            byte = await self.reader.readexactly(1)
            if byte == b"":
                raise RuntimeError("Connection closed by client")
            line.extend(byte)
            if byte == b"\n":
                break
        decoded = line.decode("utf-8").strip()
        return decoded


class Writer(object):
    def __init__(self, writer: StreamWriter) -> None:
        self.writer = writer
        self.byte_counter = 0

    async def writeline(self, data: str, client: str):
        data = data + "\n"
        out = data.encode("utf-8")
        self.writer.write(out)
        logging.debug(f"Sent {out.hex()} : {len(data)} bytes to {client}")
        await self.writer.drain()
        return

    async def close(self, client_uuid: str):
        self.writer.write_eof()
        self.writer.close()
        logging.debug(f"Closed connection to client @ {client_uuid}.")
        return


class Identifier(object):
    def __init__(self) -> None:
        self.id = 0

    async def get_new(self) -> int:
        self.id += 1
        return self.id


def parse_request(data: str) -> dict[str, Any]:
    """
    Convert json request to python dict, and check for its validity.
    """
    json_decoding_success = False
    try:
        req = json.loads(data)
        json_decoding_success = True
    except json.JSONDecodeError:
        raise RuntimeError("JSON Decode Error")

    # Check if request is valid
    request_types = ["put", "get", "delete", "abort"]
    c1 = "request" in req
    try:
        c2 = req["request"] in request_types
    except KeyError:
        c2 = False
    c3 = json_decoding_success
    valid = c1 and c2 and c3

    if not valid:
        raise RuntimeError("Invalid request received")
    return req

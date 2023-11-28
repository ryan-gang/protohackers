import sys
import logging
from asyncio import StreamReader, StreamWriter

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Reader(object):
    def __init__(self, reader: StreamReader) -> None:
        self.reader = reader

    async def readline(self) -> str:
        data = await self.reader.readuntil(separator=b"\n")
        if not data:
            raise RuntimeError("Connection closed by client")
        decoded = data.decode("utf-8").strip()
        logging.debug(f"<-- {decoded}")
        return decoded

    async def readexactly(self, n: int) -> str:
        data = await self.reader.readexactly(n)
        decoded = data.decode("utf-8")
        logging.debug(f"<-- {decoded}")
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

    async def writeline(self, data: str):
        logging.debug(f"--> {data}")
        if not data.endswith("\n"):
            data = data + "\n"
        out = data.encode("utf-8")
        self.writer.write(out)
        # logging.debug(f"Sent {len(data)} bytes to client.")
        await self.writer.drain()
        return

    async def close(self, client_id: str):
        self.writer.write_eof()
        self.writer.close()
        logging.debug(f"Closed connection to client @ {client_id}.")
        return


class ProtocolError(Exception):
    def __init__(self, message: str | Exception):
        super().__init__(message)


class ValidationError(Exception):
    def __init__(self, message: str | Exception):
        super().__init__(message)

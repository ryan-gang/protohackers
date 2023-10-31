import asyncio
import logging
import re
import sys
from typing import Optional

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

CLIENTS: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}


async def read(reader: asyncio.StreamReader):
    data = await reader.readline()
    logging.debug(f"Request : {data}")
    return data.decode("utf-8").strip()


async def write(writer: asyncio.StreamWriter, data: str):
    if not data.endswith("\n"):
        data += "\n"
    writer.write(data.encode())
    logging.debug(f"Response : {data.strip()}")
    logging.debug(f"Sent {len(data)} bytes.")
    return await writer.drain()


async def close(writer: asyncio.StreamWriter, error_msg: str, conn: str):
    await write(writer, error_msg)
    writer.write_eof()
    writer.close()
    logging.info(f"Closed connection to client @ {conn}.")


async def broadcast(msg: str, excluded: Optional[str]):
    for name in CLIENTS:
        if name == excluded:
            continue
        _, writer = CLIENTS[name]
        await write(writer, msg)


def valid_name(name: str) -> bool:
    match = re.fullmatch("[A-Za-z0-9]{1,}", name)
    return match is not None

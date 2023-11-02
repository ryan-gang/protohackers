import asyncio
import logging
import re
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

CLIENTS: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}


async def read(reader: asyncio.StreamReader):
    data = await reader.readline()
    if not data.endswith(b"\n"):
        return ""
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
    # await write(writer, error_msg)
    writer.write_eof()
    writer.close()
    logging.info(f"Closed connection to client @ {conn}.")


async def rewrite_address(data: str) -> str:
    ADDRESS = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
    pattern = r"(?=(?: |^)(7[A-Za-z0-9]{25,34})(?: |$))"

    matches = re.findall(pattern, data)
    for match in matches:
        if match:
            data = data.replace(match, ADDRESS)
    return data

import logging
import re
import sys
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
from hashlib import md5
from string import ascii_letters, digits, punctuation, whitespace
from typing import List

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

DATASTORE: dict[str, List[str]] = defaultdict(list)
# The list index is the revision number.
# Starting from r1.
DIRS: dict[str, set[str]] = defaultdict(set)
# For every directory, store all of its direct children only. (Only empty directories)
FILES: dict[str, set[str]] = defaultdict(set)
# For every directory, store all of its direct children only. (Only leaf nodes that contain data)
VALID_FILE_NAME_PATTERN = "^/[a-zA-Z0-9./_-]{1,}$"


class Reader(object):
    def __init__(self, reader: StreamReader) -> None:
        self.reader = reader

    async def readline(self) -> str:
        data = await self.reader.readuntil(separator=b"\n")
        logging.info(f"<-- {data}")
        if not data:
            raise RuntimeError("Connection closed by client")
        decoded = data.decode("utf-8").strip()
        return decoded

    async def readexactly(self, n: int) -> bytes:
        data = await self.reader.readexactly(n)
        logging.debug(f"<-- {data}")
        return data

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
        logging.info(f"--> {data}")
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


# Errors
class ProtocolError(Exception):
    def __init__(self, message: str | Exception):
        super().__init__(message)


class ValidationError(Exception):
    def __init__(self, message: str | Exception):
        super().__init__(message)


# Helpers
def parse_child_parent_relationships(file_path: str):
    """
    For every file in the vcs, create a list of relationships from the parent
    directory to its children.
    """
    parts = file_path.split("/")
    leaf = len(parts) - 2
    for idx, _ in enumerate(parts[:-1]):
        parent = "/".join(parts[: idx + 1])
        child = parts[idx + 1]
        if not parent.startswith("/"):
            parent = "/" + parent

        if idx != leaf:
            child += "/"
            DIRS[parent].add(child)
        else:
            FILES[parent].add(child)


def validate_file_name(file_path: str):
    if file_path == "/":
        return
    if (
        not file_path.startswith("/")
        or file_path.endswith("/")
        or not re.fullmatch(VALID_FILE_NAME_PATTERN, file_path)
        or file_path.count("//") > 0
    ):
        raise ValidationError("ERR illegal file name")


def validate_data(data: bytes):
    for ordinal in data:
        if chr(ordinal) not in ascii_letters + digits + punctuation + whitespace:
            raise ValidationError("ERR illegal file name")


def get_revision(msg_parts: list[str]) -> int:
    file_path = msg_parts[1]
    revision = "0"
    if len(msg_parts) == 3:
        revision = msg_parts[2][1:]
        if not revision.isdigit() or int(revision) < 1 or int(revision) > len(DATASTORE[file_path]):
            raise ValidationError("ERR no such revision")
    return int(revision)


# Core methods
async def list(writer: Writer, msg_parts: list[str]):
    if len(msg_parts) != 2:
        raise ValidationError("ERR usage: LIST dir")
    path = msg_parts[1]
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    validate_file_name(path)

    dirs, files = DIRS[path], FILES[path]
    ls: List[str] = []
    seen: set[str] = set()

    for file in files:
        full_path = path + "/" + file
        revision = len(DATASTORE[full_path]) + 1
        ls.append(f"{file} r{revision}")
        seen.add(file)
    for dir in dirs:
        if dir not in seen and dir[:-1] not in seen:
            ls.append(f"{dir} DIR")

    ls.sort()
    ls_all = "\n".join(ls)
    out = f"OK {len(ls)}\n{ls_all}"
    await writer.writeline(out)


async def get(writer: Writer, msg_parts: List[str]):
    if len(msg_parts) not in [2, 3]:
        raise ValidationError("ERR usage: PUT file length newline data")
    file_path = msg_parts[1]
    validate_file_name(file_path)
    if file_path not in DATASTORE:
        raise ProtocolError("ERR no such file")
    else:
        revision = get_revision(msg_parts)
        data = DATASTORE[file_path][revision - 1]
        resp = f"OK {len(data)}"
        await writer.writeline(resp)
        await writer.writeline(data)


async def put(writer: Writer, reader: Reader, msg_parts: List[str]):
    if len(msg_parts) != 3:
        raise ValidationError("ERR usage: PUT file length newline data")
    file_path, length = msg_parts[1], msg_parts[2]
    validate_file_name(file_path)
    data = await reader.readexactly(n=int(length))
    validate_data(data)

    data = data.decode("utf-8")
    data_hash, prev_data_hash = md5(data.encode()).hexdigest(), ""
    if len(DATASTORE[file_path]) > 0:
        prev_data = DATASTORE[file_path][-1]
        prev_data_hash = md5(prev_data.encode()).hexdigest()
    if data_hash != prev_data_hash:
        DATASTORE[file_path].append(data)
        parse_child_parent_relationships(file_path)
    resp = f"OK r{len(DATASTORE[file_path])}"
    await writer.writeline(resp)

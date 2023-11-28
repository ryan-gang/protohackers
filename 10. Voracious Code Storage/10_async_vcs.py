import asyncio
from collections import defaultdict
from hashlib import md5
import logging
import sys
from asyncio import StreamReader, StreamWriter

from async_helpers import ProtocolError, Reader, ValidationError, Writer

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)
IP, PORT = "0.0.0.0", 9090

DATASTORE: dict[str, list[str]] = defaultdict(list)
# The list index is the revision number.
# Starting from r1.
DIRS: dict[str, set[str]] = defaultdict(set)
# For every directory, store all of its direct children only. (Only empty directories)
FILES: dict[str, set[str]] = defaultdict(set)
# For every directory, store all of its direct children only. (Only leaf nodes that contain data)


def parse_child_parent_relationships(file_path: str):
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


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    peername = stream_writer.get_extra_info("peername")
    logging.info(f"Connected to client @ {peername}")
    writer = Writer(stream_writer)
    reader = Reader(stream_reader)
    while 1:
        try:
            await writer.writeline("READY")
            msg = await reader.readline()
            msg_parts = msg.split(" ")
            msg_type = msg_parts[0].upper()

            match msg_type:
                case "HELP":
                    resp = "OK usage: HELP|GET|PUT|LIST"
                    await writer.writeline(resp)
                case "PUT":
                    file_path, length = msg_parts[1], msg_parts[2]
                    if not file_path.startswith("/") or file_path.endswith("/"):
                        raise ValidationError("ERR illegal file name")
                    data = await reader.readexactly(n=int(length))
                    data_hash = md5(data.encode()).hexdigest()
                    prev_data_hash = ""
                    if len(DATASTORE[file_path]) > 0:
                        prev_data = DATASTORE[file_path][-1]
                        prev_data_hash = md5(prev_data.encode()).hexdigest()
                    if data_hash != prev_data_hash:
                        DATASTORE[file_path].append(data)
                        parse_child_parent_relationships(file_path)
                    resp = f"OK r{len(DATASTORE[file_path])}"
                    await writer.writeline(resp)

                case "GET":
                    file_path, revision = msg_parts[1], 0
                    if len(msg_parts) > 2:
                        revision = int(msg_parts[2][1:])
                    if not file_path.startswith("/") or file_path.endswith("/"):
                        raise ValidationError("ERR illegal file name")
                    if file_path not in DATASTORE:
                        raise ProtocolError("ERR no such file")
                    else:
                        data = DATASTORE[file_path][revision - 1]
                        resp = f"OK {len(data)}"
                        await writer.writeline(resp)
                        await writer.writeline(data)
                case "LIST":
                    path = msg_parts[1]
                    if path != "/" and path.endswith("/"):
                        path = path[:-1]

                    dirs = DIRS[path]
                    files = FILES[path]

                    ls: list[str] = []
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
                case _:
                    err = f"ERR illegal method:{msg_type}"
                    raise ProtocolError(err)
            await asyncio.sleep(0)
        except ProtocolError as err:
            logging.error(err)
            await writer.writeline(str(err))
        except ConnectionResetError:
            logging.error("Client disconnected.")
            await writer.close(peername)
            break
        except asyncio.exceptions.IncompleteReadError:
            logging.error("Client disconnected.")
            break
    return


async def main():
    server = await asyncio.start_server(handler, IP, PORT)
    logging.info(f"Started Camera Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")

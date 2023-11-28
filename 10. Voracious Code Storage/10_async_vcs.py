import asyncio
from collections import defaultdict
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
            msg_type = msg_parts[0]

            match msg_type:
                case "HELP":
                    resp = "OK usage: HELP|GET|PUT|LIST"
                    await writer.writeline(resp)
                case "PUT":
                    file_path, length = msg_parts[1], msg_parts[2]
                    if not file_path.startswith("/") or file_path.endswith("/"):
                        raise ValidationError("ERR illegal file name")
                    data = await reader.readexactly(n=int(length))
                    DATASTORE[file_path].append(data)
                    resp = f"OK r{len(DATASTORE[file_path])}"
                    await writer.writeline(resp)
                case "GET":
                    file_path = msg_parts[1]
                    if len(msg_parts) > 2:
                        revision = int(msg_parts[2][1:])
                    else:
                        revision = 1
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
                    pass
                case _:
                    err = f"ERR illegal method:{msg_type}"
                    raise ProtocolError(err)
            await asyncio.sleep(0)
        except ProtocolError as err:
            logging.error(err)
            await writer.writeline(str(err))
        except (asyncio.exceptions.IncompleteReadError, ConnectionResetError):
            logging.error("Client disconnected.")
            await writer.close(peername)
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

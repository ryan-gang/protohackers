import asyncio
import logging
import sys
from asyncio import StreamReader, StreamWriter

from async_helpers import ProtocolError, Reader, ValidationError, Writer, get, list, put

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)
IP, PORT = "0.0.0.0", 9090


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    peername = stream_writer.get_extra_info("peername")
    logging.debug(f"Connected to client @ {peername}")
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
                    await put(writer, reader, msg_parts)

                case "GET":
                    await get(writer, msg_parts)

                case "LIST":
                    await list(writer, msg_parts)

                case _:
                    err = f"ERR illegal method:{msg_type}"
                    raise ProtocolError(err)
            await asyncio.sleep(0)
        except (ProtocolError, ValidationError) as err:
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
    logging.info(f"Started VCS Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")

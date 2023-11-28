import asyncio
import logging
import sys
from asyncio import StreamReader, StreamWriter

from async_helpers import ProtocolError, Writer

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


async def handler(reader: StreamReader, stream_writer: StreamWriter):
    logging.info(f"Connected to client @ {stream_writer.get_extra_info('peername')}")
    writer = Writer(stream_writer)

    while 1:
        try:
            await writer.writeline("READY")
            request = await reader.readline()
            logging.debug(f"Req : {request}")
            msg = request.decode()
            msg_parts = msg.split(" ")
            msg_type = msg_parts[0]

            match msg_type:
                case "HELP":
                    resp = "OK usage: HELP|GET|PUT|LIST"
                    await writer.writeline(resp)
                case _:
                    err = f"ERR illegal method:{msg_type}"
                    raise ProtocolError(err)

            await asyncio.sleep(0)

        except ProtocolError as err:
            logging.error(err)
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

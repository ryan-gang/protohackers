import asyncio
import logging
import sys

from async_helpers import close, read, write, rewrite_address

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)
IP, PORT = "10.154.0.3", 9090
UPSTREAM_IP, UPSTREAM_PORT = "chat.protohackers.com", 16963


async def unidirectional_handler(
    up_reader: asyncio.StreamReader,
    up_writer: asyncio.StreamWriter,
    down_reader: asyncio.StreamReader,
    down_writer: asyncio.StreamWriter,
):
    up, down = up_writer.get_extra_info("peername"), down_writer.get_extra_info("peername")
    logging.info(f"Connected to downstream @ {down} and upstream @ {up}")

    while 1:
        data = await read(up_reader)
        if not data:
            raise ConnectionRefusedError(f"Connection closed by client @ {down}.")
        new_data = await rewrite_address(data)
        await write(down_writer, new_data)


async def bidirectional_handler(
    down_reader: asyncio.StreamReader,
    down_writer: asyncio.StreamWriter,
):
    # connect to upstream
    up_reader, up_writer = await asyncio.open_connection(UPSTREAM_IP, UPSTREAM_PORT)
    up, down = up_writer.get_extra_info("peername"), down_writer.get_extra_info("peername")
    try:
        await asyncio.gather(
            unidirectional_handler(up_reader, up_writer, down_reader, down_writer),
            unidirectional_handler(down_reader, down_writer, up_reader, up_writer),
        )
    except (ConnectionRefusedError, ConnectionResetError) as exc:
        logging.error(exc)
        try:
            await close(up_writer, "Connection closed by client", up)
            await close(down_writer, "Connection closed by client", down)
        except ConnectionResetError as exc:
            logging.error(exc)
        return


async def main():
    server = await asyncio.start_server(bidirectional_handler, IP, PORT)
    logging.info(f"Started MITM Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")

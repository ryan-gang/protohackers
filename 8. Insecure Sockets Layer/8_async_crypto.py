import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter

from async_helpers import Crypto, Reader, Writer, prioritise

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.128.0.2", 9090


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    client_uuid = str(uuid.uuid4()).split("-")[0]
    logging.info(
        f"Connected to client @ {stream_writer.get_extra_info('peername')}, referred to as"
        f" {client_uuid}"
    )
    cipher_spec = await stream_reader.readuntil(separator=int.to_bytes(0))
    crypto = Crypto(cipher_spec)
    logging.info(f"Cipher spec : {cipher_spec.hex()}")
    reader = Reader(stream_reader, crypto)
    writer = Writer(stream_writer, crypto)

    try:
        if crypto.no_op_cipher():
            raise RuntimeError("No-op cipher received")
        while 1:
            data = await reader.readline()
            logging.debug(f"Req : {data}")

            output = await prioritise(data)
            logging.info(f"Res : {output}")
            await writer.writeline(output, client_uuid)
            await asyncio.sleep(0)
            # Wait before the next iteration, or code gets stuck here, none of
            # the other clients are served. sleep(0) waits for the optimal
            # wait-time.
    except (asyncio.exceptions.IncompleteReadError, RuntimeError) as E:
        logging.error(E)
        await writer.close(client_uuid)
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

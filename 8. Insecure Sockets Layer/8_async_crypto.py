import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter

from async_helpers import Crypto, prioritise, readline

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.128.0.2", 9090


async def handler(reader: StreamReader, writer: StreamWriter):
    client_uuid = str(uuid.uuid4()).split("-")[0]
    logging.info(
        f"Connected to client @ {writer.get_extra_info('peername')}, referred to as {client_uuid}"
    )
    cipher_spec = await reader.readuntil(separator=int.to_bytes(0))
    crypto = Crypto(cipher_spec)
    logging.info(f"Cipher spec : {cipher_spec.hex()}")

    encode_byte_counter = decode_byte_counter = 0
    while 1:
        encoded_data = await readline(reader)
        if encoded_data == b"" or await crypto.check_for_no_op_cipher():
            writer.write_eof()
            writer.close()
            logging.debug(f"Closed connection to client @ {client_uuid}.")
            return

        logging.info(f"Encoded req : {encoded_data.hex()}")
        req, byte_counter = await crypto.decode(bytearray(encoded_data), encode_byte_counter)
        encode_byte_counter += byte_counter
        data = req.decode("utf-8").strip()
        logging.info(f"Decoded req : {data}")

        output = await prioritise(data)
        logging.info(f"Decoded res : {output}")
        output = bytearray(output.encode("utf-8"))
        response, byte_counter = await crypto.encode(output, decode_byte_counter)
        logging.info(f"Encoded res : {response.hex()}")
        decode_byte_counter += byte_counter

        writer.write(response)
        await writer.drain()
        logging.debug(f"Sent {len(response)} bytes to {client_uuid}")
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

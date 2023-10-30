import asyncio
import logging
import struct
import sys

from helpers import PriceAnalyzer

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


class PricesServerProtocol(asyncio.Protocol):
    def __init__(self) -> None:
        self.requests: bytearray = bytearray()
        self.analyzer = PriceAnalyzer()
        super().__init__()
        self.in_format, self.out_format = ">cii", ">i"

    def connection_made(self, transport: asyncio.BaseTransport):
        self.peer = transport.get_extra_info("peername")
        logging.info(f"Connected to client @ {self.peer}")
        self.transport = transport

    def data_received(self, data: bytes):
        logging.info(f"Received: {len(data)} bytes of data from {self.peer}.")
        self.requests.extend(data)
        if len(self.requests) % 9 == 0:
            for start in range(0, len(self.requests), 9):
                request = self.requests[start : start + 9]
                mode, arg_1, arg_2 = struct.unpack(self.in_format, request)
                # Might not be utf-8 decoding will fail, hence compare bytes.
                if mode == b"I":
                    seconds, price = arg_1, arg_2
                    self.analyzer.append_row(seconds, price)
                elif mode == b"Q":
                    start_time, end_time = arg_1, arg_2
                    mean_price = self.analyzer.get_mean(start_time, end_time)
                    response = struct.pack(self.out_format, mean_price)
                    logging.debug(f"Request : {request}")
                    logging.debug(f"Response : {response}")
                    self.transport.write(response)  # type: ignore
                    logging.info(f"Sent : {len(response)} bytes of data to {self.peer}.")
                else:
                    logging.debug(f"Unknown Mode passed : {mode}")
                    self.transport.write(b"")  # type: ignore
                    logging.info(f"Sent : 0 bytes of data to {self.peer}.")
            self.requests = bytearray()

    def eof_received(self):
        logging.info(f"Closed connection to client @ {self.peer}")
        self.transport.close()


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: PricesServerProtocol(), IP, PORT)
    logging.info(f"Started Echo Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


asyncio.run(main())

import asyncio
import logging
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
IP, PORT = "10.154.0.3", 9090


class EchoServerProtocol(asyncio.Protocol):
    peer: str
    transport: asyncio.BaseTransport

    def connection_made(self, transport: asyncio.BaseTransport):
        self.peer = transport.get_extra_info("peername")
        logging.info(f"Connected to client @ {self.peer}")
        self.transport = transport

    def data_received(self, data: bytes):
        logging.info(f"Received: {len(data)} bytes of data from {self.peer}.")
        self.transport.write(data)  # type: ignore
        logging.info(f"Sent : {len(data)} bytes of data to {self.peer}.")

    def eof_received(self):
        logging.info(f"Closed connection to client @ {self.peer}")
        self.transport.close()


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: EchoServerProtocol(), IP, PORT)
    logging.info(f"Started Echo Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


asyncio.run(main())

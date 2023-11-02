import asyncio
import logging
import sys
from collections import defaultdict

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
DATASTORE: dict[str, str] = defaultdict(str)
DATASTORE["version"] = "Ryan's AsyncKVStorev1"


def handler(request: bytes):
    data, response = request.decode(), None
    if "=" in data:  # Insert
        key, *_ = data.split("=")
        value = "=".join(_)
        if key != "version":  # Don't allow updates for the `version` key
            DATASTORE[key] = value
    else:  # Retrieve
        key = data
        value = DATASTORE[key]
        response = f"{key}={value}"
    return response


class KVStoreServerProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        super().__init__()

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]):
        logging.debug(f"Received: {len(data)} bytes of data from {addr}.")
        logging.debug(f"Request : {data}")
        response = handler(data)
        logging.debug(f"Response : {response}")
        if response:
            self.transport.sendto(response.encode(), addr)


async def main():
    loop = asyncio.get_running_loop()

    transport, _protocol = await loop.create_datagram_endpoint(
        lambda: KVStoreServerProtocol(), (IP, PORT)
    )

    logging.info(f"Started KV Server @ {IP}:{PORT}")

    try:
        await asyncio.sleep(3600)
    except asyncio.exceptions.CancelledError:
        logging.critical("Interrupted, shutting down.")
    finally:
        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")

import asyncio
import logging
import sys

from errors import ProtocolError

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

Clients: dict[str, "LRCPServerHandler"] = {}
# For every client we create a new Handler object.


class LRCPServerHandler:
    def __init__(self, lrcp_server_protocol: "LRCPServerProtocol", addr: tuple[str, int]) -> None:
        self.data = ""
        self.sent_data_archive = ""
        self.received_chars = 0
        self.sent_chars = 0
        self.connected = False
        self.session_id = -1
        self.ack_lengths: list[int] = []
        self.prev_ack = ""
        self.server_protocol = lrcp_server_protocol
        self.addr = addr
        self.remaining = ""
        self.req_id = -1

    async def send(self, response: str, addr: tuple[str, int], req_id: int):
        while True:
            if req_id == self.req_id:
                await self.server_protocol.send_datagram(response, addr)
                await asyncio.sleep(3)
            break

    async def handle(self, data: str):
        msg_parts = data.split("/")
        msg_type, session_id = msg_parts[1], msg_parts[2]

        if msg_type == "connect":
            self.connected = True
            self.session_id = session_id
            self.req_id = 0
            await self.server_protocol.send_datagram(f"/ack/{self.session_id}/0/", self.addr)

        elif msg_type == "data":
            pos, data = int(msg_parts[3]), msg_parts[4]
            assert pos >= 0, ProtocolError("pos can't be negative")
            if not self.connected:
                self.req_id += 1
                await self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)
            elif pos < self.received_chars:
                self.req_id += 1
                await self.server_protocol.send_datagram(self.prev_ack, self.addr)
            else:
                self.req_id += 1
                req_id = self.req_id
                self.data = self.data[:pos]
                self.data += data
                self.received_chars = pos + len(data)
                curr_ack = f"/ack/{session_id}/{self.received_chars}/"
                self.prev_ack = curr_ack
                # Retransmit.
                await asyncio.create_task(self.send(curr_ack, self.addr, req_id))
                # await self.server_protocol.send_datagram(curr_ack, self.addr)
                if self.remaining:
                    position = pos - len(self.remaining)
                    self.remaining = ""
                else:
                    position = pos
                reversed_data, self.remaining = reverse(self.data[position:])
                # Retransmit.
                output = f"/data/{self.session_id}/{self.sent_chars}/{reversed_data}/"
                self.sent_data_archive += reversed_data
                self.sent_chars += len(reversed_data)
                self.data = self.remaining
                await asyncio.create_task(self.send(curr_ack, self.addr, req_id))
                # await self.server_protocol.send_datagram(output, self.addr)

        elif msg_type == "ack":
            length = int(msg_parts[3])
            if session_id != self.session_id:
                self.req_id += 1
                await self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)
            if self.ack_lengths and length <= max(self.ack_lengths):
                return
            if length > self.sent_chars:
                raise ProtocolError("Client misbehaving")
            if length < self.sent_chars:
                self.req_id += 1
                output = (
                    f"/data/{self.session_id}/{self.sent_chars}/{self.sent_data_archive[length:]}/"
                )
                await self.server_protocol.send_datagram(output, self.addr)
                pass
            if length == self.sent_chars:
                self.req_id += 1
                return

        elif msg_type == "close":
            self.req_id += 1
            await self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)


class LRCPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        super().__init__()

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]):
        logging.debug(f"Received: {len(data)} bytes of data from {addr}.")
        logging.debug(f"Request : {data}")
        self.handler(data, addr)

    async def send_datagram(self, response: str, addr: tuple[str, int]):
        logging.debug(f"Response : {response}")
        self.transport.sendto(response.encode(), addr)

    def handler(self, request: bytes, addr: tuple[str, int]):
        data = request.decode()
        session_id = data.split("/")[2]
        if session_id not in Clients:
            Clients[session_id] = LRCPServerHandler(self, addr)
        session_object = Clients[session_id]
        _ = session_object.handle(data)


def reverse(string: str) -> tuple[str, str]:
    out: list[str] = []
    string, sep, remaining = string.rpartition("\n")
    for line in (string + sep).splitlines(keepends=True):
        line = line[:-1]
        out.append(line[::-1] + "\n")

    return "".join(out), remaining


async def main():
    loop = asyncio.get_running_loop()

    transport, _ = await loop.create_datagram_endpoint(lambda: LRCPServerProtocol(), (IP, PORT))

    logging.info(f"Started LRCP Server @ {IP}:{PORT}")

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

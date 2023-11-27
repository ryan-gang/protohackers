import asyncio
import logging
import sys
from typing import TypeAlias

from errors import ProtocolError

Addr: TypeAlias = tuple[str, int]  # (host, port)

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

Sessions: dict[str, "Session"] = {}
# For every client we create a new Handler object.


class Session:
    def __init__(self, lrcp_server_protocol: "LRCPServerProtocol", addr: Addr) -> None:
        self.data = ""
        self.sent_data_archive = ""
        self.read = 0
        self.sent_chars = 0
        self.connected = False
        self.session_id = -1
        self.ack_lengths: list[int] = []
        self.server_protocol = lrcp_server_protocol
        self.addr = addr
        self.processed_upto = 0
        self.last_ack_pos = 0

    async def handle_dropped_acks(self):
        while 1:
            if self.last_ack_pos < self.sent_chars:
                pos = self.sent_chars
                output = f"/data/{self.session_id}/{pos}/{self.sent_data_archive[pos:]}/"
                self.server_protocol.send_datagram(output, self.addr)
            await asyncio.sleep(3)

    def handle(self, data: str):
        msg_parts = data.split("/")
        msg_type, session_id = msg_parts[1], msg_parts[2]

        if msg_type == "connect":
            self.connected = True
            self.session_id = session_id
            self.server_protocol.send_datagram(f"/ack/{self.session_id}/0/", self.addr)
            asyncio.create_task(self.handle_dropped_acks())

        elif msg_type == "data":
            pos, data = int(msg_parts[3]), msg_parts[4]
            unescaped_data = data.replace("\\\\", "\\").replace("\\/", "/")
            assert pos >= 0, ProtocolError("pos can't be negative")
            if not self.connected:
                self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)

            if self.read >= pos:
                curr_read = pos + len(unescaped_data)
                if curr_read > self.read:
                    self.data = self.data[:pos] + unescaped_data
                    self.read = curr_read
                ack = f"/ack/{session_id}/{self.read}/"
                self.server_protocol.send_datagram(ack, self.addr)
                reversed_data, remaining = reverse(self.data[self.processed_upto :])
                self.processed_upto = self.read - len(remaining)
                output = f"/data/{self.session_id}/{self.sent_chars}/{reversed_data}/"
                self.sent_data_archive += reversed_data
                self.sent_chars += len(reversed_data)
                self.server_protocol.send_datagram(output, self.addr)
            else:
                previous_ack = f"/ack/{session_id}/{self.read}/"
                self.server_protocol.send_datagram(previous_ack, self.addr)

        elif msg_type == "ack":
            pos = int(msg_parts[3])
            self.last_ack_pos = max(pos, self.last_ack_pos)
            if session_id != self.session_id:
                self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)
            if self.ack_lengths and pos <= max(self.ack_lengths):
                return
            if pos > self.sent_chars:
                raise ProtocolError("Client misbehaving")
            if pos < self.sent_chars:
                output = f"/data/{self.session_id}/{pos}/{self.sent_data_archive[pos:]}/"
                self.server_protocol.send_datagram(output, self.addr)
            if pos == self.sent_chars:
                return

        elif msg_type == "close":
            self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)


class LRCPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        super().__init__()

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Addr):
        logging.debug(f"Received: {len(data)} bytes of data from {addr}.")
        logging.debug(f"Request : {data}")
        self.handler(data, addr)

    def send_datagram(self, response: str, addr: Addr):
        logging.debug(f"Response : {response}")
        self.transport.sendto(response.encode(), addr)
        self.transport.sendto(response.encode(), addr)

    def handler(self, request: bytes, addr: Addr):
        data = request.decode()
        session_id = data.split("/")[2]
        if session_id not in Sessions:
            Sessions[session_id] = Session(self, addr)
        session_object = Sessions[session_id]
        session_object.handle(data)


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

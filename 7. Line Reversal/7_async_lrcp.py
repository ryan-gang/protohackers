import asyncio
import logging
import sys
from typing import TypeAlias

from errors import ProtocolError, ValidationError

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
        self.closed = False

    async def handle_dropped_acks(self):
        while 1:
            if self.last_ack_pos < self.sent_chars:
                pos = self.last_ack_pos
                output = f"/data/{self.session_id}/{pos}/{self.sent_data_archive[pos:]}/"
                self.server_protocol.send_chunked_datagram(output, self.addr)
            if self.closed:
                return
            await asyncio.sleep(3)

    def handle_connect(self, msg_parts: list[str]):
        session_id = msg_parts[1]
        self.connected = True
        self.session_id = session_id
        self.server_protocol.send_datagram(f"/ack/{self.session_id}/0/", self.addr)
        asyncio.create_task(self.handle_dropped_acks())
        logging.info(f"Created dropped ack handling for : {session_id}")

    def handle_data(self, msg_parts: list[str]):
        session_id, pos, unescaped_data = msg_parts[1], int(msg_parts[2]), msg_parts[3]
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
            escaped_reversed_data = reversed_data.replace("\\", "\\\\").replace("/", "\\/")
            output = f"/data/{self.session_id}/{self.sent_chars}/{escaped_reversed_data}/"
            self.sent_data_archive += reversed_data
            self.sent_chars += len(reversed_data)
            self.server_protocol.send_chunked_datagram(output, self.addr)
        else:
            previous_ack = f"/ack/{session_id}/{self.read}/"
            self.server_protocol.send_datagram(previous_ack, self.addr)

    def handle_ack(self, msg_parts: list[str]):
        session_id, pos = msg_parts[1], int(msg_parts[2])
        self.last_ack_pos = max(pos, self.last_ack_pos)
        if session_id != self.session_id:
            self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)
        if self.ack_lengths and pos <= max(self.ack_lengths):
            return
        if pos > self.sent_chars:
            self.handle_close(msg_parts)
            raise ProtocolError("Client misbehaving")
        if pos < self.sent_chars:
            output = f"/data/{self.session_id}/{pos}/{self.sent_data_archive[pos:]}/"
            self.server_protocol.send_chunked_datagram(output, self.addr)
        if pos == self.sent_chars:
            return

    def handle_close(self, msg_parts: list[str]):
        session_id = msg_parts[1]
        self.server_protocol.send_datagram(f"/close/{session_id}/", self.addr)
        self.closed = True

    def handle(self, data: str):
        msg_parts = data[1:-1].split("/", maxsplit=3)
        msg_type = msg_parts[0]

        if msg_type == "connect":
            self.handle_connect(msg_parts)

        elif msg_type == "data":
            self.handle_data(msg_parts)

        elif msg_type == "ack":
            self.handle_ack(msg_parts)

        elif msg_type == "close":
            self.handle_close(msg_parts)


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

    def send_chunked_datagram(self, data: str, addr: Addr):
        MAX_LEN = 750
        parts = data[1:-1].split("/")
        msg_type, session_id, pos = parts[0], int(parts[1]), int(parts[2])
        data = "/".join(parts[3:])
        n = len(data)
        for i in range(0, n, MAX_LEN):
            chunk = data[i : i + MAX_LEN]
            res = f"/{msg_type}/{session_id}/{pos}/{chunk}/"
            pos += len(chunk)
            self.send_datagram(res, addr)

    def handler(self, request: bytes, addr: Addr):
        data = request.decode()
        unescaped_data = data.replace("\\\\", "\\").replace("\\/", "/")
        try:
            validate_data(data)
            msg_parts = unescaped_data[1:-1].split("/", maxsplit=3)
            session_id = msg_parts[1]
            if session_id not in Sessions:
                Sessions[session_id] = Session(self, addr)
            session_object = Sessions[session_id]
            session_object.handle(unescaped_data)
        except (ValidationError, ProtocolError) as E:
            logging.error(E)


def validate_data(data: str):
    msg_types = {"connect": 2, "data": 4, "ack": 3, "close": 2}
    if not (data[0] == data[-1] == "/"):
        raise ValidationError("Message should start and end with /")
    parts = data.count("/") - data.count("\\/")
    msg_parts = data[1:-1].split("/", maxsplit=3)
    if len(msg_parts) < 2:
        raise ValidationError("Invalid message parts")
    msg_type, session = msg_parts[0], msg_parts[1]
    if msg_type not in ["connect", "data", "ack", "close"]:
        raise ValidationError("Undefined message type")
    if len(msg_parts) != msg_types[msg_type]:
        raise ValidationError("Invalid message received")
    if parts != (exp := msg_types[msg_type] + 1):
        raise ValidationError(f"Message contains too many parts : {parts}, expected : {exp}")
    if len(data) > 1000:
        raise ValidationError("Message can't be longer than 1000 chars")
    if not session.isdigit() or int(session) < 0 or int(session) >= 2**31:
        raise ValidationError("Invalid session")


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

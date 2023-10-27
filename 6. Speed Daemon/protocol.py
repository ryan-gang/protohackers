import logging
import socket
import struct
import sys
from typing import Callable

from errors import ProtocolError

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="ERROR",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Parser(object):
    def __init__(self):
        self.u8 = ">B"
        self.u16 = ">H"
        self.u32 = ">I"
        self.lp_str: Callable[[int], str] = lambda length: ">" + "B" * length
        # Length prefixed str

        # Hex codes of message types against messages types.
        self.codes = {
            "ERROR": 10,
            "PLATE": 20,
            "TICKET": 21,
            "WHEARTBEAT": 40,
            "HEARTBEAT": 41,
            "CAMERA": 80,
            "DISPATCHER": 81,
        }

    def parse_str(self, str_bytes: bytes) -> tuple[str, bytes]:
        """
        Given a bytes object, assumes the first part is a length
        prefixed str (of all u8's), returns the parsed str and the
        remaining bytes object.
        """
        length = str_bytes[0]
        string = str_bytes[1 : length + 1]
        fmt = self.lp_str(length)
        unpacked = struct.unpack(fmt, string)
        remaining = str_bytes[length + 1 :]
        return "".join(chr(ascii) for ascii in unpacked), remaining

    def _parse_uint(self, str_bytes: bytes, fmt: str, length: int) -> tuple[int, bytes]:
        """
        Given a bytes object, returns the parsed unsigned int and the
        remaining bytes object.
        str_bytes is the bytes data, fmt is the format and length is the count of bytes.
        """
        length //= 8
        uint = str_bytes[0:length]
        unpacked = struct.unpack(fmt, uint)
        remaining = str_bytes[length:]
        return unpacked[0], remaining

    def parse_uint8(self, str_bytes: bytes) -> tuple[int, bytes]:
        return self._parse_uint(str_bytes, fmt=self.u8, length=8)

    def parse_uint16(self, str_bytes: bytes) -> tuple[int, bytes]:
        return self._parse_uint(str_bytes, fmt=self.u16, length=16)

    def parse_uint32(self, str_bytes: bytes) -> tuple[int, bytes]:
        return self._parse_uint(str_bytes, fmt=self.u32, length=32)

    def parse_array_uint(
        self, str_bytes: bytes, array_length: int, fmt: str, length: int
    ) -> tuple[list[int], bytes]:
        """
        Given a bytes object, returns an array of all parsed unsigned int and the
        remaining bytes object. str_bytes is the bytes data, fmt is the format (of
        the to be unpacked bytes) and length is the count of bytes (in a single
        uint).
        """
        length //= 8
        length *= array_length
        uint = str_bytes[0:length]
        array_fmt = fmt[0] + fmt[1] * array_length
        unpacked = struct.unpack(array_fmt, uint)

        remaining = str_bytes[length:]
        return list(map(int, unpacked)), remaining

    def parse_message_type_to_hex(self, str_bytes: bytes) -> tuple[str, bytes]:
        return hex(str_bytes[0])[2:], str_bytes[1:]

    # Type : 10
    def parse_error_data(self, str_bytes: bytes) -> tuple[str, bytes]:
        msg, str_bytes = self.parse_str(str_bytes)

        return (msg, str_bytes)

    # Type : 20
    def parse_plate_data(self, str_bytes: bytes) -> tuple[str, int, bytes]:
        plate, str_bytes = self.parse_str(str_bytes)
        timestamp, str_bytes = self.parse_uint32(str_bytes)

        return (plate, timestamp, str_bytes)

    # Type : 21
    def parse_ticket_data(
        self, str_bytes: bytes
    ) -> tuple[str, int, int, int, int, int, int, bytes]:
        plate, str_bytes = self.parse_str(str_bytes)
        road, str_bytes = self.parse_uint16(str_bytes)
        mile1, str_bytes = self.parse_uint16(str_bytes)
        timestamp1, str_bytes = self.parse_uint32(str_bytes)
        mile2, str_bytes = self.parse_uint16(str_bytes)
        timestamp2, str_bytes = self.parse_uint32(str_bytes)
        speed, str_bytes = self.parse_uint16(str_bytes)

        return (plate, road, mile1, timestamp1, mile2, timestamp2, speed, str_bytes)

    # Type : 40
    def parse_wantheartbeat_data(self, str_bytes: bytes) -> tuple[int, bytes]:
        interval, str_bytes = self.parse_uint32(str_bytes)

        return (interval, str_bytes)

    # Type : 80
    def parse_iamcamera_data(self, str_bytes: bytes) -> tuple[int, int, int, bytes]:
        try:
            road, str_bytes = self.parse_uint16(str_bytes)
            mile, str_bytes = self.parse_uint16(str_bytes)
            limit, str_bytes = self.parse_uint16(str_bytes)
        except Exception as E:
            raise ProtocolError(E)
        return (road, mile, limit, str_bytes)

    # Type : 81
    def parse_iamdispatcher_data(self, str_bytes: bytes) -> tuple[list[int], bytes]:
        num_roads, str_bytes = self.parse_uint8(str_bytes)
        roads, str_bytes = self.parse_array_uint(str_bytes, num_roads, self.u16, 16)

        return (roads, str_bytes)


class Serializer(object):
    def __init__(self):
        self.u8 = ">B"
        self.u16 = ">H"
        self.u32 = ">I"
        self.lp_str: Callable[[int], str] = lambda length: ">" + "B" * length
        # Length prefixed str

        # Actual int value of codes, can be serialized to their hex values.
        self.codes = {
            "ERROR": 16,
            "PLATE": 32,
            "TICKET": 33,
            "WHEARTBEAT": 64,
            "HEARTBEAT": 65,
            "CAMERA": 128,
            "DISPATCHER": 129,
        }

    def _serialize_lp_str(self, data: str) -> bytes:
        l = self._serialize_uint8(len(data))
        return l + self._serialize_str(data)

    def _serialize_str(self, data: str) -> bytes:
        return bytes(data, "utf-8")

    def _serialize_uint(self, str_bytes: int, fmt: str) -> bytes:
        return struct.pack(fmt, str_bytes)

    def _serialize_uint8(self, str_bytes: int) -> bytes:
        return self._serialize_uint(str_bytes, self.u8)

    def _serialize_uint16(self, str_bytes: int) -> bytes:
        return self._serialize_uint(str_bytes, self.u16)

    def _serialize_uint32(self, str_bytes: int) -> bytes:
        return self._serialize_uint(str_bytes, self.u32)

    def serialize_error_data(self, msg: str) -> bytes:
        CODE_NAME = "ERROR"
        CODE = self.codes[CODE_NAME]
        code = self._serialize_uint8(CODE)
        data = self._serialize_lp_str(msg)
        return code + data

    def serialize_ticket_data(
        self,
        plate: str,
        road: int,
        mile1: int,
        timestamp1: int,
        mile2: int,
        timestamp2: int,
        speed: int,
    ) -> bytes:
        CODE_NAME = "TICKET"
        CODE = self.codes[CODE_NAME]

        data = bytearray()
        data.extend(self._serialize_uint8(CODE))
        data.extend(self._serialize_lp_str(plate))
        data.extend(self._serialize_uint16(road))
        data.extend(self._serialize_uint16(mile1))
        data.extend(self._serialize_uint32(timestamp1))
        data.extend(self._serialize_uint16(mile2))
        data.extend(self._serialize_uint32(timestamp2))
        data.extend(self._serialize_uint16(speed))

        return bytes(data)

    def serialize_heartbeat_data(self) -> bytes:
        CODE_NAME = "HEARTBEAT"
        CODE = self.codes[CODE_NAME]
        data = self._serialize_uint8(CODE)
        return data


class SocketHandler(object):
    def __init__(self):
        self.parser = Parser()

    def send_data(self, conn: socket.socket, response: bytes):
        try:
            logging.debug(f"Response : {response}")
            conn.send(response)
            # logging.info(f"Sent {len(response)} bytes.")
        except Exception as E:
            logging.error(E)

    def _read(self, conn: socket.socket, bits: int) -> bytes:
        _bytes = bits // 8
        data = conn.recv(_bytes)
        return data

    def _read_uint8(self, conn: socket.socket, size: int = 1) -> bytes:
        return self._read(conn, 8 * size)

    def _read_uint16(self, conn: socket.socket, size: int = 1) -> bytes:
        return self._read(conn, 16 * size)

    def _read_uint32(self, conn: socket.socket, size: int = 1) -> bytes:
        return self._read(conn, 32 * size)

    def _read_uint_arr(self, conn: socket.socket) -> bytes:
        l = self._read_uint8(conn)
        length = self.parser.parse_uint8(l)[0]
        return l + self._read_uint16(conn, size=length)

    def _read_str(self, conn: socket.socket) -> bytes:
        l = self._read_uint8(conn)
        length = self.parser.parse_uint8(l)[0]
        return l + self._read_uint8(conn, size=length)

    def read_data(self, conn: socket.socket) -> tuple[str, bytes]:
        msg_type_bytes = self._read_uint8(conn)
        if not msg_type_bytes:
            raise ConnectionRefusedError("Client Disconnected")
        # logging.info(msg_type_bytes.hex())
        msg_type, _ = self.parser.parse_message_type_to_hex(msg_type_bytes)
        data = bytearray()
        if msg_type == "20":
            data.extend(self._read_str(conn))
            data.extend(self._read_uint32(conn))
            return msg_type, bytes(data)
        elif msg_type == "40":
            data.extend(self._read_uint32(conn))
            return msg_type, bytes(data)
        elif msg_type == "80":
            data.extend(self._read_uint16(conn, size=3))
            return msg_type, bytes(data)
        elif msg_type == "81":
            data.extend(self._read_uint_arr(conn))
            return msg_type, bytes(data)
        else:
            raise RuntimeError("Unexpected msg_type")

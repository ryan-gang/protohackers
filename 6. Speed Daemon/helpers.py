import struct
from typing import Callable


class Parser(object):
    def __init__(self):
        self.u8 = ">B"
        self.u16 = ">H"
        self.u32 = ">I"
        self.lp_str: Callable[[int], str] = lambda length: ">" + "B" * length
        # Length prefixed str

    def _parse_str(self, str_bytes: bytes) -> tuple[str, bytes]:
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

    def _parse_uint8(self, str_bytes: bytes) -> tuple[int, bytes]:
        return self._parse_uint(str_bytes, fmt=self.u8, length=8)

    def _parse_uint16(self, str_bytes: bytes) -> tuple[int, bytes]:
        return self._parse_uint(str_bytes, fmt=self.u16, length=16)

    def _parse_uint32(self, str_bytes: bytes) -> tuple[int, bytes]:
        return self._parse_uint(str_bytes, fmt=self.u32, length=32)

    def _parse_array_uint(
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

    def parse_message_type(self, str_bytes: bytes) -> tuple[str, bytes]:
        return hex(str_bytes[0])[2:], str_bytes[1:]

    # Type : 10
    def parse_error_data(self, str_bytes: bytes) -> tuple[str, bytes]:
        msg, str_bytes = self._parse_str(str_bytes)

        return (msg, str_bytes)

    # Type : 20
    def parse_plate_data(self, str_bytes: bytes) -> tuple[str, int, bytes]:
        plate, str_bytes = self._parse_str(str_bytes)
        timestamp, str_bytes = self._parse_uint32(str_bytes)

        return (plate, timestamp, str_bytes)

    # Type : 21
    def parse_ticket_data(
        self, str_bytes: bytes
    ) -> tuple[str, int, int, int, int, int, int, bytes]:
        plate, str_bytes = self._parse_str(str_bytes)
        road, str_bytes = self._parse_uint16(str_bytes)
        mile1, str_bytes = self._parse_uint16(str_bytes)
        timestamp1, str_bytes = self._parse_uint32(str_bytes)
        mile2, str_bytes = self._parse_uint16(str_bytes)
        timestamp2, str_bytes = self._parse_uint32(str_bytes)
        speed, str_bytes = self._parse_uint16(str_bytes)

        return (plate, road, mile1, timestamp1, mile2, timestamp2, speed, str_bytes)

    # Type : 40
    def parse_wantheartbeat_data(self, str_bytes: bytes) -> tuple[int, bytes]:
        interval, str_bytes = self._parse_uint32(str_bytes)

        return (interval, str_bytes)

    # Type : 80
    def parse_iamcamera_data(self, str_bytes: bytes) -> tuple[int, int, int, bytes]:
        road, str_bytes = self._parse_uint32(str_bytes)
        mile, str_bytes = self._parse_uint32(str_bytes)
        limit, str_bytes = self._parse_uint32(str_bytes)

        return (road, mile, limit, str_bytes)

    # Type : 81
    def parse_iamdispatcher_data(self, str_bytes: bytes) -> tuple[list[int], bytes]:
        num_roads, str_bytes = self._parse_uint8(str_bytes)
        roads, str_bytes = self._parse_array_uint(str_bytes, num_roads, self.u16, 16)

        return (roads, str_bytes)


class Camera(object):
    type = "Camera"
    road = None
    mile = None
    limit = None

    def __init__(self, road: int, mile: int, limit: int):
        self.road = road
        self.mile = mile
        self.limit = limit

    def __str__(self):
        return f"Camera@{self.road}-{self.mile}"


class Dispatcher(object):
    type = "Dispatcher"
    num_roads = None
    roads = []

    def __init__(self, num_roads: int, roads: list[int]):
        self.num_roads = num_roads
        self.roads = roads

    def __str__(self):
        return f"Dispatcher@{id(self)}"

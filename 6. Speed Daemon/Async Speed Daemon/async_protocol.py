import logging
import struct
import sys
from asyncio import StreamReader
from typing import Callable

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

        # Int codes of message types against messages types.
        self.codes = {
            "ERROR": 16,  # 0x10
            "PLATE": 32,  # 0x20
            "TICKET": 33,  # 0x21
            "WHEARTBEAT": 64,  # 0x40
            "HEARTBEAT": 65,  # 0x41
            "CAMERA": 128,  # 0x80
            "DISPATCHER": 129,  # 0x81
        }

    async def parse_str(self, reader: StreamReader) -> str:
        length = await self.parse_uint8(reader)
        data = await self.parse_array_uint(reader, length, self.u8, bits=8)
        return "".join(chr(ascii) for ascii in data)

    async def _parse_uint(self, reader: StreamReader, fmt: str, bits: int) -> int:
        uint = await reader.readexactly(bits // 8)
        unpacked, *_ = struct.unpack(fmt, uint)
        return unpacked

    async def parse_uint8(self, reader: StreamReader) -> int:
        return await self._parse_uint(reader, fmt=self.u8, bits=8)

    async def parse_uint16(self, reader: StreamReader) -> int:
        return await self._parse_uint(reader, fmt=self.u16, bits=16)

    async def parse_uint32(self, reader: StreamReader) -> int:
        return await self._parse_uint(reader, fmt=self.u32, bits=32)

    async def parse_array_uint(
        self, reader: StreamReader, array_length: int, fmt: str, bits: int
    ) -> list[int]:
        bytes = array_length * (bits // 8)
        data = await reader.readexactly(bytes)
        array_fmt = fmt[0] + fmt[1] * array_length
        unpacked = struct.unpack(array_fmt, data)
        print(data, unpacked, array_fmt)
        return list(map(int, unpacked))

    async def parse_message_type(self, reader: StreamReader) -> int:
        return await self.parse_uint8(reader)

    # Type : 20
    async def parse_plate_data(self, reader: StreamReader) -> tuple[str, int]:
        plate = await self.parse_str(reader)
        timestamp = await self.parse_uint32(reader)

        return plate, timestamp

    # Type : 40
    async def parse_wantheartbeat_data(self, reader: StreamReader) -> int:
        interval = await self.parse_uint32(reader)
        return interval

    # Type : 80
    async def parse_iamcamera_data(self, reader: StreamReader) -> tuple[int, int, int]:
        # try:
        road = await self.parse_uint16(reader)
        mile = await self.parse_uint16(reader)
        limit = await self.parse_uint16(reader)
        # except Exception as E:
        #     raise ProtocolError(E)
        return (road, mile, limit)

    # Type : 81
    async def parse_iamdispatcher_data(self, reader: StreamReader) -> list[int]:
        num_roads = await self.parse_uint8(reader)
        roads = await self.parse_array_uint(reader, num_roads, self.u16, 16)

        return roads

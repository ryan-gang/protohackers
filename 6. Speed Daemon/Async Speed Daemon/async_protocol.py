import logging
import struct
import sys
from asyncio import StreamReader, StreamWriter
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

# Messages types -> Integer message codes. (Can be serialized to their exp values)
MSG_CODES = {
    "ERROR": 16,  # 0x10
    "PLATE": 32,  # 0x20
    "TICKET": 33,  # 0x21
    "WHEARTBEAT": 64,  # 0x40
    "HEARTBEAT": 65,  # 0x41
    "CAMERA": 128,  # 0x80
    "DISPATCHER": 129,  # 0x81
}
# Format strings.
U8 = ">B"
U16 = ">H"
U32 = ">I"
LP_STR: Callable[[int], str] = lambda length: ">" + "B" * length  # Length prefixed str


class Parser(object):
    async def parse_str(self, reader: StreamReader) -> str:
        length = await self.parse_uint8(reader)
        data = await self.parse_array_uint(reader, length, U8, bits=8)
        return "".join(chr(ascii) for ascii in data)

    async def _parse_uint(self, reader: StreamReader, fmt: str, bits: int) -> int:
        uint = await reader.readexactly(bits // 8)
        unpacked, *_ = struct.unpack(fmt, uint)
        return unpacked

    async def parse_uint8(self, reader: StreamReader) -> int:
        return await self._parse_uint(reader, fmt=U8, bits=8)

    async def parse_uint16(self, reader: StreamReader) -> int:
        return await self._parse_uint(reader, fmt=U16, bits=16)

    async def parse_uint32(self, reader: StreamReader) -> int:
        return await self._parse_uint(reader, fmt=U32, bits=32)

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
        roads = await self.parse_array_uint(reader, num_roads, U16, 16)

        return roads


class Serializer(object):
    async def _serialize_lp_str(self, data: str) -> bytes:
        return await self._serialize_uint8(len(data)) + await self._serialize_str(data)

    async def _serialize_str(self, data: str) -> bytes:
        return bytes(data, "utf-8")

    async def _serialize_uint(self, str_bytes: int, fmt: str) -> bytes:
        return struct.pack(fmt, str_bytes)

    async def _serialize_uint8(self, str_bytes: int) -> bytes:
        return await self._serialize_uint(str_bytes, U8)

    async def _serialize_uint16(self, str_bytes: int) -> bytes:
        return await self._serialize_uint(str_bytes, U16)

    async def _serialize_uint32(self, str_bytes: int) -> bytes:
        return await self._serialize_uint(str_bytes, U32)

    async def serialize_error_data(self, msg: str) -> bytes:
        CODE_NAME = "ERROR"
        CODE = MSG_CODES[CODE_NAME]

        code = await self._serialize_uint8(CODE)
        data = await self._serialize_lp_str(msg)
        return code + data

    async def serialize_ticket_data(
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
        CODE = MSG_CODES[CODE_NAME]

        data = bytearray()
        data.extend(await self._serialize_uint8(CODE))
        data.extend(await self._serialize_lp_str(plate))
        data.extend(await self._serialize_uint16(road))
        data.extend(await self._serialize_uint16(mile1))
        data.extend(await self._serialize_uint32(timestamp1))
        data.extend(await self._serialize_uint16(mile2))
        data.extend(await self._serialize_uint32(timestamp2))
        data.extend(await self._serialize_uint16(speed))

        return bytes(data)

    async def serialize_heartbeat_data(self) -> bytes:
        CODE_NAME = "HEARTBEAT"
        CODE = MSG_CODES[CODE_NAME]

        data = await self._serialize_uint8(CODE)
        return data


class SocketHandler(object):
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.p = Parser()

    async def write(self, data: str):
        if not data.endswith("\n"):
            data += "\n"
        self.writer.write(data.encode())
        logging.debug(f"Response : {data.strip()}")
        logging.debug(f"Sent {len(data)} bytes.")
        return await self.writer.drain()

    async def close(self, error_msg: str, conn: str):
        await self.write(error_msg)
        self.writer.write_eof()
        self.writer.close()
        logging.info(f"Closed connection to client @ {conn}.")

    async def read(self):
        try:
            msg_code = await self.p.parse_message_type(self.reader)
        except ConnectionRefusedError:
            logging.error("Client Disconnected.")
            return ""
        if msg_code == 20:
            _ = await self.p.parse_plate_data(self.reader)
        elif msg_code == 40:
            _ = await self.p.parse_wantheartbeat_data(self.reader)
        elif msg_code == 80:
            _ = await self.p.parse_iamcamera_data(self.reader)
        elif msg_code == 81:
            _ = await self.p.parse_iamdispatcher_data(self.reader)
        else:
            raise RuntimeError("Unexpected msg_type")

        logging.debug(f"Type : {msg_code}, Data : {_}")
        return msg_code, _

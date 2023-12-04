import logging
import struct
import sys
from asyncio import StreamReader, StreamWriter
from typing import Callable

from errors import ProtocolError
from messages import (OK, CreatePolicy, DeletePolicy, DialAuthority, Error,
                      Hello, PolicyResult, PopulationActual, PopulationTarget,
                      SiteVisit, TargetPopulations)

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

# Bytes message types -> Messages code.
MSG_CODES = {
    "HELLO": 80,  # 0x50
    "ERROR": 81,  # 0x51
    "OK": 82,  # 0x52
    "DIALAUTHORITY": 83,  # 0x53
    "TARGETPOPULATIONS": 84,  # 0x54
    "CREATEPOLICY": 85,  # 0x55
    "DELETEPOLICY": 86,  # 0x56
    "POLICYRESULT": 87,  # 0x57
    "SITEVISIT": 88,  # 0x58
}
# Format strings.
U8 = ">B"
U16 = ">H"
U32 = ">I"
LP_STR: Callable[[int], str] = lambda length: ">" + "B" * length  # Length prefixed str
FORMAT = {"str": LP_STR, "u8": U8, "u16": U16, "u32": U32}


class Reader(object):
    def __init__(self, reader: StreamReader, parser: "Parser") -> None:
        self.reader = reader
        self.parser = parser

    async def read_message(self, log: bool = False) -> tuple[int, bytearray]:
        """
        Returns the msg_code (identifying the type), and the bytearray for the request.
        """
        out = bytearray()
        msg_code = await self.reader.readexactly(1)
        out.extend(msg_code)
        l = await self.reader.readexactly(4)
        out.extend(l)
        length, _ = self.parser.parse_uint32(l)
        d = await self.reader.readexactly(length - 5)
        out.extend(d)
        if log:
            logging.debug(f"Request : {out}")
        return out[0], out

    async def read_and_parse_u32(self) -> int:
        b = await self.reader.readexactly(4)
        u, _ = self.parser.parse_uint32(b)
        return u


class Parser(object):
    def parse_str(self, data: bytes, length: int) -> tuple[str, bytes]:
        res = struct.unpack(LP_STR(length), data[:length])
        remaining = data[length:]
        return "".join(chr(ascii) for ascii in res), remaining

    def _parse_uint(self, data: bytes, fmt: str, length: int) -> tuple[int, bytes]:
        """
        Given a bytes object, returns the parsed unsigned int and the
        remaining bytes object.
        `data` is the bytes data, `fmt` is the format and `length` is the number of bytes.
        """
        length //= 8
        uint = data[0:length]
        unpacked = struct.unpack(fmt, uint)
        remaining = data[length:]
        return unpacked[0], remaining

    def parse_uint8(self, data: bytes) -> tuple[int, bytes]:
        return self._parse_uint(data, fmt=U8, length=8)

    def parse_uint16(self, data: bytes) -> tuple[int, bytes]:
        return self._parse_uint(data, fmt=U16, length=16)

    def parse_uint32(self, data: bytes) -> tuple[int, bytes]:
        return self._parse_uint(data, fmt=U32, length=32)

    def parse_message(self, data: bytes):
        if (sum(data) % 256) != 0:
            raise ProtocolError("Incorrect Checksum")
        msg_code, data = self.parse_uint8(data)
        match msg_code:
            case 80:
                return self.parse_hello(data)
            case 81:
                return self.parse_error(data)
            case 82:
                return self.parse_ok(data)
            case 83:
                return self.parse_dial_authority(data)
            case 84:
                return self.parse_target_populations(data)
            case 85:
                return self.parse_create_policy(data)
            case 86:
                return self.parse_delete_policy(data)
            case 87:
                return self.parse_policy_result(data)
            case 88:
                return self.parse_site_visit(data)
            case _:
                return ""

    # HELLO
    def parse_hello(self, data: bytes):
        _, data = self.parse_uint32(data)
        l, data = self.parse_uint32(data)
        proto, data = self.parse_str(data, l)
        version, data = self.parse_uint32(data)
        assert proto == "pestcontrol", f"Bad protocol: {proto}"
        assert version == 1, f"Bad version : {version}"
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return Hello(proto, version)

    # ERROR
    def parse_error(self, data: bytes):
        _, data = self.parse_uint32(data)
        l, data = self.parse_uint32(data)
        error_msg, data = self.parse_str(data, l)
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return Error(error_msg)

    # OK
    def parse_ok(self, data: bytes):
        return OK()

    # DIALAUTHORITY
    def parse_dial_authority(self, data: bytes):
        _, data = self.parse_uint32(data)
        site, data = self.parse_uint32(data)
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return DialAuthority(site)

    # TARGETPOPULATIONS
    def parse_target_populations(self, data: bytes):
        _, data = self.parse_uint32(data)
        site, data = self.parse_uint32(data)
        array_len, data = self.parse_uint32(data)
        pops: list[PopulationTarget] = []
        for _ in range(array_len):
            l, data = self.parse_uint32(data)
            species, data = self.parse_str(data, l)
            minimum, data = self.parse_uint32(data)
            maximum, data = self.parse_uint32(data)
            pop = PopulationTarget(species, minimum, maximum)
            pops.append(pop)
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return TargetPopulations(site, pops)

    # CREATEPOLICY
    def parse_create_policy(self, data: bytes):
        _, data = self.parse_uint32(data)
        l, data = self.parse_uint32(data)
        species, data = self.parse_str(data, l)
        action, act = data[0], False
        if action == 144:
            act = "CULL"  # Cull
        elif action == 160:
            act = "CONSERVE"  # Conserve
        else:
            raise ProtocolError("Unknown Action in Policy")
        assert len(data) == 2, f"Unconsumed data even after parsing : {data}"

        return CreatePolicy(species, act)

    # DELETEPOLICY
    def parse_delete_policy(self, data: bytes):
        _, data = self.parse_uint32(data)
        policy, data = self.parse_uint32(data)
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return DeletePolicy(policy)

    # POLICYRESULT
    def parse_policy_result(self, data: bytes):
        _, data = self.parse_uint32(data)
        policy, data = self.parse_uint32(data)
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return PolicyResult(policy)

    # SITEVISIT
    def parse_site_visit(self, data: bytes):
        _, data = self.parse_uint32(data)
        site, data = self.parse_uint32(data)
        array_len, data = self.parse_uint32(data)
        pops: list[PopulationActual] = []
        for _ in range(array_len):
            l, data = self.parse_uint32(data)
            species, data = self.parse_str(data, l)
            count, data = self.parse_uint32(data)
            pop = PopulationActual(species, count)
            pops.append(pop)
        assert len(data) == 1, f"Unconsumed data even after parsing : {data}"

        return SiteVisit(site, pops)


class Serializer(object):
    def _serialize_lp_str(self, data: str) -> bytes:
        return self._serialize_uint32(len(data)) + self._serialize_str(data)

    def _serialize_str(self, data: str) -> bytes:
        return bytes(data, "utf-8")

    def _serialize_uint(self, data: int, fmt: str) -> bytes:
        return struct.pack(fmt, data)

    def _serialize_uint8(self, data: int) -> bytes:
        return self._serialize_uint(data, U8)

    def _serialize_uint16(self, data: int) -> bytes:
        return self._serialize_uint(data, U16)

    def _serialize_uint32(self, data: int) -> bytes:
        return self._serialize_uint(data, U32)

    def serialize_message(self, data: bytearray, msg_code: int) -> bytes:
        length = len(data) + 1 + 4 + 1
        out = (self._serialize_uint8(msg_code)) + (self._serialize_uint32(length)) + data
        checksum = (256 - (sum(out) % 256)) % 256
        out += self._serialize_uint8(checksum)
        return out

    def serialize_hello(self, msg: Hello) -> bytes:
        CODE_NAME = "HELLO"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(self._serialize_lp_str(msg.protocol))
        out.extend(self._serialize_uint32(msg.version))
        return self.serialize_message(out, CODE)

    def serialize_error(self, msg: Error) -> bytes:
        CODE_NAME = "ERROR"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(self._serialize_lp_str(msg.message))
        return self.serialize_message(out, CODE)

    def serialize_ok(self, msg: OK) -> bytes:
        CODE_NAME = "OK"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        return self.serialize_message(out, CODE)

    def serialize_dial_authority(self, msg: DialAuthority) -> bytes:
        CODE_NAME = "DIALAUTHORITY"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(self._serialize_uint32(msg.site))
        return self.serialize_message(out, CODE)

    def serialize_create_policy(self, msg: CreatePolicy) -> bytes:
        CODE_NAME = "CREATEPOLICY"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(self._serialize_lp_str(msg.species))
        if msg.action == "CONSERVE":
            action = 160
        else:
            action = 144
        out.extend(self._serialize_uint8(action))
        return self.serialize_message(out, CODE)

    def serialize_delete_policy(self, msg: DeletePolicy) -> bytes:
        CODE_NAME = "DELETEPOLICY"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(self._serialize_uint32(msg.policy))
        return self.serialize_message(out, CODE)


class Writer(object):
    def __init__(self, writer: StreamWriter) -> None:
        self.writer = writer
        self.p = Parser()

    async def write(self, data: bytes, log: bool = False):
        self.writer.write(data)
        if log:
            logging.debug(f"Response : {data.strip()}")
        await self.writer.drain()
        return

    async def close(self, conn: str):
        self.writer.write_eof()
        self.writer.close()
        logging.debug(f"Closed connection to client @ {conn}.")

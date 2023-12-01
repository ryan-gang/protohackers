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

# Bytes message codes -> Messages types.
MSG_CODES = {
    80: "HELLO",  # 0x50
    81: "ERROR",  # 0x51
    82: "OK",  # 0x52
    83: "DIALAUTHORITY",  # 0x53
    84: "TARGETPOPULATIONS",  # 0x54
    85: "CREATEPOLICY",  # 0x55
    86: "DELETEPOLICY",  # 0x56
    87: "POLICYRESULT",  # 0x57
    88: "SITEVISIT",  # 0x58
}
# Format strings.
U8 = ">B"
U16 = ">H"
U32 = ">I"
LP_STR: Callable[[int], str] = lambda length: ">" + "B" * length  # Length prefixed str
FORMAT = {"str": LP_STR, "u8": U8, "u16": U16, "u32": U32}


class SocketHandler(object):
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.p = Parser()

    async def write(self, data: str, log: bool = True):
        self.writer.write(data.encode())
        if log:
            logging.debug(f"Response : {data.strip()}")
            logging.debug(f"Sent {len(data)} bytes.")
        return await self.writer.drain()

    async def close(self, conn: str):
        self.writer.write_eof()
        self.writer.close()
        logging.debug(f"Closed connection to client @ {conn}.")


class Reader(object):
    def __init__(self, reader: StreamReader, parser: "Parser") -> None:
        self.reader = reader
        self.parser = parser

    async def read_message(self) -> tuple[int, bytearray]:
        """
        Returns the msg_code (identifying the type), and the bytearray for the request.
        """
        out = bytearray()
        msg_code = await self.reader.readexactly(1)
        out.extend(msg_code)
        l = await self.reader.readexactly(4)
        out.extend(l)
        length, _ = self.parser.parse_uint32(l)
        d = await self.reader.readexactly(length - 2)
        out.extend(d)
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

        return Hello(proto, version)

    # ERROR
    def parse_error(self, data: bytes):
        _, data = self.parse_uint32(data)
        l, data = self.parse_uint32(data)
        error_msg, data = self.parse_str(data, l)

        return Error(error_msg)

    # OK
    def parse_ok(self, data: bytes):
        return OK()

    # DIALAUTHORITY
    def parse_dial_authority(self, data: bytes):
        _, data = self.parse_uint32(data)
        site, data = self.parse_uint32(data)

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

        return TargetPopulations(site, pops)

    # CREATEPOLICY
    def parse_create_policy(self, data: bytes):
        _, data = self.parse_uint32(data)
        l, data = self.parse_uint32(data)
        species, data = self.parse_str(data, l)
        action, act = data[0], False
        if action == 144:
            act = False  # Cull
        elif action == 160:
            act = True  # Conserve
        else:
            raise ProtocolError("Unknown Action in Policy")

        return CreatePolicy(species, act)

    # DELETEPOLICY
    def parse_delete_policy(self, data: bytes):
        _, data = self.parse_uint32(data)
        policy, data = self.parse_uint32(data)
        return DeletePolicy(policy)

    # POLICYRESULT
    def parse_policy_result(self, data: bytes):
        _, data = self.parse_uint32(data)
        policy, data = self.parse_uint32(data)
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

        return SiteVisit(site, pops)

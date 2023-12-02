import struct
from typing import Callable

from messages import OK, CreatePolicy, DeletePolicy, DialAuthority, Error, Hello

# Format strings.
U8 = ">B"
U16 = ">H"
U32 = ">I"
LP_STR: Callable[[int], str] = lambda length: ">" + "B" * length  # Length prefixed

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


class Serializer(object):
    async def _serialize_lp_str(self, data: str) -> bytes:
        return await self._serialize_uint32(len(data)) + await self._serialize_str(data)

    async def _serialize_str(self, data: str) -> bytes:
        return bytes(data, "utf-8")

    async def _serialize_uint(self, data: int, fmt: str) -> bytes:
        return struct.pack(fmt, data)

    async def _serialize_uint8(self, data: int) -> bytes:
        return await self._serialize_uint(data, U8)

    async def _serialize_uint16(self, data: int) -> bytes:
        return await self._serialize_uint(data, U16)

    async def _serialize_uint32(self, data: int) -> bytes:
        return await self._serialize_uint(data, U32)

    async def serialize_message(self, data: bytearray, msg_code: int) -> bytes:
        length = len(data) + 1 + 4 + 1
        out = (
            (await self._serialize_uint8(msg_code)) + (await self._serialize_uint32(length)) + data
        )
        checksum = 256 - (sum(out) % 256)
        out += await self._serialize_uint8(checksum)
        return out

    async def serialize_hello(self, msg: Hello) -> bytes:
        CODE_NAME = "HELLO"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(await self._serialize_lp_str(msg.protocol))
        out.extend(await self._serialize_uint32(msg.version))
        return await self.serialize_message(out, CODE)

    async def serialize_error(self, msg: Error) -> bytes:
        CODE_NAME = "ERROR"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(await self._serialize_lp_str(msg.message))
        return await self.serialize_message(out, CODE)

    async def serialize_ok(self, msg: OK) -> bytes:
        CODE_NAME = "OK"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        return await self.serialize_message(out, CODE)

    async def serialize_dial_authority(self, msg: DialAuthority) -> bytes:
        CODE_NAME = "DIALAUTHORITY"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(await self._serialize_uint32(msg.site))
        return await self.serialize_message(out, CODE)

    async def serialize_create_policy(self, msg: CreatePolicy) -> bytes:
        CODE_NAME = "CREATEPOLICY"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(await self._serialize_lp_str(msg.species))
        action: int = 160 if msg.action else 144
        out.extend(await self._serialize_uint8(action))
        return await self.serialize_message(out, CODE)

    async def serialize_delete_policy(self, msg: DeletePolicy) -> bytes:
        CODE_NAME = "DELETEPOLICY"
        CODE = MSG_CODES[CODE_NAME]

        out = bytearray()
        out.extend(await self._serialize_uint32(msg.policy))
        return await self.serialize_message(out, CODE)

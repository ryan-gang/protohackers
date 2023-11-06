import logging
import os
import re
import sys
from asyncio import StreamReader, StreamWriter
from heapq import heappop, heappush
from typing import Callable

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


async def prioritise(toys: str) -> str:
    heap: list[tuple[int, str]] = []
    toys = toys.strip()
    for toy in toys.split(","):
        match = re.match("([0-9]*)x .*", toy)
        if match:
            val = int(match.groups()[0])
            heappush(heap, (-val, toy))
    return heappop(heap)[1]


class Crypto(object):
    def __init__(self, schema: bytes) -> None:
        # Pass schema while instantiating object, all subsequent calls will use
        # internal representation for crypto.
        self.ENC = {
            1: self._reversebits,
            2: self._xor,
            3: self._xor_with_pos,
            4: self._add,
            5: self._add_with_pos,
        }
        self.DEC = {
            1: self._reversebits,
            2: self._xor,
            3: self._xor_with_pos,
            4: self._sub,
            5: self._sub_with_pos,
        }
        self.encode_schema: list[
            tuple[Callable[[int, int, int], int], int]
        ] = self._parse_schema_enc(schema)
        self.decode_schema: list[
            tuple[Callable[[int, int, int], int], int]
        ] = self._parse_schema_dec(schema)

    def _parse_schema_enc(self, schema: bytes) -> list[tuple[Callable[[int, int, int], int], int]]:
        """
        Break the bytes object into groups of ops.
        Makes the encode and decode method concise.
        Solves the schema reversal problem in decode.
        """
        groups: list[tuple[Callable[[int, int, int], int], int]] = []
        idx = 0
        while idx < len(schema):
            bit = schema[idx]
            if bit == 0:
                idx += 1
                continue
            if bit == 2 or bit == 4:
                val = schema[idx + 1]
                group = (self.ENC[bit], val)
                idx += 2
            else:
                group = (self.ENC[bit], 0)
                idx += 1
            groups.append(group)

        return groups

    def _parse_schema_dec(self, schema: bytes) -> list[tuple[Callable[[int, int, int], int], int]]:
        """
        Break the bytes object into groups of ops.
        Makes the encode and decode method concise.
        """
        groups: list[tuple[Callable[[int, int, int], int], int]] = []
        idx = 0
        while idx < len(schema):
            bit = schema[idx]
            if bit == 0:
                idx += 1
                continue
            if bit == 2 or bit == 4:
                val = schema[idx + 1]
                group = (self.DEC[bit], val)
                idx += 2
            else:
                group = (self.DEC[bit], 0)
                idx += 1
            groups.append(group)

        return groups[::-1]

    def _reversebits(self, b: int, n: int, byte_counter: int) -> int:
        return int("{:08b}".format(b)[::-1], 2)

    def _xor(self, b: int, n: int, byte_counter: int) -> int:
        return (b ^ n) & 255

    def _xor_with_pos(self, b: int, n: int, byte_counter: int) -> int:
        return self._xor(b, byte_counter, n)

    def _add(self, b: int, n: int, byte_counter: int) -> int:
        return (b + n) & 255

    def _add_with_pos(self, b: int, n: int, byte_counter: int) -> int:
        return self._add(b, byte_counter, n)

    def _sub(self, b: int, n: int, byte_counter: int) -> int:
        return (b - n) & 255

    def _sub_with_pos(self, b: int, n: int, byte_counter: int) -> int:
        return self._sub(b, byte_counter, n)

    def decode(self, data: int, byte_counter: int) -> int:
        for group in self.decode_schema:
            func, param = group
            # print(f"Running {func} with params : {data}, {param}, {byte_counter}")
            data = func(data, param, byte_counter)
            # print(f"Output is {data}")
        return data

    def encode(self, str_data: str, byte_counter: int) -> int:
        data = ord(str_data)
        for group in self.encode_schema:
            func, param = group
            # print(f"Running {func} with params : {data}, {param}, {byte_counter}")
            data = func(data, param, byte_counter)
            # print(f"Output is {data}")
        return data

    def print_hex(self, b: bytearray) -> None:
        for bit in b:
            print(hex(bit).replace("0x", ""), end=" ")
        print()

    def no_op_cipher(self) -> bool:
        data = os.urandom(100)
        encoded = b""
        for idx, val in enumerate(data):
            encoded += int.to_bytes(self.decode(val, idx))

        return data == encoded


class Reader(object):
    def __init__(self, reader: StreamReader, crypto: Crypto) -> None:
        self.reader = reader
        self.crypto = crypto
        self.byte_counter = 0

    async def readline(self) -> str:
        line: list[str] = []
        while True:
            byte = await self.reader.readexactly(1)
            if byte == b"":
                raise RuntimeError("Connection closed by client")
            code = self.crypto.decode(byte[0], self.byte_counter)
            char = chr(code)
            self.byte_counter += 1
            line.append(char)
            if char == "\n":
                break
        return "".join(line)


class Writer(object):
    def __init__(self, writer: StreamWriter, crypto: Crypto) -> None:
        self.writer = writer
        self.crypto = crypto
        self.byte_counter = 0

    async def writeline(self, data: str, client: str):
        data = data + "\n"
        out = bytearray()
        for idx, val in enumerate(data):
            index = idx + self.byte_counter
            out.append(self.crypto.encode(val, index))

        self.byte_counter += len(data)
        self.writer.write(out)
        logging.info(f"Sent {out.hex()} : {len(data)} bytes to {client}")
        await self.writer.drain()
        return

    async def close(self, client_uuid: str):
        self.writer.write_eof()
        self.writer.close()
        logging.info(f"Closed connection to client @ {client_uuid}.")
        return

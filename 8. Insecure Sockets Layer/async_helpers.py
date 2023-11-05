import logging
import sys

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Crypto(object):
    def __init__(self) -> None:
        pass

    def _parse_schema(self, schema: bytes) -> list[list[int]]:
        """
        Break the bytes object into groups of ops.
        Makes the encode and decode method concise.
        Solves the schema reversal problem in decode.
        """
        groups: list[list[int]] = []
        idx = 0
        while idx < len(schema):
            bit = schema[idx]
            if bit == 2 or bit == 4:
                group = [bit, schema[idx + 1]]
                idx += 2
            else:
                group = [bit]
                idx += 1
            groups.append(group)

        return groups

    async def _reversebits(self, b: bytearray) -> bytearray:
        for idx, val in enumerate(b):
            b[idx] = int("{:08b}".format(val)[::-1], 2)
        return b

    async def _xor(self, b: bytearray, n: int) -> bytearray:
        for idx, val in enumerate(b):
            b[idx] = val ^ n
        return b

    async def _xor_with_pos(self, b: bytearray, start: int) -> bytearray:
        n = start
        for idx, val in enumerate(b):
            b[idx] = val ^ n
            n += 1
        return b

    async def _add(self, b: bytearray, n: int) -> bytearray:
        for idx, val in enumerate(b):
            b[idx] = (val + n) % 256
        return b

    async def _add_with_pos(self, b: bytearray, start: int) -> bytearray:
        n = start
        for idx, val in enumerate(b):
            b[idx] = (val + n) % 256
            n += 1
        return b

    async def _sub(self, b: bytearray, n: int) -> bytearray:
        for idx, val in enumerate(b):
            b[idx] = (val - n) % 256
        return b

    async def _sub_with_pos(self, b: bytearray, start: int) -> bytearray:
        n = start
        for idx, val in enumerate(b):
            b[idx] = (val - n) % 256
            n += 1
        return b

    async def encode(
        self, data: bytearray, schema: bytes, byte_counter: int
    ) -> tuple[bytearray, int]:
        """
        Given the data, schema and byte_counter encodes the data and returns the
        modified data to be sent to client.
        """
        for action in self._parse_schema(schema):
            match action:
                case [0]:
                    pass
                case [1]:
                    data = await self._reversebits(data)
                case [2, *val]:
                    xor_val = val[0]
                    data = await self._xor(data, n=xor_val)
                case [3]:
                    data = await self._xor_with_pos(data, start=byte_counter)
                case [4, *val]:
                    add_val = val[0]
                    data = await self._add(data, n=add_val)
                case [5]:
                    data = await self._add_with_pos(data, start=byte_counter)
                case _:
                    pass
        return (data, byte_counter + len(data))

    async def decode(
        self, data: bytearray, schema: bytes, byte_counter: int
    ) -> tuple[bytearray, int]:
        """
        Given the data, schema and byte_counter decodes the data and returns the
        original data to be processed.
        """
        for action in self._parse_schema(schema)[::-1]:
            match action:
                case [0]:
                    pass
                case [1]:
                    data = await self._reversebits(data)
                case [2, *val]:
                    xor_val = val[0]
                    data = await self._xor(data, n=xor_val)
                case [3]:
                    data = await self._xor_with_pos(data, start=byte_counter)
                case [4, *val]:
                    add_val = val[0]
                    data = await self._sub(data, n=add_val)
                case [5]:
                    data = await self._sub_with_pos(data, start=byte_counter)
                case _:
                    pass
        return (data, byte_counter + len(data))

    def print_hex(self, b: bytearray) -> None:
        for bit in b:
            print(hex(bit).replace("0x", ""), end=" ")
        print()

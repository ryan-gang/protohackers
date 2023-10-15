"""
Ref :
https://docs.python.org/3.7/library/struct.html#format-characters
"""


import datetime
import struct
import logging
import pandas as pd


class PriceAnalyzer:
    def __init__(self) -> None:
        self.datastore = pd.DataFrame(columns=["timestamp", "price"])

    def append_row(self, seconds: int, price: int) -> None:
        timestamp = datetime.datetime.utcfromtimestamp(seconds)
        row = {"timestamp": timestamp, "price": price}
        logging.debug(f"Append row : {row}")
        df = pd.DataFrame([row])
        self.datastore = pd.concat([self.datastore, df])

    def get_mean(self, start_time: int, end_time: int) -> int:
        start_timestamp = datetime.datetime.utcfromtimestamp(start_time)
        end_timestamp = datetime.datetime.utcfromtimestamp(end_time)

        logging.debug(f"Get mean : {start_timestamp} to {end_timestamp}")
        mean_price = self.datastore[
            (self.datastore["timestamp"] >= start_timestamp)
            & (self.datastore["timestamp"] <= end_timestamp)
        ]["price"].mean()
        logging.info(f"Mean : {mean_price}")
        if pd.isna(mean_price):
            return 0
        else:
            return int(mean_price)


def handle_request(data: bytes, analyzer: PriceAnalyzer):
    in_format = ">cii"
    out_format = ">i"

    mean_price = 0

    for start in range(0, len(data), 9):
        end = start + 9
        part = data[start:end]
        mode, arg_1, arg_2 = struct.unpack(in_format, part)

        if mode.decode() == "I":
            seconds, price = arg_1, arg_2
            analyzer.append_row(seconds, price)
        elif mode.decode() == "Q":
            start_time, end_time = arg_1, arg_2
            mean_price = analyzer.get_mean(start_time, end_time)
    return struct.pack(out_format, mean_price)

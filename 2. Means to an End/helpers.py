import datetime
import logging

import pandas as pd


class PriceAnalyzer:
    def __init__(self) -> None:
        self.datastore = pd.DataFrame(columns=["timestamp", "price"])
        self.__initialize__()

    def __initialize__(self) -> None:
        self.processed_all_rows = True
        # Each append only adds data to internal lists. We need to add all the
        # data from these lists to our datastore before computing the mean, this
        # flag denotes if its safe to compute mean at this point of time.
        # ie all lists data have been added to datastore.
        self.timestamps: list[datetime.datetime] = []
        self.prices: list[int] = []

    def append_row(self, seconds: int, price: int) -> None:
        """
        Simply appends the data to a list, to be processed in batch later.
        """
        timestamp = datetime.datetime.utcfromtimestamp(seconds)
        logging.debug(f"Append row : {timestamp}, {price}")

        self.timestamps.append(timestamp)
        self.prices.append(price)
        self.processed_all_rows = False

    def get_mean(self, start_time: int, end_time: int) -> int:
        """
        Process all the data stores in the lists, in a single batch mode.
        Compute and return mean.
        """
        start_timestamp = datetime.datetime.utcfromtimestamp(start_time)
        end_timestamp = datetime.datetime.utcfromtimestamp(end_time)
        logging.debug(f"Get mean : {start_timestamp} to {end_timestamp}")

        if not self.processed_all_rows:
            df = pd.DataFrame({"timestamp": self.timestamps, "price": self.prices})
            logging.info(f"Adding {len(df)} rows to datastore.")
            self.datastore = pd.concat([self.datastore, df])  # type: ignore
            self.__initialize__()

        mean_price = self.datastore[
            (self.datastore["timestamp"] >= start_timestamp)
            & (self.datastore["timestamp"] <= end_timestamp)
        ][
            "price"
        ].mean()  # type: ignore
        if pd.isna(mean_price):  # type: ignore
            out = 0
        else:
            out = int(mean_price)
        logging.info(f"Mean : {out}")
        return out

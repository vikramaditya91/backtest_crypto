from __future__ import annotations

import datetime
import logging
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
import xarray as xr
from crypto_history.utilities.general_utilities import register_factory
from sqlalchemy import create_engine

from backtest_crypto.utilities.general import InsufficientHistory
from backtest_crypto.history_collect.clean_history import remove_duplicates

logger = logging.getLogger(__package__)


class AbstractRawHistoryObtainCreator(ABC):
    """Abstract disk-writer creator"""

    @abstractmethod
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        """factory method to create the disk-writer"""
        pass

    def store_largest_xarray_in_singleton(self,
                                          *args,
                                          **kwargs):
        product = self.factory_method(*args, **kwargs)
        product.store_largest_da_on_borg(product.get_fresh_xarray())
        return product.get_full_history_store()


@register_factory(section="access_xarray", identifier="sqlite")
class SQLiteCoinHistoryCreator(AbstractRawHistoryObtainCreator):
    """SQLite creator"""

    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        return ConcreteSQLiteCoinHistoryAccess(*args, **kwargs)


class ConcreteAbstractCoinHistoryAccess:
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.largest_xarray_dict = None

    @abstractmethod
    def get_fresh_xarray(self):
        pass

    def store_largest_da_on_borg(self, dataarray_dict):
        # TODO Certainly better ways to do it
        for key in dataarray_dict.keys():
            for ohlcv_field in dataarray_dict[key].ohlcv_fields.values:
                if ohlcv_field != "weight":
                    dataarray_dict[key].loc[{"ohlcv_fields": ohlcv_field}] = \
                        dataarray_dict[key].loc[{"ohlcv_fields": ohlcv_field}].astype(float)
        self.largest_xarray_dict = dataarray_dict

    def get_full_history_store(self) -> FullHistoryStore:
        if self.largest_xarray_dict is None:
            dataarray_dict = self.get_fresh_xarray()
            self.store_largest_da_on_borg(dataarray_dict)
        else:
            dataarray_dict = self.largest_xarray_dict
        return FullHistoryStore(dataarray_dict)


class ConcreteSQLiteCoinHistoryAccess(ConcreteAbstractCoinHistoryAccess):
    def __init__(self,
                 olhcv_field,
                 overall_start,
                 overall_end,
                 candle,
                 reference_coin,
                 file_path,
                 mapped_class,
                 table_name_list
                 ):
        super(ConcreteSQLiteCoinHistoryAccess, self).__init__()
        self.largest_xarray = None
        self.overall_end = overall_end
        self.overall_start = overall_start
        self.reference_coin = reference_coin
        self.candle = candle
        self.file_path = file_path
        self.ohlcv_field = olhcv_field
        self.sqlite_db_path = file_path
        self.engine = create_engine(
            f'sqlite:///{file_path}',
            echo=True
        )
        self.mapped_class = mapped_class
        self.table_name_list = table_name_list

    def get_list_of_df(self):
        df_dict = {}
        for table_name in self.table_name_list:
            raw_df = pd.read_sql_table(table_name, con=self.engine)
            raw_df = raw_df.set_index('timestamp', drop=True)
            df_dict[table_name.split("_")[-1]] = raw_df
        return df_dict

    def df_to_xarray(self,
                     candle,
                     df):
        underlying_np = np.array([[df.values, np.full(df.shape, candle)]])
        return xr.DataArray(underlying_np,
                            dims=["reference_assets",
                                  "ohlcv_fields",
                                  "timestamp",
                                  "base_assets"],
                            coords=[
                                [self.reference_coin],
                                [self.ohlcv_field, "weight"],
                                df.index,
                                df.columns])

    def get_fresh_xarray(self):
        df_dict = self.get_list_of_df()
        x_array_dict = {}
        for candle, df in df_dict.items():
            logger.info("Finished accessing the sql to generate the df")
            non_duplicate_df = remove_duplicates(df)
            x_array_dict[candle] = self.df_to_xarray(candle, non_duplicate_df)
        return x_array_dict


class FullHistoryStore:
    def __init__(self,
                 dataarray):
        self.dataarray = dataarray
        self.timestamp_dict = {}

    def get_instantaneous_history(self,
                                  current_time,
                                  candle,
                                  ohlcv_field="open"):
        da = self.dataarray[candle]
        try:
            instant_history = da.sel(timestamp=current_time.timestamp() * 1000)
        except KeyError:
            raise InsufficientHistory(f"History not present in {current_time}")
        da = instant_history.dropna("base_assets")
        da_dict = da.loc[{"ohlcv_fields": ohlcv_field}].to_dict()
        return dict(zip(da_dict["coords"]["base_assets"]["data"],
                        map(float, da_dict["data"][0])
                        ))

    def select_history(self,
                       start: datetime.datetime,
                       end: datetime.datetime,
                       dataarray,
                       candle):

        if start > end:
            temp_end = end
            end = start
            start = temp_end
            logger.warning("Switching start and end in select history as start is after end")
        # TODO Potential for improvement here
        timestamp_list = self.get_timestamps(candle)
        starter = start.timestamp() * 1000
        ender = end.timestamp() * 1000
        filtered_timestamps = [item for item in timestamp_list if starter < item < ender]
        return dataarray.sel(timestamp=filtered_timestamps)

    def get_merged_histories(self,
                             start_time,
                             end_time,
                             backward_details,
                             remaining):
        sub_histories = []
        sub_end = start_time
        for sub_start_tdelta, sub_end_tdelta, candle in backward_details:
            sub_start = end_time + sub_start_tdelta
            sub_end = end_time + sub_end_tdelta
            if sub_start < start_time:
                sub_start = start_time
            if sub_end < start_time:
                sub_end = start_time
            sub_history = self.select_history(sub_start,
                                              sub_end,
                                              self.dataarray[candle],
                                              candle=candle)
            sub_histories.append(sub_history)
        sub_histories.append(self.select_history(sub_end,
                                                 start_time,
                                                 self.dataarray[remaining],
                                                 candle=remaining))
        joined_xarray = xr.concat([*sub_histories], dim="timestamp")
        # TODO Raise an error if history is empty
        return joined_xarray.sortby("timestamp")

    def get_simple_history(self,
                           start_time,
                           end_time,
                           candle):
        return self.select_history(start_time,
                                   end_time,
                                   self.dataarray[candle],
                                   candle)

    def get_timestamps(self,
                       candle):
        if candle not in self.timestamp_dict.keys():
            self.timestamp_dict[candle] = self.dataarray[candle].timestamp.values.tolist()
        return self.timestamp_dict[candle]


def store_largest_xarray(creator: AbstractRawHistoryObtainCreator,
                         overall_start,
                         overall_end,
                         candle,
                         ohlcv_field,
                         *args,
                         **kwargs):
    return creator.store_largest_xarray_in_singleton(ohlcv_field,
                                                     overall_start,
                                                     overall_end,
                                                     candle,
                                                     *args,
                                                     **kwargs)


def get_merged_history(datarray_object: FullHistoryStore,
                       *args,
                       **kwargs):
    return datarray_object.get_merged_histories(*args,
                                                **kwargs)


def get_simple_history(full_history_da_dict: FullHistoryStore,
                       *args,
                       **kwargs
                       ):
    return full_history_da_dict.get_simple_history(*args,
                                                   **kwargs)


def get_instantaneous_history_from_datarray(datarray_object: FullHistoryStore,
                                            current_time,
                                            candle):
    return datarray_object.get_instantaneous_history(current_time,
                                                     candle)

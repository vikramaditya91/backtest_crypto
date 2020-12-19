from __future__ import annotations
import logging
import datetime
import xarray as xr
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from backtest_crypto.utilities.general import Singleton
from crypto_history.utilities.general_utilities import register_factory

logger = logging.getLogger(__package__)


class AbstractRawHistoryObtainCreator(ABC):
    """Abstract disk-writer creator"""
    @abstractmethod
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        """factory method to create the disk-writer"""
        pass

    def store_largest_xarray_in_borg(self,
                                     *args,
                                     **kwargs):
        product = self.factory_method(*args, **kwargs)
        product.store_largest_da_on_borg(product.get_fresh_xarray())

    def merge_simplified_history(self,
                                 *args,
                                 **kwargs):
        product = self.factory_method()
        return product.get_merged_histories(*args, **kwargs)

    def get_simple_history(self,
                           *args,
                           **kwargs):
        product = self.factory_method()
        return product.get_simple_history(*args, **kwargs)


@register_factory(section="access_xarray", identifier="sqlite")
class SQLiteCoinHistoryCreator(AbstractRawHistoryObtainCreator):
    """SQLite creator"""
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        return ConcreteSQLiteCoinHistoryAccess(*args, **kwargs)


class ConcreteAbstractCoinHistoryAccess(metaclass=Singleton):
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp_dict = {}

    @abstractmethod
    def get_merged_histories(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_fresh_xarray(self):
        pass

    @abstractmethod
    def get_simple_history(self, *args, **kwargs):
        pass

    def store_largest_da_on_borg(self, dataarray_dict):
        # TODO Certainly better ways to do it
        self.largest_xarray_dict = dataarray_dict

    def get_xarray(self):
        if self.largest_xarray_dict is None:
            dataarray_dict = self.get_fresh_xarray()
            self.store_largest_da_on_borg(dataarray_dict)
        else:
            dataarray_dict = self.largest_xarray_dict
        return dataarray_dict

    def get_timestamps(self,
                       candle):
        if candle not in self.timestamp_dict.keys():
            self.timestamp_dict[candle] = self.largest_xarray_dict[candle].timestamp.values.tolist()
        return self.timestamp_dict[candle]


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
        underlying_np = np.array([[df.values,np.full(df.shape, candle)]])
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
            x_array_dict[candle] = self.df_to_xarray(candle, df)
        return x_array_dict

    def select_history(self,
                       start: datetime.datetime,
                       end: datetime.datetime,
                       dataarray,
                       candle):
        # TODO Potential for improvement here
        timestamp_list = self.get_timestamps(candle)
        starter = start.timestamp() * 1000
        ender = end.timestamp() * 1000
        filtered_timestamps = [item for item in timestamp_list if starter < item < ender]
        selected = dataarray.sel(timestamp=filtered_timestamps)
        return selected

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
                                              self.largest_xarray_dict[candle],
                                              candle=candle)
            sub_histories.append(sub_history)
        sub_histories.append(self.select_history(sub_end,
                                                 start_time,
                                                 self.largest_xarray_dict[remaining],
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
                                   self.largest_xarray_dict[candle],
                                   candle)


def store_largest_xarray(creator: AbstractRawHistoryObtainCreator,
                         overall_start,
                         overall_end,
                         candle,
                         ohlcv_field,
                         *args,
                         **kwargs):
    return creator.store_largest_xarray_in_borg(ohlcv_field,
                                                overall_start,
                                                overall_end,
                                                candle,
                                                *args,
                                                **kwargs)


def get_merged_history(creator: AbstractRawHistoryObtainCreator,
                       *args,
                       **kwargs):
    return creator.merge_simplified_history(*args,
                                            **kwargs)


def get_simple_history(creator: AbstractRawHistoryObtainCreator,
                       *args,
                       **kwargs
                       ):
    return creator.get_simple_history(*args,
                                      **kwargs)

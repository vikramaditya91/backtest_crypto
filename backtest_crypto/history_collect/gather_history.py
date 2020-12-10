from __future__ import annotations
import pathlib
import logging
import datetime
from sqlalchemy.orm import sessionmaker
import xarray as xr
import pandas as pd
import numpy as np
from typing import Union, List
from collections import Counter
from abc import ABC, abstractmethod
from itertools import chain
from sqlalchemy import create_engine
import crypto_oversold
from backtest_crypto.utilities.general import Singleton
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from crypto_history.utilities.general_utilities import register_factory, Borg
from crypto_history.utilities.general_utilities import check_for_write_access

logger = logging.getLogger(__package__)


class AbstractRawHistoryObtainCreator(ABC):
    """Abstract disk-writer creator"""
    @abstractmethod
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        """factory method to create the disk-writer"""
        pass

    def get_split_coin_history(self,
                               *args,
                               **kwargs):
        product = self.factory_method()
        return product.get_split_xarray(*args, **kwargs)

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


@register_factory(section="access_xarray", identifier="web_request")
class WebRequestCoinHistoryCreator(AbstractRawHistoryObtainCreator):
    """JSON creator"""
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        return ConcreteWebRequestCoinHistoryAccess(*args, **kwargs)


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

    @abstractmethod
    def get_merged_histories(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_fresh_xarray(self):
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

    def get_split_xarray(self,
                         current_start,
                         current_end,
                         available=False,
                         masked=False,
                         default="1h"):
        if self.largest_xarray is None:
            dataarray = self.get_fresh_xarray()
            self.store_largest_da_on_borg(dataarray)
        else:
            dataarray = self.largest_xarray

        all_ts = dataarray.timestamp.values.tolist()
        if available is True:
            required_ts = list(
                filter(
                    lambda x: (x > current_start.timestamp() * 1000) & (x < current_end.timestamp() * 1000),
                    all_ts)
            )
            available_da = dataarray.sel(timestamp=required_ts, drop=True)
        else:
            available_da = None

        if masked is True:
            required_ts = list(
                filter(
                    lambda x: x > current_end.timestamp()*1000,
                    all_ts)
            )
            masked_da = dataarray.sel(timestamp=required_ts, drop=True)
        else:
            masked_da = None

        return available_da, masked_da


class ConcreteWebRequestCoinHistoryAccess(ConcreteAbstractCoinHistoryAccess):
    def init_state(self, *args, **kwargs):
        raise NotImplementedError

    def get_fresh_xarray(self):
        raise NotImplementedError


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

    @staticmethod
    def select_history(start,
                       end,
                       dataarray):
        timestamps = list(
            filter(
                lambda x: start.timestamp()*1000 > x > end.timestamp()*1000, dataarray.timestamp.values.tolist())
        )
        return dataarray.sel(timestamp=timestamps)

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
                                              self.largest_xarray_dict[candle])
            sub_histories.append(sub_history)
        sub_histories.append(self.select_history(sub_end,
                                                 start_time,
                                                 self.largest_xarray_dict[remaining]))
        joined_xarray = xr.concat([*sub_histories], dim="timestamp")
        # TODO Raise an error if history is empty
        return joined_xarray.sortby("timestamp")


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


def yield_split_coin_history(creator: AbstractRawHistoryObtainCreator,
                             time_interval_iterator: TimeIntervalIterator,
                             available=True,
                             masked=True
                             ):
    for current_start, current_end in time_interval_iterator.time_intervals:
        yield creator.get_split_coin_history(current_start,
                                             current_end,
                                             available,
                                             masked)


def get_history_between(creator: AbstractRawHistoryObtainCreator,
                        start_time,
                        end_time,
                        available=True,
                        masked=True):
    return creator.get_split_coin_history(start_time,
                                          end_time,
                                          available=available,
                                          masked=masked)


def get_simplified_history(creator: AbstractRawHistoryObtainCreator,
                           *args,
                           **kwargs):
    return creator.merge_simplified_history(*args,
                                            **kwargs)

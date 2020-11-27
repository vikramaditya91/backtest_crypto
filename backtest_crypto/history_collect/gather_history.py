from __future__ import annotations
import pathlib
import logging
import datetime
from sqlalchemy.orm import sessionmaker
import xarray as xr
import pandas as pd
from typing import Union, List
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
import crypto_oversold
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


class Singleton (type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ConcreteAbstractCoinHistoryAccess(metaclass=Singleton):
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def get_fresh_xarray(self):
        pass

    def store_largest_da_on_borg(self, dataarray):
        # TODO Certainly better ways to do it
        self.largest_xarray = dataarray

    def get_xarray(self):
        if self.largest_xarray is None:
            dataarray = self.get_fresh_xarray()
            self.store_largest_da_on_borg(dataarray)
        else:
            dataarray = self.largest_xarray
        return dataarray

    def available_ts(self,
                     timestamp,
                     current_start,
                     current_end):
        return (timestamp > current_start.timestamp() * 1000) & \
               (timestamp < current_end.timestamp() * 1000)

    def masked_ts(self,
                  timestamp,
                  current_end):
        return timestamp > current_end.timestamp()*1000

    def get_split_xarray(self,
                         current_start,
                         current_end,
                         available=False,
                         masked=False):
        if self.largest_xarray is None:
            dataarray = self.get_fresh_xarray()
            self.store_largest_da_on_borg(dataarray)
        else:
            dataarray = self.largest_xarray

        if available is True:
            available_da = dataarray.where(self.available_ts(dataarray.timestamp,
                                                     current_start,
                                                     current_end), drop=True)
        else:
            available_da = None

        if masked is True:
            masked_da = dataarray.where(self.masked_ts(dataarray.timestamp,
                                                     current_end), drop=True)
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
                 overall_start,
                 overall_end,
                 olhcv_field,
                 candle,
                 reference_coin,
                 file_path,
                 mapped_class,
                 table_name
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
        self.table_name = table_name

    def get_df(self):
        raw_df = pd.read_sql_table(self.table_name, con=self.engine)
        return raw_df.set_index('timestamp', drop=True)

    def df_to_xarray(self,
                     df):
        dataarray = xr.DataArray(df)
        dataarray = dataarray.rename({"dim_1": "base_assets"})
        full_da = dataarray.expand_dims({"reference_assets": [self.reference_coin],
                                         "ohlcv_fields": [self.ohlcv_field, "weight"]})
        # For some reason it is not WRITEABLE when dimensions expanded
        return full_da.copy()

    def substitute_weight_value(self,
                                dataarray):
        dataarray.loc[{"ohlcv_fields": "weight"}] = self.candle
        return dataarray

    def get_fresh_xarray(self):
        df = self.get_df()
        da = self.df_to_xarray(df)
        return self.substitute_weight_value(da)


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

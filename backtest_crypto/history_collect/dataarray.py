from __future__ import annotations
import pathlib
import logging
from sqlalchemy.orm import sessionmaker
import xarray as xr
import pandas as pd
from typing import Union, List
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from crypto_history.utilities.general_utilities import register_factory
from crypto_history.utilities.general_utilities import check_for_write_access

logger = logging.getLogger(__package__)


class AbstractObtainCoinHistoryCreator(ABC):
    """Abstract disk-writer creator"""
    @abstractmethod
    def factory_method(self, *args, **kwargs):
        """factory method to create the disk-writer"""
        pass

    def get_coin_history(self,
                         start_time,
                         end_time,
                         ):
        pass


@register_factory(section="acess_xarray", identifier="web_request")
class WebRequestCoinHistoryCreator(AbstractObtainCoinHistoryCreator):
    """JSON creator"""
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        pass


@register_factory(section="acess_xarray", identifier="sqlite")
class SQLiteCoinHistoryCreator(AbstractObtainCoinHistoryCreator):
    """SQLite creator"""
    def factory_method(self, *args, **kwargs) -> ConcreteAbstractCoinHistoryAccess:
        pass


class ConcreteAbstractCoinHistoryAccess(ABC):
    pass


class ConcreteWebRequestCoinHistoryAccess(ConcreteAbstractCoinHistoryAccess):
    pass


class ConcreteSQLiteCoinHistoryAccess(ConcreteAbstractCoinHistoryAccess):
    pass


def yield_coin_history(creator: AbstractObtainCoinHistoryCreator,
                       time_interval,
                       *args, **kwargs):
    pass


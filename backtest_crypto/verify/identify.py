from abc import ABC, abstractmethod
import pandas as pd
from crypto_history.utilities.general_utilities import Borg
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from collections import namedtuple
import asyncio
import crypto_history
from pprint import pprint
from datetime import timedelta
from crypto_oversold import class_builders, init_logger
from backtest_crypto.history_collect.gather_history import AbstractRawHistoryObtainCreator, get_history_between
from crypto_oversold.core_calc import candle_independent, \
    identify_oversold, normalize_by_all_tickers, preprocess_oversold_calc
from crypto_oversold.emit_data.save_to_disk import write_oversold_da_to_file
from crypto_oversold.raw_history.access_raw_history import XArrayWebRequest, get_xarray_dataarray
import logging
import tempfile

logger = logging.getLogger(__name__)

class AbstractIdentifyCreator(ABC):
    @abstractmethod
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def get_coins_from_df(self,
                          time_interval_iterator,
                          data_source_general,
                          start_time,
                          end_time,
                          data_source_specific,
                          *args,
                          **kwargs):
        concrete = self.factory_method(time_interval_iterator,
                                       data_source_general,
                                       data_source_specific)
        return concrete.get_value_of_df_at(start_time,
                                           end_time,
                                           *args,
                                           **kwargs)


class CryptoOversoldCreator(AbstractIdentifyCreator):
    def factory_method(self, *args, **kwargs):
        return ConcreteCryptoOversoldIdentify(*args, **kwargs)


class AbstractConcreteIdentify(ABC, Borg):
    def __init__(self,
                 time_interval_iterator,
                 data_source_general,
                 data_source_specific):
        super().__init__()

        if not self._shared_state:
            self.multi_index_series = self.initialize_series(time_interval_iterator)
        self.PotentialCoin = None
        self.data_source_specific = data_source_specific
        self.data_source_general = data_source_general

    @staticmethod
    def get_multi_index(time_interval_iterator,
                        ):
        time_intervals = time_interval_iterator.time_intervals
        start_list, end_list = zip(*time_intervals)
        return pd.MultiIndex.from_product([list(set(start_list)),
                                           list(set(end_list))],
                                          names=['start_time',
                                                 'end_time'])

    def initialize_series(self,
                          time_interval_iterator):
        return pd.Series(index=self.get_multi_index(time_interval_iterator))

    def get_value_of_df_at(self,
                           start_time,
                           end_time,
                           *args,
                           **kwargs):
        oversold_coin = self.PotentialCoin(*args, **kwargs)
        start_levels, end_levels = self.multi_index_series.index.levels
        assert start_time in start_levels, \
            f"{start_time} not in the multi-index"
        assert end_time in end_levels, \
            f"{end_time} not in the multi-index"
        if pd.isnull(
                self.multi_index_series[start_time][end_time]
        ):
            self.multi_index_series[start_time, end_time] = [{oversold_coin: self.get_potential_coins_for(start_time, end_time, **kwargs)}]
        else:
            if isinstance(self.multi_index_series[start_time, end_time], list):
                if oversold_coin not in self.multi_index_series[start_time][end_time][0].keys():
                    self.multi_index_series[start_time, end_time].update(
                        {
                            oversold_coin:
                                self.get_potential_coins_for(**kwargs)
                        }
                    )
        return self.multi_index_series[start_time, end_time][0][oversold_coin]

    @abstractmethod
    def get_potential_coins_for(self, *args, **kwargs):
        raise NotImplementedError


class ConcreteCryptoOversoldIdentify(AbstractConcreteIdentify):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.PotentialCoin = namedtuple('OversoldCoin',
                                       ["lower_cutoff",
                                        "higher_cutoff",
                                        "reference_coin",
                                        "ohlcv_field"]
                                       )

    def get_potential_coins_for(self,
                                start_time,
                                end_time,
                                lower_cutoff,
                                higher_cutoff,
                                reference_coin,
                                ohlcv_field,
                                ):
        access_creator = class_builders.get("access_xarray").get(self.data_source_general)()

        available_da, _ = get_history_between(access_creator,
                                              start_time,
                                              end_time,
                                              available=True,
                                              masked=False)
        if available_da.timestamp.__len__() == 0:
            return []
        normalized_field = f"{ohlcv_field}_normalized_by_weight"

        pre_processed_instance = preprocess_oversold_calc. \
            ReformatForOversoldCalc(exchange=self.data_source_specific,
                                    timestamp_drop_fraction=0.5,
                                    coin_drop_fraction=0.975)

        pre_processed_da = pre_processed_instance.perform_cleaning_operations(available_da,
                                                                              cleaners=["remove_futures",
                                                                                        "type_convert_datarray",
                                                                                        "entire_na_column_removal",
                                                                                        "remove_coins_with_missing_data",
                                                                                        "drop_coins_ending_latest_nan",
                                                                                        "remove_largely_invalid_ts",
                                                                                        "remove_null_rows_absolute"])
        logger.debug(f"The dataarray in the unmasked history has been pre-processed for {start_time} {end_time}")

        candle_independent_instance = candle_independent.CandleIndependence. \
            create_candle_independent_instance(pre_processed_da)
        candle_independent_da = candle_independent_instance.get_values_candle_independent(
            weight_average_tuple=(ohlcv_field,)
        )
        logger.debug(f"The candle independent value is calculated for {start_time} {end_time}")

        normalized_by_weight = candle_independent_instance. \
            normalize_da_by_weight(candle_independent_da,
                                   to_normalize=(ohlcv_field,)
                                   )

        normalize_against_tickers_instance = normalize_by_all_tickers.NormalizeAgainstTickers()
        normalized_ds = normalize_against_tickers_instance. \
            normalize_against_other_coins(
                normalized_by_weight,
                to_normalize=(normalized_field,)
            )

        identify_latest_oversold = identify_oversold.IdentifyOversold(normalized_ds)
        oversold_coins = identify_latest_oversold. \
            identify_latest_oversold(
                data_var=normalized_field,
                upper_limit=higher_cutoff
            )
        logger.debug(f"Oversold coins are: {oversold_coins}")
        return oversold_coins.base_assets.values.tolist()


def get_potential_coin_at(creator: AbstractIdentifyCreator,
                          time_interval_iterator: TimeIntervalIterator,
                          data_source_general: str,
                          start_time,
                          end_time,
                          data_source_specific,
                          *args,
                          **kwargs):
    return creator.get_coins_from_df(time_interval_iterator,
                                     data_source_general,
                                     start_time,
                                     end_time,
                                     data_source_specific,
                                     **kwargs)

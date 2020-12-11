import logging
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import timedelta

import pandas as pd
from crypto_history.utilities.general_utilities import Borg
from crypto_oversold import class_builders
from crypto_oversold.core_calc import candle_independent, \
    identify_oversold, normalize_by_all_tickers, preprocess_oversold_calc

from backtest_crypto.history_collect.gather_history import get_simplified_history
from backtest_crypto.utilities.general import InsufficientHistory
from backtest_crypto.utilities.iterators import TimeIntervalIterator

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

    def obtain_all_potential_combinations(self,
                                          time_interval_iterator,
                                          data_source_general,
                                          data_source_specific):
        concrete = self.factory_method(time_interval_iterator,
                                       data_source_general,
                                       data_source_specific)
        return concrete.multi_index_series_all_coins


class CryptoOversoldCreator(AbstractIdentifyCreator):
    def factory_method(self, *args, **kwargs):
        return ConcreteCryptoOversoldIdentify(*args, **kwargs)


class AbstractConcreteIdentify(ABC):
    _shared_state = {}

    def __init__(self,
                 time_interval_iterator,
                 data_source_general,
                 data_source_specific):
        self.__dict__ = self._shared_state
        if not self._shared_state:
            self.multi_index_series_oversold_coins = self.initialize_series(time_interval_iterator)
            self.multi_index_series_all_coins = self.initialize_series(time_interval_iterator)
        self.OversoldCoin = None
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

    def get_last_ts_coin_dict_in_current_run(self,
                                             start_time,
                                             end_time,
                                             **kwargs):
        if pd.isnull(
                self.multi_index_series_all_coins[start_time][end_time]
        ):
            last_ts_coins_dict = self.get_dictionary_of_all_coins(start_time,
                                                                  end_time,
                                                                  **kwargs)

            self.multi_index_series_all_coins[start_time, end_time] = [last_ts_coins_dict]
        else:
            last_ts_coins_dict = self.multi_index_series_all_coins[start_time][end_time][0]
        return last_ts_coins_dict

    def get_value_of_df_at(self,
                           start_time,
                           end_time,
                           *args,
                           **kwargs):
        oversold_coin = self.OversoldCoin(*args, **kwargs)
        start_levels, end_levels = self.multi_index_series_oversold_coins.index.levels
        assert start_time in start_levels, \
            f"{start_time} not in the multi-index"
        assert end_time in end_levels, \
            f"{end_time} not in the multi-index"
        if pd.isnull(
                self.multi_index_series_oversold_coins[start_time, end_time]
        ):
            if isinstance(self.multi_index_series_oversold_coins[start_time, end_time], list):
                if oversold_coin in self.multi_index_series_oversold_coins[start_time][end_time][0].keys():
                    return self.multi_index_series_oversold_coins[start_time, end_time][0][oversold_coin]

        last_ts_coins_dict = self.get_last_ts_coin_dict_in_current_run(start_time,
                                                                       end_time,
                                                                       **kwargs)

        list_of_oversold_coins = self.filter_required_coins_from_all_coin_dict(last_ts_coins_dict,
                                                                               **kwargs)

        if isinstance(self.multi_index_series_oversold_coins[start_time, end_time], list):
            self.multi_index_series_oversold_coins[start_time, end_time][0].update(
                {oversold_coin: list_of_oversold_coins}
            )
        else:
            self.multi_index_series_oversold_coins[start_time, end_time] = [{oversold_coin: list_of_oversold_coins}]
        return self.multi_index_series_oversold_coins[start_time, end_time][0][oversold_coin]

    @abstractmethod
    def get_potential_value_of_all_coins(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def filter_required_coins_from_all_coin_dict(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def get_dictionary_of_all_coins(self, *args, **kwargs):
        raise NotImplementedError


class ConcreteCryptoOversoldIdentify(AbstractConcreteIdentify):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.OversoldCoin = namedtuple('OversoldCoin',
                                       ["lower_cutoff",
                                        "higher_cutoff",
                                        "reference_coin",
                                        "ohlcv_field"]
                                       )

    def get_potential_value_of_all_coins(self,
                                         start_time,
                                         end_time,
                                         lower_cutoff,
                                         higher_cutoff,
                                         reference_coin,
                                         ohlcv_field):
        access_creator = class_builders.get("access_xarray").get(self.data_source_general)()

        available_da = get_simplified_history(access_creator,
                                              start_time,
                                              end_time,
                                              backward_details=((timedelta(days=0), -timedelta(days=2), "1h"),),
                                              remaining="1d")

        if available_da.timestamp.__len__() == 0:
            raise InsufficientHistory
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
        dataset_normalized_coins = normalize_against_tickers_instance. \
            normalize_against_other_coins(
                normalized_by_weight,
                to_normalize=(normalized_field,)
            )
        return identify_oversold.IdentifyOversold.get_last_timestamp_values(dataset_normalized_coins,
                                                                            normalized_field)

    def get_dictionary_of_all_coins(self,
                                    *args,
                                    **kwargs):
        last_ts_coins = self.get_potential_value_of_all_coins(*args, **kwargs)
        if last_ts_coins is not None:
            return identify_oversold.IdentifyOversold. \
                get_dictionary_of_last_ts_all_coins(last_ts_coins)
        else:
            return []

    def filter_required_coins_from_all_coin_dict(self,
                                                 dictionary_of_all_coins,
                                                 lower_cutoff,
                                                 higher_cutoff,
                                                 reference_coin,
                                                 ohlcv_field,
                                                 ):
        if dictionary_of_all_coins:
            return identify_oversold.IdentifyOversold.filter_coins_dict_by_limit(dictionary_of_all_coins,
                                                                                 higher_cutoff,
                                                                                 lower_cutoff)


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


def get_complete_potential_coins_all_combinations(creator: AbstractIdentifyCreator,
                                                  time_interval_iterator,
                                                  data_source_general,
                                                  data_source_specific):
    return creator.obtain_all_potential_combinations(time_interval_iterator,
                                                     data_source_general,
                                                     data_source_specific)

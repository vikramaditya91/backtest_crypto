from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import timedelta
from typing import Tuple, Dict

import pandas as pd
from crypto_oversold import class_builders
from crypto_oversold.core_calc import candle_independent, \
    identify_oversold, normalize_by_all_tickers, preprocess_oversold_calc

from backtest_crypto.history_collect.gather_history import get_merged_history
from backtest_crypto.utilities.data_structs import time_interval_iterator_to_pd_multiindex
from backtest_crypto.utilities.general import InsufficientHistory, MissingPotentialCoinTimeIndexError

logger = logging.getLogger(__name__)


class PotentialNamedTuple:
    @staticmethod
    def get_tuple_instance(**potential_coin_strategy):
        potential_coin = namedtuple('PotentialCoin',
                                    list(potential_coin_strategy.keys())
                                    )
        return potential_coin(**potential_coin_strategy)


class MultiIndexPotential(pd.DataFrame):
    @classmethod
    def load_pickled(cls,
                     pickled_potential_coin_path,
                     ):
        pickled_series = pd.read_pickle(pickled_potential_coin_path)
        df = cls(pickled_series)
        df["potential"] = None
        return df

    @classmethod
    def initialize_series(cls,
                          time_interval_iterator):
        return cls(index=time_interval_iterator_to_pd_multiindex(time_interval_iterator),
                   columns=["all", "potential"])


class PotentialCoinClient:
    _shared_state = {}

    def __init__(self,
                 time_interval_iterator,
                 potential_calc_creator: AbstractIdentifyCreator,
                 data_source,
                 pickled_potential_coin_path=None
                 ):
        self.__dict__ = self._shared_state
        if pickled_potential_coin_path is not None:
            self.multi_index_df = MultiIndexPotential.load_pickled(pickled_potential_coin_path)
        if not self._shared_state:
            self.multi_index_df = MultiIndexPotential.initialize_series(time_interval_iterator)
        self.potential_calc_creator = potential_calc_creator
        self.data_source_general, self.data_source_specific = data_source

    def does_potential_coin_exist_in_object(self,
                                            history_start,
                                            history_end,
                                            potential_coin):
        if pd.isnull(
                self.multi_index_df["potential"][history_start, history_end]
        ):
            if isinstance(self.multi_index_df["potential"][history_start, history_end], list):
                return potential_coin in self.multi_index_df["potential"][history_start][history_end][0].keys()
        return False

    def filter_potential(self,
                         original_dict,
                         potential_coin_strategy,
                         ):
        lower_cutoff, higher_cutoff = self.get_low_high_cutoff(potential_coin_strategy)
        return dict(filter(lambda x: lower_cutoff < x[1] < higher_cutoff, original_dict.items()))

    def update_potential_coin_location(self,
                                       history_start,
                                       history_end,
                                       potential_coin,
                                       potential_coin_strategy):
        if pd.isnull(
                self.multi_index_df["all"][history_start, history_end]
        ):
            self.update_potential_value_for_all_coins(history_start,
                                                      history_end,
                                                      potential_coin_strategy)
        all_coins_dict = self.multi_index_df["all"][history_start, history_end][0]

        dict_of_potential_coins = self.filter_potential(all_coins_dict,
                                                        potential_coin_strategy)

        if isinstance(self.multi_index_df["potential"][history_start, history_end], list):
            self.multi_index_df["potential"][history_start, history_end][0].update(
                {potential_coin: dict_of_potential_coins}
            )
        else:
            self.multi_index_df["potential"][history_start, history_end] = [
                {potential_coin: dict_of_potential_coins}]

    @staticmethod
    def get_low_high_cutoff(potential_coin_strategy: Dict) -> Tuple[float, float]:
        if ("low_cutoff" in potential_coin_strategy.keys()) and \
                ("high_cutoff" in potential_coin_strategy.keys()):
            low_cutoff = potential_coin_strategy["low_cutoff"]
            high_cutoff = potential_coin_strategy["high_cutoff"]
        elif ("cutoff_mean" in potential_coin_strategy.keys()) and \
                ("cutoff_deviation" in potential_coin_strategy.keys()):
            low_cutoff = potential_coin_strategy["cutoff_mean"] - potential_coin_strategy["cutoff_deviation"]
            high_cutoff = potential_coin_strategy["cutoff_mean"] + potential_coin_strategy["cutoff_deviation"]
        else:
            raise ValueError("Low and high-cutoffs indeterminate")
        return low_cutoff, high_cutoff

    def get_potential_strategy_tuple(self,
                                     potential_coin_strategy: Dict):
        low_cutoff, high_cutoff = self.get_low_high_cutoff(potential_coin_strategy)
        potential_input_dict = dict(low_cutoff=low_cutoff,
                                    high_cutoff=high_cutoff,
                                    reference_coin=potential_coin_strategy["reference_coin"],
                                    ohlcv_field=potential_coin_strategy["ohlcv_field"])
        return PotentialNamedTuple.get_tuple_instance(**potential_input_dict)

    def get_potential_coin_at(self,
                              consider_history,
                              potential_coin_strategy,
                              ):
        instance_potential_strategy = self.get_potential_strategy_tuple(potential_coin_strategy)
        history_start, history_end = consider_history
        try:
            self.multi_index_df["all"][history_start, history_end]
        except KeyError as e:
            raise MissingPotentialCoinTimeIndexError
        if not self.does_potential_coin_exist_in_object(history_start,
                                                        history_end,
                                                        instance_potential_strategy):
            self.update_potential_coin_location(history_start,
                                                history_end,
                                                instance_potential_strategy,
                                                potential_coin_strategy)
        return self.multi_index_df["potential"][history_start, history_end][0][instance_potential_strategy]

    def get_complete_potential_coins_all_combinations(self):
        return self.multi_index_df["all"]

    def update_potential_value_for_all_coins(self,
                                             start_time,
                                             end_time,
                                             potential_coin_strategy):
        self.multi_index_df["all"][start_time, end_time] = [
            self.get_dictionary_of_all_coins_fresh(start_time,
                                                   end_time,
                                                   potential_coin_strategy)
        ]

    def get_dictionary_of_all_coins_fresh(self,
                                          start_time,
                                          end_time,
                                          potential_coin_strategy):
        return self.potential_calc_creator.get_dict_of_all_coins(
            self.data_source_general,
            self.data_source_specific,
            start_time,
            end_time,
            potential_coin_strategy,
        )


class AbstractIdentifyCreator(ABC):
    @abstractmethod
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def get_dict_of_all_coins(self,
                              data_source_general,
                              data_source_specific,
                              history_start,
                              history_end,
                              potential_coin_strategy,
                              ):
        concrete = self.factory_method(data_source_general,
                                       data_source_specific)
        return concrete.get_potential_dict_of_all_coins(history_start,
                                                        history_end,
                                                        potential_coin_strategy)


class CryptoOversoldCreator(AbstractIdentifyCreator):
    def factory_method(self, *args, **kwargs):
        return ConcreteCryptoOversoldIdentify(*args, **kwargs)


class AbstractConcreteIdentify(ABC):
    def __init__(self,
                 data_source_general,
                 data_source_specific):
        self.data_source_specific = data_source_specific
        self.data_source_general = data_source_general

    @abstractmethod
    def all_coins_potential_at_history_end(self, *args, **kwargs):
        raise NotImplementedError


class ConcreteCryptoOversoldIdentify(AbstractConcreteIdentify):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_potential_dict_of_all_coins(self,
                                        history_start,
                                        history_end,
                                        potential_coin_strategy):
        ds = self.all_coins_potential_at_history_end(history_start,
                                                     history_end,
                                                     potential_coin_strategy)
        return identify_oversold.IdentifyOversold.get_dictionary_of_last_ts_all_coins(ds)

    def all_coins_potential_at_history_end(self,
                                           history_start,
                                           history_end,
                                           potential_coin_strategy):
        access_creator = class_builders.get("access_xarray").get(self.data_source_general)()

        available_da = get_merged_history(access_creator,
                                          history_start,
                                          history_end,
                                          backward_details=((timedelta(days=0), -timedelta(days=2), "1h"),),
                                          remaining="1d")

        if available_da.timestamp.__len__() == 0:
            raise InsufficientHistory

        ohlcv_field = potential_coin_strategy["ohlcv_field"]
        normalized_field = f"{ohlcv_field}_normalized_by_weight"

        pre_processed_instance = preprocess_oversold_calc. \
            ReformatForOversoldCalc(exchange=self.data_source_specific,
                                    timestamp_drop_fraction=0.5,
                                    coin_drop_fraction=0.975)

        pre_processed_da = pre_processed_instance.perform_cleaning_operations(
            available_da,
            cleaners=["remove_futures",
                      "type_convert_datarray",
                      "entire_na_column_removal",
                      "remove_coins_with_missing_data",
                      "drop_coins_ending_latest_nan",
                      "remove_largely_invalid_ts",
                      "remove_null_rows_absolute"]
        )
        logger.debug(f"The dataarray in the unmasked history has been pre-processed for {history_start} {history_end}")

        candle_independent_instance = candle_independent.CandleIndependence. \
            create_candle_independent_instance(pre_processed_da)
        candle_independent_da = candle_independent_instance.get_values_candle_independent(
            weight_average_tuple=(ohlcv_field,)
        )
        logger.debug(f"The candle independent value is calculated for {history_start} {history_end}")

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

import itertools
import logging

import xarray as xr

from backtest_crypto.utilities.general import InsufficientHistory
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from backtest_crypto.verify.identify_potential_coins import CryptoOversoldCreator, \
    PotentialCoinClient
from backtest_crypto.verify.simulate_success import validate_success, MarketBuyLimitSellCreator

logger = logging.getLogger(__name__)


class GatherGeneral:
    def __init__(self,
                 data_accessor,
                 data_source_general,
                 data_source_specific,
                 reference_coin,
                 ohlcv_field,
                 time_interval_iterator,
                 source_iterators,
                 success_iterators,
                 target_iterators,
                 ):
        self.data_accessor = data_accessor
        self.data_source_general = data_source_general
        self.data_source_specific = data_source_specific
        self.reference_coin = reference_coin
        self.ohlcv_field = ohlcv_field
        self.time_interval_iterator = time_interval_iterator
        self.success_iterators = success_iterators
        self.source_iterators = source_iterators
        self.target_iterators = target_iterators
        self.time_interval_coordinate = "time_intervals"

    def get_coords_for_dataset(self):
        coordinates = [(self.time_interval_coordinate,
                        self.time_interval_iterator.get_list_time_intervals_str())]
        for success in self.success_iterators:
            coordinates.append((success.__name__, success()))
        for source in self.source_iterators:
            coordinates.append((source.__name__, source()))
        return coordinates

    def obtain_potential(self,
                         potential_coin_client,
                         coordinate_dict,
                         potential_start,
                         potential_end):
        potential_coin_strategy = dict(low_cutoff=coordinate_dict["low_cutoff"],
                                       high_cutoff=coordinate_dict["high_cutoff"],
                                       reference_coin=self.reference_coin,
                                       ohlcv_field=self.ohlcv_field)
        consider_history = (potential_start, potential_end)
        return potential_coin_client.get_potential_coin_at(
            consider_history=consider_history,
            potential_coin_strategy=potential_coin_strategy,
        )

    def yield_tuple_strategy(self):
        coordinates = self.get_coords_for_dataset()
        for item in itertools.product(*(dict(coordinates).values())):
            yield item


class GatherPotential(GatherGeneral):
    def store_potential_coins_pickled(self,
                                      narrowed_start_time,
                                      narrowed_end_time,
                                      pickled_file_path):
        data_source = (self.data_source_general, self.data_source_specific)
        potential_coin_client = PotentialCoinClient(self.time_interval_iterator,
                                                    CryptoOversoldCreator(),
                                                    data_source,
                                                    )
        coordinate_keys = dict(self.get_coords_for_dataset()).keys()
        for tuple_strategy in self.yield_tuple_strategy():
            coordinate_dict = dict(zip(coordinate_keys, tuple_strategy))
            string_start_end = coordinate_dict["time_intervals"]

            history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                string_start_end
            )
            if narrowed_end_time >= history_end:
                if history_end >= narrowed_start_time:
                    try:
                        self.obtain_potential(potential_coin_client,
                                              coordinate_dict,
                                              history_start,
                                              history_end)
                    except InsufficientHistory:
                        logger.warning(f"Insufficient history for {history_start} to {history_end}")
        pandas_series = potential_coin_client.get_complete_potential_coins_all_combinations()
        pandas_series.to_pickle(pickled_file_path)


class GatherSuccess(GatherGeneral):
    """
    Collects the various time-stamps, gets potential coins and simulates them
    """

    def __init__(self, *args, **kwargs):
        super(GatherSuccess, self).__init__(*args, **kwargs)
        # TODO The intialization of potential coin to disk does not need this?
        self.gathered_dataset = self.initialize_success_dataset()

    def initialize_success_dataarray(self):
        return xr.DataArray(None, coords=self.get_coords_for_dataset())

    def initialize_success_dataset(self):
        nan_da = self.initialize_success_dataarray()
        dataset = xr.Dataset(dict(map(lambda data_var: (data_var, nan_da), self.target_iterators)))
        for data_variable in dataset:
            dataset[data_variable] = dataset[data_variable].copy()
        return dataset

    def get_simulation_arguments(self,
                                 coords):
        success_dict = {}
        for item in self.success_iterators:
            if item.__name__ == "days_to_run":
                success_dict["days_to_run"] = TimeIntervalIterator.numpy_dt_to_timedelta(
                    coords["days_to_run"].values
                )
            else:
                success_dict[item.__name__] = coords[item.__name__].values.tolist()
        return success_dict

    def set_success_with_calc(self,
                              simulation_input_dict,
                              potential_coins,
                              potential_end,
                              simulation_timedelta):
        success_dict = validate_success(MarketBuyLimitSellCreator(),
                                        self.data_accessor,
                                        potential_coins,
                                        potential_end,
                                        simulation_timedelta=simulation_timedelta,
                                        success_criteria=self.target_iterators,
                                        ohlcv_field=self.ohlcv_field,
                                        simulation_input_dict=simulation_input_dict
                                        )

        self.set_success_in_dataset(success_dict,
                                    simulation_input_dict)

    def overall_success_calculator(self,
                                   narrowed_start_time,
                                   narrowed_end_time,
                                   loaded_potential_coins=None):
        data_source = (self.data_source_general, self.data_source_specific)
        potential_coin_client = PotentialCoinClient(
            self.time_interval_iterator,
            CryptoOversoldCreator(),
            data_source,
            loaded_potential_coins,
        )

        coordinate_keys = dict(self.get_coords_for_dataset()).keys()

        # TODO Remove debug lines
        count = 0
        potential = 0
        simulate = 0
        for tuple_strategy in self.yield_tuple_strategy():
            count += 1
            coordinate_dict = dict(zip(coordinate_keys, tuple_strategy))
            string_start_end = coordinate_dict["time_intervals"]

            history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                string_start_end
            )
            if narrowed_end_time >= history_end:
                if history_end >= narrowed_start_time:
                    try:
                        import time
                        first = time.time()
                        potential_coins = self.obtain_potential(potential_coin_client,
                                                                coordinate_dict,
                                                                history_start,
                                                                history_end)
                        second = time.time()
                        potential = potential + second -first
                        simulation_timedelta = coordinate_dict["days_to_run"]
                        self.set_success_with_calc(coordinate_dict,
                                                   potential_coins,
                                                   history_end,
                                                   simulation_timedelta)
                        third = time.time()
                        simulate = simulate + third - second

                    except InsufficientHistory:
                        logger.warning(f"Insufficient history for {history_start} "
                                       f"to {history_end}")
                    if count%2000 == 0:
                        logger.info(f"Got potential coins in {potential/count}")
                        logger.info(f"Got simulate coins in {simulate/count}")

        return self.gathered_dataset

    def set_success_in_dataset(self,
                               success_dict,
                               success_input_dict):
        for success_criteria, success in success_dict.items():
            self.gathered_dataset[success_criteria].loc[success_input_dict] = success

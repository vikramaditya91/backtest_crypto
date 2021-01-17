import itertools
import logging
from abc import ABC, abstractmethod

import xarray as xr

from backtest_crypto.utilities.general import InsufficientHistory, MissingPotentialCoinTimeIndexError
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from backtest_crypto.verify.identify_potential_coins import CryptoOversoldCreator, PotentialCoinClient
from backtest_crypto.verify.individual_indicator_calculator import calculate_indicator
from backtest_crypto.verify.simulate_timesteps import calculate_simulation

logger = logging.getLogger(__name__)


class GatherAbstract(ABC):
    def __init__(self,
                 data_accessor,
                 data_source,
                 reference_coin,
                 ohlcv_field,
                 iterators,
                 ):
        self.data_accessor = data_accessor
        self.data_source_general, self.data_source_specific = data_source
        self.reference_coin = reference_coin
        self.ohlcv_field = ohlcv_field
        self.time_interval_iterator = iterators["time"]
        self.success_iterators = iterators["success"]
        self.source_iterators = iterators["source"]
        self.target_iterators = iterators["target"]
        self.strategy_iterators = iterators["strategy"]
        self.do_not_sort_list = ["days_to_run"]

    @abstractmethod
    def get_coords_for_dataset(self):
        pass

    def yield_tuple_strategy(self):
        coordinates = self.get_coords_for_dataset()
        for key, values in coordinates:
            if key not in self.do_not_sort_list:
                values.sort()

        first_item = None
        for item in itertools.product(*(dict(coordinates).values())):
            if first_item != item[0]:
                try:
                    history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                        item[0]
                    )
                    logger.info(f"Updating the first item: {history_start} to {history_end}")
                except Exception:
                    logger.info(f"Updating the first item: {item[0]}")
                first_item = item[0]
            yield item

    def initialize_success_dataarray(self):
        return xr.DataArray(None, coords=self.get_coords_for_dataset())

    def initialize_success_dataset(self):
        nan_da = self.initialize_success_dataarray()
        dataset = xr.Dataset(dict(map(lambda data_var: (data_var, nan_da), self.target_iterators)))
        for data_variable in dataset:
            dataset[data_variable] = dataset[data_variable].copy()
        return dataset


class GatherPotential(GatherAbstract):
    def get_coords_for_dataset(self):
        coordinates = [("time_intervals", self.time_interval_iterator.get_list_time_intervals_str())]
        for success in self.success_iterators:
            coordinates.append((success.__name__, success()))
        for source in self.source_iterators:
            coordinates.append((source.__name__, source()))
        return coordinates

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
                        potential_coin_client.get_potential_coin_at(
                            consider_history=(history_start, history_end),
                            potential_coin_strategy={**coordinate_dict,
                                                     "ohlcv_field": self.ohlcv_field,
                                                     "reference_coin": self.reference_coin}
                        )
                    except InsufficientHistory:
                        logger.warning(f"Insufficient history for {history_start} to {history_end}")
        pandas_series = potential_coin_client.get_complete_potential_coins_all_combinations()
        pandas_series.to_pickle(pickled_file_path)


class GatherSimulation(GatherAbstract):
    def __init__(self, *args, **kwargs):
        super(GatherSimulation, self).__init__(*args, **kwargs)
        self.gathered_dataset = self.initialize_success_dataset()

    def get_coords_for_dataset(self):
        coordinates = [("time_intervals", self.time_interval_iterator.get_list_time_intervals_str()),
                       ("strategy", list(strategy for strategy in self.strategy_iterators))]
        for success in self.success_iterators:
            coordinates.append((success.__name__, success()))
        for source in self.source_iterators:
            coordinates.append((source.__name__, source()))
        return coordinates

    def simulation_calculator(self,
                              narrowed_start_time,
                              narrowed_end_time,
                              loaded_potential_coins):
        data_source = (self.data_source_general, self.data_source_specific)
        potential_coin_client = PotentialCoinClient(
            self.time_interval_iterator,
            CryptoOversoldCreator(),
            data_source,
            loaded_potential_coins,
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
                        self.simulate_timestep(coordinate_dict,
                                               potential_coin_client)
                    except InsufficientHistory as e:
                        # pass
                        logger.warning(f"Insufficient history for {history_start} "
                                       f"to {history_end}. Reason {e}")
        return self.gathered_dataset

    def simulate_timestep(self,
                          simulation_input_dict,
                          potential_coin_client
                          ):
        strategy = simulation_input_dict.pop("strategy")()
        simulate_result_dict = calculate_simulation(strategy,
                                                self.data_accessor,
                                                ohlcv_field=self.ohlcv_field,
                                                simulation_input_dict=simulation_input_dict,
                                                potential_coin_client=potential_coin_client,
                                                simulate_criteria=self.target_iterators,
                                                )

        self.set_simulator_in_dataset(simulate_result_dict,
                                      simulation_input_dict)

    def set_simulator_in_dataset(self,
                                 simulate_result_dict,
                                 success_input_dict
                                 ):
        for simulate_criterion, success in simulate_result_dict.items():
            self.gathered_dataset[simulate_criterion].loc[success_input_dict] = success


class GatherIndicator(GatherAbstract):
    """
    Collects the various time-stamps, gets potential coins and simulates them
    """

    def __init__(self, *args, **kwargs):
        super(GatherIndicator, self).__init__(*args, **kwargs)
        self.gathered_dataset = self.initialize_success_dataset()

    def get_coords_for_dataset(self):
        coordinates = [("time_intervals", self.time_interval_iterator.get_list_time_intervals_str()),
                       ("strategy", list(strategy for strategy in self.strategy_iterators))]
        for success in self.success_iterators:
            coordinates.append((success.__name__, success()))
        for source in self.source_iterators:
            coordinates.append((source.__name__, source()))
        return coordinates

    def indicator_insert(self,
                         simulation_input_dict,
                         potential_coins,
                         potential_end,
                         simulation_timedelta):
        strategy = simulation_input_dict.pop("strategy")()
        success_dict = calculate_indicator(strategy,
                                           self.data_accessor,
                                           potential_coins,
                                           potential_end,
                                           simulation_timedelta=simulation_timedelta,
                                           success_criteria=self.target_iterators,
                                           ohlcv_field=self.ohlcv_field,
                                           simulation_input_dict=simulation_input_dict
                                           )

        self.set_indicator_in_dataset(success_dict,
                                      simulation_input_dict)

    def overall_individual_indicator_calculator(self,
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

        for tuple_strategy in self.yield_tuple_strategy():
            coordinate_dict = dict(zip(coordinate_keys, tuple_strategy))
            string_start_end = coordinate_dict["time_intervals"]
            history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                string_start_end
            )
            if narrowed_end_time >= history_end:
                if history_end >= narrowed_start_time:
                    try:
                        potential_coins = potential_coin_client.get_potential_coin_at(
                            consider_history=(history_start, history_end),
                            potential_coin_strategy={**coordinate_dict,
                                                     "ohlcv_field": self.ohlcv_field,
                                                     "reference_coin": self.reference_coin}
                        )
                    except MissingPotentialCoinTimeIndexError:
                        logger.debug(f"Potential coins are unavailable for {coordinate_dict}")
                        continue

                    simulation_timedelta = coordinate_dict["days_to_run"]
                    self.indicator_insert(coordinate_dict,
                                          potential_coins,
                                          history_end,
                                          simulation_timedelta)

                    # logger.warning(f"Insufficient history for {history_start} "
                    #                f"to {history_end}")
        return self.gathered_dataset

    def set_indicator_in_dataset(self,
                                 success_dict,
                                 success_input_dict):
        for success_criteria, success in success_dict.items():
            self.gathered_dataset[success_criteria].loc[success_input_dict] = success

import itertools
import logging
from abc import ABC, abstractmethod
from typing import List
from multiprocessing import Pool

import xarray as xr

from backtest_crypto.utilities.general import InsufficientHistory, MissingPotentialCoinTimeIndexError
from backtest_crypto.verify.identify_potential_coins import CryptoOversoldCreator, PotentialCoinClient
from backtest_crypto.verify.individual_indicator_calculator import calculate_indicator
from backtest_crypto.verify.simulate_timesteps import calculate_simulation_client

logger = logging.getLogger(__name__)


class GatherAbstract(ABC):
    def __init__(self,
                 full_history_da_dict,
                 reference_coin,
                 ohlcv_field,
                 iterators,
                 potential_coin_path=None
                 ):
        self.reference_coin = reference_coin
        self.ohlcv_field = ohlcv_field
        self.time_interval_iterator = iterators["time"]
        self.success_iterators = iterators["success"]
        self.source_iterators = iterators["source"]
        self.target_iterators = iterators["target"]
        self.strategy_iterators = iterators["strategy"]
        self.do_not_sort_list = ["days_to_run"]
        self.full_history_da_dict = full_history_da_dict
        self.potential_coin_path = potential_coin_path
        self._potential_client = None
        self.pool_count = 8

    @property
    def potential_client(self):
        if self._potential_client is None:
            self._potential_client = PotentialCoinClient(
                self.time_interval_iterator,
                CryptoOversoldCreator(),
                self.full_history_da_dict,
                self.potential_coin_path,
            )
        return self._potential_client

    @abstractmethod
    def get_coords_for_dataset(self):
        pass

    def sort_coordinates(self,
                         coordinates):
        for key, values in coordinates:
            if key not in self.do_not_sort_list:
                values.sort()

    def get_tuple_strategy_wo_time_intervals(self):
        coordinates = self.get_coords_for_dataset()
        self.sort_coordinates(coordinates)
        strategic_items = []
        non_ts_coordinates = [item for item in coordinates if item[0] != "time_intervals"]
        for item in itertools.product(*(dict(non_ts_coordinates).values())):
            strategic_items.append(item)
        return strategic_items

    def initialize_success_dataarray(self):
        return xr.DataArray(None, coords=self.get_coords_for_dataset())

    def initialize_success_dataset(self):
        nan_da = self.initialize_success_dataarray()
        dataset = xr.Dataset(dict(map(lambda data_var: (data_var, nan_da), self.target_iterators)))
        for data_variable in dataset:
            dataset[data_variable] = dataset[data_variable].copy()
        return dataset

    def assemble_dynamic_arguments_for_pool(self,
                                            time_interval,
                                            narrowed_end_time,
                                            narrowed_start_time
                                            ):
        tuple_strategy_list = self.get_tuple_strategy_wo_time_intervals()
        collect_args = []
        for tuple_strategy_wo_ts in tuple_strategy_list:
            coordinate_dict = self.get_coordinate_dict(time_interval,
                                                       tuple_strategy_wo_ts)
            string_start_end = coordinate_dict['time_intervals']
            history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                string_start_end
            )
            if narrowed_end_time >= history_end:
                if history_end >= narrowed_start_time:
                    collect_args.append(coordinate_dict)
        return collect_args

    def get_coordinate_dict(self,
                            time_interval,
                            tuple_strategy
                            ):
        coordinate_keys = dict(self.get_coords_for_dataset()).keys()
        coordinate_dict = dict(zip(coordinate_keys,
                                   (time_interval, *tuple_strategy)))
        coordinate_dict['time_intervals'] = time_interval
        return coordinate_dict

    def yield_time_intervals(self):
        coordinates = self.get_coords_for_dataset()
        self.sort_coordinates(coordinates)
        time_intervals: List = dict(coordinates)['time_intervals']

        first_item = None

        for time_interval in time_intervals:
            if first_item != time_interval:
                try:
                    history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                        time_interval
                    )
                    logger.info(f"Updating the first item: {history_start} to {history_end}")
                except Exception:
                    logger.info(f"Updating the first item: {time_interval}")
                first_item = time_interval
            yield time_interval


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
        for time_interval in self.yield_time_intervals():
            collected_args = self.assemble_dynamic_arguments_for_pool(time_interval,
                                                    narrowed_end_time,
                                                    narrowed_start_time)

            for coordinate_dict in collected_args:
                string_start_end = coordinate_dict["time_intervals"]

                history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                    string_start_end
                )
                if narrowed_end_time >= history_end:
                    if history_end >= narrowed_start_time:
                        try:
                            potential_coin_strategy = {**coordinate_dict,
                                                       "ohlcv_field": self.ohlcv_field,
                                                       "reference_coin": self.reference_coin}
                            instance_potential_strategy = self.potential_client.\
                                get_potential_strategy_tuple(potential_coin_strategy)
                            self.potential_client.update_potential_coin_location(history_start,
                                                                                 history_end,
                                                                                 instance_potential_strategy,
                                                                                 potential_coin_strategy)
                        except InsufficientHistory:
                            logger.warning(f"Insufficient history for {history_start} to {history_end}")
            pandas_series = self.potential_client.get_complete_potential_coins_all_combinations()
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

    def collect_arguments(self,
                          time_interval,
                          narrowed_start_time,
                          narrowed_end_time,
                          ):
        collected_args = self.assemble_dynamic_arguments_for_pool(time_interval,
                                                                  narrowed_end_time,
                                                                  narrowed_start_time)
        collected_args = [(self.ohlcv_field,
                           item,
                           self.potential_client,
                           self.target_iterators,
                           self.full_history_da_dict) for item in collected_args]
        return collected_args

    def simulation_calculator(self,
                              narrowed_start_time,
                              narrowed_end_time,
                              ):
        for time_interval in self.yield_time_intervals():
            collected_args = self.collect_arguments(time_interval,
                                                    narrowed_start_time,
                                                    narrowed_end_time)
            with Pool(self.pool_count) as pool:
                simulation_results = pool.starmap(self.execute_simulation, collected_args)

            self.store_simulation_results(simulation_results,
                                          collected_args)
        return self.gathered_dataset

    def store_simulation_results(self,
                                 simulation_results,
                                 collected_args):
        for sim_result, collected_arg in zip(simulation_results, collected_args):
            self.set_simulator_in_dataset(sim_result,
                                          collected_arg[1])

    @staticmethod
    def execute_simulation(ohlcv_field,
                           coordinate_dict,
                           potential_client,
                           target_iterators,
                           dataarray_dict
                           ):
        try:
            strategy = coordinate_dict.pop("strategy")()
            return calculate_simulation_client(strategy,
                                               ohlcv_field=ohlcv_field,
                                               simulation_input_dict=coordinate_dict,
                                               potential_coin_client=potential_client,
                                               simulate_criteria=target_iterators,
                                               dataarray_dict=dataarray_dict
                                               )
        except InsufficientHistory as e:
            # pass
            logger.warning(f"Insufficient history. Reason {e}")

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
                                           self.full_history_da_dict,
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
                                                narrowed_end_time):
        for time_interval in self.yield_time_intervals():
            collected_args = self.assemble_dynamic_arguments_for_pool(time_interval,
                                                                      narrowed_end_time,
                                                                      narrowed_start_time)

            for coordinate_dict in collected_args:
                string_start_end = coordinate_dict["time_intervals"]
                history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                    string_start_end
                )
                if narrowed_end_time >= history_end:
                    if history_end >= narrowed_start_time:
                        try:
                            potential_coin_strategy = {**coordinate_dict,
                                                       "ohlcv_field": self.ohlcv_field,
                                                       "reference_coin": self.reference_coin}
                            instance_potential_strategy = self.potential_client. \
                                get_potential_strategy_tuple(potential_coin_strategy)
                            self.potential_client.update_potential_coin_location(history_start,
                                                                                 history_end,
                                                                                 instance_potential_strategy,
                                                                                 potential_coin_strategy)
                            potential_coins = self.potential_client.get_potential_coin_at(
                                consider_history=(history_start, history_end),
                                potential_coin_strategy={**coordinate_dict,
                                                         "ohlcv_field": self.ohlcv_field,
                                                         "reference_coin": self.reference_coin}
                            )
                        except InsufficientHistory:
                            logger.warning(f"Insufficient history for {history_start} to {history_end}")
                        else:
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

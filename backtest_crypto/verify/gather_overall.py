import itertools
import logging
import xarray as xr
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from backtest_crypto.verify.identify_potential_coins import CryptoOversoldCreator, \
    PotentialCoinClient
from backtest_crypto.verify.simulate_success import validate_success, MarketBuyLimitSellCreator
from backtest_crypto.utilities.general import InsufficientHistory
from backtest_crypto.utilities.data_structs import coordinates_to_dict

logger = logging.getLogger(__name__)


class Gather:
    """
    Collects the various time-stamps, gets potential coins and simulates them
    """
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
        # TODO The intialization of potential coin to disk does not need this?
        self.gathered_dataset = self.initialize_dataset()

    def get_coords_for_dataset(self):
        coordinates = [(self.time_interval_coordinate,
                        self.time_interval_iterator.get_list_time_intervals_str())]
        for success in self.success_iterators:
            coordinates.append((success.__name__, success()))
        for source in self.source_iterators:
            coordinates.append((source.__name__, source()))
        return coordinates

    def initialize_dataarray(self):
        return xr.DataArray(None, coords=self.get_coords_for_dataset())

    def initialize_dataset(self):
        nan_da = self.initialize_dataarray()
        dataset = xr.Dataset(dict(map(lambda data_var: (data_var, nan_da), self.target_iterators)))
        for data_variable in dataset:
            dataset[data_variable] = dataset[data_variable].copy()
        return dataset

    def yield_coordinates_to_fill(self,
                                  ds: xr.Dataset):
        indexes = ds.indexes
        for coordinate in itertools.product(
                *(indexes[coord] for coord in ds.coords)
        ):
            yield coordinate

    def yield_coordinate_of_dataset(self):
        for coordinate in self.yield_coordinates_to_fill(self.gathered_dataset):
            yield self.gathered_dataset.loc[{k: v for k, v in zip(
                self.gathered_dataset.coords, coordinate
            )}]

    def yield_tuple_strategy(self):
        coordinates = self.get_coords_for_dataset()
        for item in itertools.product(*(dict(coordinates).values())):
            yield item

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

    def simulate_success(self,
                         coords,
                         potential_coins,
                         potential_end,
                         simulation_timedelta):
        simulation_arguments = self.get_simulation_arguments(coords)
        success_dict = validate_success(MarketBuyLimitSellCreator(),
                                        self.data_accessor,
                                        potential_coins,
                                        potential_end,
                                        simulation_timedelta=simulation_timedelta,
                                        success_criteria=self.target_iterators,
                                        ohlcv_field=self.ohlcv_field,
                                        **simulation_arguments)

        self.set_success_in_dataset(success_dict,
                                    coords)

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

    def overall_success_calculator(self,
                                   loaded_potential_coins=None):
        data_source = (self.data_source_general, self.data_source_specific)
        potential_coin_client = PotentialCoinClient(
            self.time_interval_iterator,
            CryptoOversoldCreator(),
            data_source,
            loaded_potential_coins,
        )


        for coords in self.yield_coordinate_of_dataset():
            history_start, history_end = self.time_interval_iterator.get_datetime_objects_from_str(
                coords.time_intervals.values.tolist()
            )
            coord_dict = coordinates_to_dict(coords)

            potential_coins = self.obtain_potential(potential_coin_client,
                                                    coord_dict,
                                                    history_start,
                                                    history_end)

            simulation_timedelta = TimeIntervalIterator.numpy_dt_to_timedelta(coords.days_to_run.values)

            self.simulate_success(coords,
                                  potential_coins,
                                  history_end,
                                  simulation_timedelta)

        return self.gathered_dataset

    def set_success_in_dataset(self,
                               success_dict,
                               coordinates):
        for success_criteria, success in success_dict.items():
            self.gathered_dataset[success_criteria].loc[coordinates.coords] = success
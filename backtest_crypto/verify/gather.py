import itertools
import logging
from datetime import timedelta

import numpy as np
import xarray as xr

from backtest_crypto.verify.identify import get_potential_coin_at, CryptoOversoldCreator
from backtest_crypto.verify.simulate import validate_success, MarketBuyLimitSellCreator

logger = logging.getLogger(__name__)


class Gather:
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
        self.dataset_values = self.initialize_dataset()

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
        return xr.Dataset(dict(map(lambda data_var: (data_var, nan_da), self.target_iterators)))

    def yield_coordinates_to_fill(self,
                                  ds: xr.Dataset):
        indexes = ds.indexes
        for coordinate in itertools.product(
                *(indexes[coord] for coord in ds.coords)
        ):
            yield coordinate

    def yield_items_from_dataset(self):
        for coordinate in self.yield_coordinates_to_fill(self.dataset_values):
            yield self.dataset_values.loc[{k: v for k, v in zip(
                self.dataset_values.coords, coordinate
            )}]

    @staticmethod
    def numpy_dt_to_timedelta(numpy_dt):
        return timedelta(
            seconds=int(numpy_dt / np.timedelta64(1, 's'))
        )

    def collect_all_items(self):
        for coords in self.yield_items_from_dataset():
            potential_start, potential_end = self.time_interval_iterator.get_datetime_objects_from_str(
                coords.time_intervals.values.tolist()
            )
            simulation_timedelta = self.numpy_dt_to_timedelta(coords.days_to_run.values)

            potential_coins = get_potential_coin_at(
                CryptoOversoldCreator(),
                self.time_interval_iterator,
                data_source_general=self.data_source_general,
                data_source_specific=self.data_source_specific,
                lower_cutoff=coords.low_cutoff.values.tolist(),
                higher_cutoff=coords.high_cutoff.values.tolist(),
                reference_coin=self.reference_coin,
                ohlcv_field=self.ohlcv_field,
                start_time=potential_start,
                end_time=potential_end
            )
            success_dict = {}
            for item in self.success_iterators:
                if item.__name__ == "days_to_run":
                    success_dict["days_to_run"] = self.numpy_dt_to_timedelta(coords["days_to_run"].values)
                else:
                    success_dict[item.__name__] = coords[item.__name__].values.tolist()

            success_dict = validate_success(MarketBuyLimitSellCreator(),
                                            self.data_accessor,
                                            potential_coins,
                                            potential_end,
                                            simulation_timedelta=simulation_timedelta,
                                            success_criteria=self.target_iterators,
                                            ohlcv_field=self.ohlcv_field,
                                            **success_dict)
            self.set_success_in_dataset(success_dict,
                                        coords)
        return self.dataset_values

    def set_success_in_dataset(self,
                               success_dict,
                               coordinates):
        for success_criteria, success in success_dict.items():
            self.dataset_values[success_criteria].loc[coordinates.coords] = success

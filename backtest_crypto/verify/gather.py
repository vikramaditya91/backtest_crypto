import logging
import itertools
import xarray as xr

logger = logging.getLogger(__name__)


class Gather:
    def __init__(self,
                 time_interval_iterator,
                 source_iterators,
                 success_iterators,
                 data_vars):
        self.time_interval_iterator = time_interval_iterator
        self.success_iterators = success_iterators
        self.source_iterators = source_iterators
        self.data_vars = data_vars
        self.time_interval_coordinate = "time_intervals"

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
        return xr.Dataset(dict(map(lambda data_var: (data_var, nan_da), self.data_vars)))

    def yield_coordinates_to_fill(self,
                                  ds: xr.Dataset):
        indexes = ds.indexes
        for coordinate in itertools.product(
                *(indexes[coord] for coord in ds.coords)
        ):
            yield coordinate

    def yield_items_from_dataset(self):
        empty_ds = self.initialize_dataset()
        for coordinate in self.yield_coordinates_to_fill(empty_ds):
            yield empty_ds.loc[{k: v for k, v in zip(empty_ds.coords, coordinate)}]

from abc import ABC, abstractmethod
import xarray as xr
import numpy as np
import pandas as pd
from matplotlib import cm
from matplotlib import pyplot as plt
from backtest_crypto.utilities.iterators import TimeIntervalIterator

class AbstractGraphCreator(ABC):
    @abstractmethod
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def visualize_graph(self,
                        simulation_dataset,
                        *args,
                        **kwargs):
        product = self.factory_method(simulation_dataset)
        return product.generate_graph(*args, **kwargs)


class SurfaceGraph3DCreator(AbstractGraphCreator):
    def factory_method(self, *args, **kwargs):
        return SurfaceGraph3DConcrete(*args, **kwargs)


class AbstractGraphConcrete(ABC):
    def __init__(self, simulation_dataset):
        self.simulation_dataset = simulation_dataset

    @abstractmethod
    def generate_graph(self,
                       *args,
                       **kwargs):
        pass

    def get_time_interval_list(self,
                               time_interval_list):
        time_intervals_datetime = list(map(TimeIntervalIterator.get_datetime_objects_from_str, time_interval_list))
        _, end_list = zip(*time_intervals_datetime)
        return end_list

    def get_axes_for_surface(self,
                             list_of_items,
                             multiplier):
        if all(isinstance(
                item, pd._libs.tslibs.timedeltas.Timedelta
        ) for item in list_of_items):
            list_of_items = list(map(lambda x: x.to_pytimedelta().days, list_of_items))
        return np.outer(list_of_items, np.ones(multiplier))


class SurfaceGraph3DConcrete(AbstractGraphConcrete):
    pass

    def generate_graph(self,
                       data_vars,
                       surface_graph_axes,
                       standard_other_dict):
        assert len(data_vars) == 1, f"Should have only 1 data_vars to plot"
        assert len(surface_graph_axes) == 2, f"Should have only 2 axes"
        values_to_plot = self.simulation_dataset[data_vars[0]].sel(standard_other_dict,
                                                                   tolerance=0.01,
                                                                   method="nearest")
        if "time_intervals" in surface_graph_axes:
            time_intervals = self.get_time_interval_list(values_to_plot.get_index(surface_graph_axes[0]).to_list())
        else:
            values_to_plot = values_to_plot.mean(dim="time_intervals")
            x_index = values_to_plot.get_index(surface_graph_axes[0])
            y_index = values_to_plot.get_index(surface_graph_axes[1])

            x_axis = self.get_axes_for_surface(x_index.values.tolist(),
                                               len(y_index))
            y_axis = self.get_axes_for_surface(y_index.values.tolist(),
                                               len(x_index))

        # y_list = values_to_plot.get_index(surface_graph_axes[1]).to_list()
        #
        # x_axis = self.get_axes_for_surface(list(map(lambda x: x.timestamp(), time_intervals)),
        #                                    y_list.__len__())
        # y_axis = self.get_axes_for_surface(y_list, len(time_intervals))
        fig = plt.figure()
        ax = plt.axes(projection="3d")
        ax.set_xlabel(surface_graph_axes[0])
        ax.set_ylabel(surface_graph_axes[1])
        ax.set_zlabel(data_vars[0])

        z_axis_values = values_to_plot.copy().values
        z_axis_values.resize(len(x_axis), len(y_axis))
        z_axis_values = np.where(z_axis_values == None, 0, z_axis_values).astype(float)
        ax.plot_surface(x_axis, y_axis.T, z_axis_values, cmap=cm.coolwarm, edgecolor='none', alpha=0.5)

        plt.show()
        a = 1


def show_graph(creator: AbstractGraphCreator,
               *args,
               **kwargs):
    return creator.visualize_graph(*args,
                                   **kwargs)

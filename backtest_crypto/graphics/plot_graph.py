from abc import ABC, abstractmethod
import numpy as np
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

    @staticmethod
    def get_axes_for_surface(list_of_items,
                             multiplier):
        if list_of_items.dtype == np.dtype('<m8[ns]'):
            list_of_items = list(map(lambda x: x.to_pytimedelta().days, list_of_items))
        # TODO Very broad condition
        if all(isinstance(
            item, str
        ) for item in list_of_items):
            list_of_items = TimeIntervalIterator.get_time_interval_list(list_of_items)
        return np.tile(list_of_items, [multiplier, 1]).transpose()


class SurfaceGraph3DConcrete(AbstractGraphConcrete):
    def get_x_y_axis(self,
                     surface_graph_axes,
                     values_to_plot):
        if "time_intervals" in surface_graph_axes:
            time_index_in_axes = surface_graph_axes.index("time_intervals")
            other_index_in_axes = surface_graph_axes.index([item for item in surface_graph_axes if item != "time_intervals"][0])
            time_index = values_to_plot.get_index(surface_graph_axes[time_index_in_axes])
            other_index = values_to_plot.get_index(surface_graph_axes[other_index_in_axes])

            x_axis = self.get_axes_for_surface(time_index, len(other_index))
            y_axis = self.get_axes_for_surface(other_index, len(time_index))
        else:
            values_to_plot = values_to_plot.mean(dim="time_intervals")
            x_index = values_to_plot.get_index(surface_graph_axes[0])
            y_index = values_to_plot.get_index(surface_graph_axes[1])

            x_axis = self.get_axes_for_surface(x_index,
                                               len(y_index))
            y_axis = self.get_axes_for_surface(y_index,
                                               len(x_index))
        return x_axis, y_axis

    @staticmethod
    def get_time_sorted_ds(dataset):
        sorted_ts = dataset.time_intervals.values.tolist()
        sorted_ts.sort()
        return dataset.sel({"time_intervals": sorted_ts})

    def generate_graph(self,
                       data_vars,
                       surface_graph_axes,
                       standard_other_dict):
        assert len(data_vars) == 1, f"Should have only 1 data_vars to plot"
        assert len(surface_graph_axes) == 2, f"Should have only 2 axes"
        simulation_dataset = self.simulation_dataset.sel(
            {"strategy": standard_other_dict.pop("strategy")}
        )
        simulation_dataset = self.get_time_sorted_ds(simulation_dataset)
        values_to_plot = simulation_dataset[data_vars[0]].sel(standard_other_dict,
                                                                   tolerance=0.01,
                                                                   method="nearest")
        x_axis, y_axis = self.get_x_y_axis(surface_graph_axes,
                                           values_to_plot)

        ax = plt.axes(projection="3d")
        self.set_labels(ax,
                        surface_graph_axes)
        ax.set_zlabel(data_vars[0])
        # ax.set_zlim([0.85, 1.1])
        z_axis_values = values_to_plot.copy().values
        z_axis_values.resize(len(x_axis), len(y_axis))
        z_axis_values = np.where(z_axis_values == None, 0, z_axis_values).astype(float)
        ax.plot_surface(x_axis, y_axis.T, z_axis_values, cmap=cm.coolwarm, edgecolor='none', alpha=0.5)
        plt.show()

    def set_labels(self,
                   axis,
                   surface_graph_axes):
        # TODO Terrible way to do it
        if "time_intervals" in surface_graph_axes:
            axis.set_xlabel(surface_graph_axes[1])
            axis.set_ylabel(surface_graph_axes[0])
        else:
            axis.set_xlabel(surface_graph_axes[0])
            axis.set_ylabel(surface_graph_axes[1])


def show_graph(creator: AbstractGraphCreator,
               *args,
               **kwargs):
    return creator.visualize_graph(*args,
                                   **kwargs)

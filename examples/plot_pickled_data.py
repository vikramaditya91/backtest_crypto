import pickle
import pathlib
import datetime
from os import path
import numpy as np
from backtest_crypto.graphics.plot_graph import show_graph, SurfaceGraph3DCreator
from backtest_crypto.verify.individual_indicator_calculator import MarketBuyLimitSellIndicatorCreator
from backtest_crypto.verify.simulate_timesteps import MarketBuyLimitSellSimulationCreator


def plot_simulation():
    pickle_file = path.join(pathlib.Path(pathlib.Path(__file__).parents[2] /
                                         "common_db" /
                                         f"simulate_results_100d_25-Aug-2018_17-Nov-2020"
                                         ))
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)
    for item in simulated_dataset:
        simulated_dataset[item] = simulated_dataset[item].fillna(0)
    simulated_dataset = simulated_dataset.sel(time_intervals=simulated_dataset.time_intervals[1:-1])
    show_graph(SurfaceGraph3DCreator(),
               simulated_dataset,
               data_vars=[
                   "calculate_end_of_run_value",
               ],
               surface_graph_axes=[
                   # "percentage_increase",
                   # "days_to_run",
                   "time_intervals",
                   "high_cutoff",
                   # "low_cutoff"
               ],
               standard_other_dict={
                   "percentage_increase": 0.05,
                   "days_to_run": np.timedelta64(datetime.timedelta(days=20)),
                   # "high_cutoff": 2,
                   "low_cutoff": 0,
                   # "max_coins_to_buy": 4,
                   "strategy": MarketBuyLimitSellSimulationCreator
               }
               )


def plot_indicator():
    pickle_file = path.join(pathlib.Path(pathlib.Path(__file__).parents[2] /
                                         "common_db" /
                                         # f"simulate_results_100d_25-Aug-2018_17-Nov-2020"
                                         "success_results_10d_25-Aug-2018_17-Nov-2020"
                                         ))
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)
    for item in simulated_dataset:
        simulated_dataset[item] = simulated_dataset[item].fillna(0)
    simulated_dataset = simulated_dataset.sel(time_intervals=simulated_dataset.time_intervals[1:-1])
    show_graph(SurfaceGraph3DCreator(),
               simulated_dataset,
               data_vars=[
                   "percentage_of_bought_coins_hit_target",
                   # "end_of_run_value_of_bought_coins_if_sold_on_target",
               ],
               surface_graph_axes=[
                   # "percentage_increase",
                   # "days_to_run",
                   "time_intervals",
                   "high_cutoff",
                   # "low_cutoff"
               ],
               standard_other_dict={
                   "percentage_increase": 0.05,
                   "days_to_run": np.timedelta64(datetime.timedelta(days=20)),
                   # "high_cutoff": 2,
                   "low_cutoff": 0,
                   "strategy": MarketBuyLimitSellIndicatorCreator
               }
               )


if __name__ == "__main__":
    plot_indicator()

import pickle
import pathlib
import datetime
from os import path
import numpy as np
from backtest_crypto.graphics.plot_graph import show_graph, SurfaceGraph3DCreator, SurfaceGraph3DSubPlotCreator
from backtest_crypto.verify.individual_indicator_calculator import MarketBuyLimitSellIndicatorCreator
from backtest_crypto.verify.simulate_timesteps import MarketBuyLimitSellSimulationCreator, \
    MarketBuyTrailingSellSimulationCreator, LimitBuyLimitSellSimulationCreator


def plot_simulation():
    pickle_file = "/Users/vikram/Documents/Personal/s3_sync/result_temp_2"
    item = "calculate_end_of_run_value"
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)
    for item in simulated_dataset:
        simulated_dataset[item] = simulated_dataset[item].dropna("time_intervals")
        simulated_dataset[item] = simulated_dataset[item].fillna(0)
    # simulated_dataset = simulated_dataset.sel(time_intervals=simulated_dataset.time_intervals[1:-1])

    show_graph(SurfaceGraph3DCreator(),
               simulated_dataset,
               data_vars=[
                   "calculate_end_of_run_value",
               ],
               surface_graph_axes=[
                   # "stop_price_sell",
                   "percentage_increase",
                   # "limit_sell_adjust_trail",
                   # "days_to_run",
                   # "time_intervals",
                   "percentage_reduction",
                   # "low_cutoff"
               ],
               standard_other_dict={
                   "cutoff_mean": 2.5,
                   # "percentage_increase": 0.05,
                   # "percentage_reduction": reduction,
                   "days_to_run": np.timedelta64(datetime.timedelta(days=20)),
                   "cutoff_deviation": 5,
                   # "high_cutoff": 5,
                   # "low_cutoff": 1,
                   # "limit_sell_adjust_trail": 0.01,
                   "max_coins_to_buy": 4,
                   "strategy": LimitBuyLimitSellSimulationCreator
               }
               )


def plot_simulation_sub():
    pickle_file = "/Users/vikram/Documents/Personal/s3_sync/result_temp_2"
    item = "calculate_end_of_run_value"
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)
    for item in simulated_dataset:
        simulated_dataset[item] = simulated_dataset[item].dropna("time_intervals")
        simulated_dataset[item] = simulated_dataset[item].fillna(0)
    # simulated_dataset = simulated_dataset.sel(time_intervals=simulated_dataset.time_intervals[1:-1])

    show_graph(SurfaceGraph3DSubPlotCreator(),
               simulated_dataset,
               data_vars=[
                   "calculate_end_of_run_value",
               ],
               surface_graph_axes=[
                   # "stop_price_sell",
                   "percentage_increase",
                   # "limit_sell_adjust_trail",
                   # "days_to_run",
                   "time_intervals",
                   # "percentage_reduction",
                   # "low_cutoff"
               ],
               standard_other_dict={
                   "cutoff_mean": 2.5,
                   # "percentage_increase": [0.05, 0.075, 0.1],
                   "percentage_reduction": [0, 0.05, 0.01],
                   "days_to_run": np.timedelta64(datetime.timedelta(days=20)),
                   "cutoff_deviation": 5,
                   # "high_cutoff": 5,
                   # "low_cutoff": 1,
                   # "limit_sell_adjust_trail": 0.01,
                   "max_coins_to_buy": 4,
                   "strategy": LimitBuyLimitSellSimulationCreator
               },
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
    plot_simulation_sub()

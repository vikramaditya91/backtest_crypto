import pickle
import pathlib
import datetime
from os import path
import numpy as np
from matplotlib import pyplot as plt
from backtest_crypto.graphics.plot_graph import show_graph, SurfaceGraph3DCreator


def main():
    pickle_file = path.join(pathlib.Path(pathlib.Path(__file__).parents[1] /
                                         "database" /
                                         f"25_Jan_2017_TO_18_Nov_2020_BTC_1h.db"))
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)
    simulated_dataset = simulated_dataset.sel(time_intervals=simulated_dataset.time_intervals[20:-20])
    show_graph(SurfaceGraph3DCreator(),
               simulated_dataset,
               data_vars=["percentage_of_bought_coins_hit_target"],
               surface_graph_axes=["days_to_run",
                                   "percentage_increase"],
               standard_other_dict={
                   # "percentage_increase": 0.035,
                   # "days_to_run": np.timedelta64(datetime.timedelta(days=20)),
                   "high_cutoff": 0.7,
                   "low_cutoff": 0}
               )


if __name__ == "__main__":
    main()
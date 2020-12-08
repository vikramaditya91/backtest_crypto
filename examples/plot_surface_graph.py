import pickle
import pathlib
from os import path
from matplotlib import pyplot as plt
from backtest_crypto.graphics.build_graph import show_graph, SurfaceGraph3DCreator


def main():
    pickle_file = path.join(pathlib.Path(pathlib.Path(__file__).parents[1] /
                                         "database" /
                                         f"coin_3d_iter_results_1d"))
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)

    show_graph(SurfaceGraph3DCreator(),
               simulated_dataset,
               data_vars=["end_of_run_value_of_bought_coins_if_not_sold"],
               surface_graph_axes=["time_intervals",
                                   "days_to_run"]
               )













if __name__ == "__main__":
    main()
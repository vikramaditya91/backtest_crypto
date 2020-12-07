import pickle
from os import path
from matplotlib import pyplot as plt
from backtest_crypto.graphics.build_graph import show_graph, SurfaceGraph3DCreator


def main():
    pickle_file = path.join("/home/vikramaditya/PycharmProjects/database/coin_3d_iter")
    with open(pickle_file, "rb") as fp:
        simulated_dataset = pickle.load(fp)

    show_graph(SurfaceGraph3DCreator(),
               simulated_dataset,
               data_vars=["number_of_bought_coins_hit_target"],
               surface_graph_axes=["time_intervals",
                                   "high_cutoff"]
               )













if __name__ == "__main__":
    main()
import pickle
import functools
from pathlib import Path
import pandas as pd


def main():
    pickled_files_location = Path(Path(__file__).parents[2] / "database" / "potential_coins")
    assert pickled_files_location.exists()

    data_series = []
    for item in pickled_files_location.iterdir():
        with open(item, "rb") as fp:
            contents = pickle.load(fp)
            data_series.append(contents)

    sorted_data_series = list(map(lambda x: x.sort_index(), data_series))

    unused_dropped_na = list(map(lambda x: x.dropna(), sorted_data_series))

    joined_series = pd.concat(unused_dropped_na)
    sorted_series = joined_series.sort_index()

    sorted_series.to_pickle(
        str(Path(Path(__file__).parents[2] / "database" / "potential_coins" / "joined_potential_pickle"))
    )


if __name__ == "__main__":
    main()

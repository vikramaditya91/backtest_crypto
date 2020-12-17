import pandas as pd
import xarray as xr

def time_interval_iterator_to_pd_multiindex(time_interval_iterator,
                                            ):
    time_intervals = time_interval_iterator.time_intervals
    start_list, end_list = zip(*time_intervals)
    return pd.MultiIndex.from_product([list(set(start_list)),
                                       list(set(end_list))],
                                      names=['start_time',
                                             'end_time'])


def coordinates_to_dict(dataset: xr.Dataset):
    coords = dataset.to_dict()["coords"]
    return dict(map(lambda x:(x[0], x[1]["data"]), coords.items()))

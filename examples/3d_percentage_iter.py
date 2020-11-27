import matplotlib.pyplot as plt
from datetime import datetime
from backtest_crypto.history_collect.gather_history import yield_split_coin_history, store_largest_xarray, get_history_between
from crypto_history import class_builders
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins
from backtest_crypto.utilities.iterators import TimeIntervalIterator, \
    ManualSourceIterators, ManualSuccessIterators, DataVars
from pprint import pprint
from backtest_crypto.verify.gather import Gather
from backtest_crypto.verify.simulate import SimulateDataset
from backtest_crypto.verify.identify import get_potential_coin_at, CryptoOversoldCreator

def main():

    overall_start = datetime(day=25, month=1, year=2017)
    overall_end = datetime(day=18, month=11, year=2020)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "3d"
    interval = "3d"
    data_source_general = "sqlite"
    data_source_specific = "binance"

    time_interval_iterator = TimeIntervalIterator(overall_start,
                                                  overall_end,
                                                  interval,
                                                  forward_in_time=False,
                                                  increasing_range=False)

    table_name = f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_{interval}"
    sqlite_access_creator = class_builders.get("access_xarray").get(data_source_general)()

    store_largest_xarray(sqlite_access_creator,
                         overall_start=overall_start,
                         overall_end=overall_end,
                         candle=candle,
                         reference_coin=reference_coin,
                         ohlcv_field=ohlcv_field,
                         file_path=f"/home/vikramaditya/PycharmProjects/{candle}.db",
                         mapped_class=OversoldCoins,
                         table_name=table_name)

    coin_history_yield = yield_split_coin_history(sqlite_access_creator,
                                                  time_interval_iterator=time_interval_iterator,
                                                  )

    manual_source_iterators = ManualSourceIterators()
    manual_success_iterators = ManualSuccessIterators()
    data_vars = DataVars()
    gather_all = Gather(time_interval_iterator,
                        success_iterators=[manual_success_iterators.percentage_increase,
                                           manual_success_iterators.percentge_reduction],
                        source_iterators=[manual_source_iterators.high_cutoff,
                                          manual_source_iterators.low_cutoff,
                                          ],
                        data_vars=data_vars.data_vars())
    yield_dataset_items = gather_all.yield_items_from_dataset()


    identify = get_potential_coin_at(CryptoOversoldCreator(),
                                     time_interval_iterator,
                                     data_source_general=data_source_general,
                                     data_source_specific=data_source_specific,
                                     lower_cutoff=0,
                                     higher_cutoff=0.7,
                                     reference_coin=reference_coin,
                                     ohlcv_field=ohlcv_field,
                                     start_time=datetime(2017, 1, 25, 0, 0),
                                     end_time=datetime(2017, 1, 26, 0, 0)
                                     )

    simulate = SimulateDataset()
    simulate.populate_dataset(
        yield_dataset_items,
        coin_history_yield


    )

    for known_history, masked_history in coin_history_yield:
        print("known_history.shape ", known_history.shape)
        print("masked_history.shape ", masked_history.shape)


if __name__ == "__main__":
    main()





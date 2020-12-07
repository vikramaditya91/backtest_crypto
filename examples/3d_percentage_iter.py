import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from backtest_crypto.history_collect.gather_history import store_largest_xarray
from crypto_history import class_builders, init_logger
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins
from backtest_crypto.utilities.iterators import TimeIntervalIterator, \
    ManualSourceIterators, ManualSuccessIterators
import logging
import pathlib
import pickle
from backtest_crypto.verify.gather import Gather


def main():
    init_logger(logging.DEBUG)
    overall_start = datetime(day=25, month=8, year=2018)
    overall_end = datetime(day=18, month=11, year=2020)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "1d"
    interval = "1d"
    data_source_general = "sqlite"
    data_source_specific = "binance"

    time_interval_iterator = TimeIntervalIterator(overall_start,
                                                  overall_end,
                                                  interval,
                                                  forward_in_time=False,
                                                  increasing_range=False)

    table_name = f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_{candle}"
    sqlite_access_creator = class_builders.get("access_xarray").get(data_source_general)()

    store_largest_xarray(sqlite_access_creator,
                         overall_start=overall_start,
                         overall_end=overall_end,
                         candle=candle,
                         reference_coin=reference_coin,
                         ohlcv_field=ohlcv_field,
                         file_path=str(pathlib.Path(pathlib.Path(__file__).parents[1] /
                                                    "database" /
                                                    f"25_Jan_2017_TO_18_Nov_2020_BTC_{candle}.db")),
                         mapped_class=OversoldCoins,
                         table_name=table_name)

    source_iterators = ManualSourceIterators()
    success_iterators = ManualSuccessIterators()
    gather_items = Gather(
                          sqlite_access_creator,
                          data_source_general,
                          data_source_specific,
                          reference_coin,
                          ohlcv_field,
                          time_interval_iterator,
                          source_iterators=[
                              source_iterators.high_cutoff,
                              source_iterators.low_cutoff
                          ],
                          success_iterators=[
                              success_iterators.percentage_increase,
                              success_iterators.days_to_run
                          ],
                          target_iterators=["number_of_bought_coins_hit_target"],
                          additional_settings={"percentage_increase": 0.05})
    collective_ds = gather_items.collect_all_items()
    with open(pathlib.Path(pathlib.Path(__file__).parents[1] / "database" / f"coin_3d_iter_results_{interval}"), "wb") as fp:
        pickle.dump(collective_ds, fp)


if __name__ == "__main__":
    main()





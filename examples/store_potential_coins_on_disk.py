import logging
import pathlib
from datetime import datetime

from crypto_history import class_builders, init_logger
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins

from backtest_crypto.history_collect.gather_history import store_largest_xarray
from backtest_crypto.utilities.iterators import TimeIntervalIterator, \
    ManualSourceIterators, ManualSuccessIterators
from backtest_crypto.verify import gather_overall


def main():
    init_logger(logging.DEBUG)
    overall_start = datetime(day=25, month=8, year=2018)
    overall_end = datetime(day=18, month=11, year=2020)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "1h"
    interval = "100d"
    data_source_general = "sqlite"
    data_source_specific = "binance"

    table_name_list = [f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_1d",
                       f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_1h"]

    sqlite_access_creator = class_builders.get("access_xarray").get(data_source_general)()

    store_largest_xarray(sqlite_access_creator,
                         overall_start=overall_start,
                         overall_end=overall_end,
                         candle=candle,
                         reference_coin=reference_coin,
                         ohlcv_field=ohlcv_field,
                         file_path=str(pathlib.Path(pathlib.Path(__file__).parents[2] /
                                                    "common_db" /
                                                    f"25_Jan_2017_TO_18_Nov_2020_BTC_1h_1d.pickled")),
                         mapped_class=OversoldCoins,
                         table_name_list=table_name_list)

    source_iterators = ManualSourceIterators()

    interval = "50d"
    time_interval_iterator = TimeIntervalIterator(overall_start,
                                                  overall_end,
                                                  interval,
                                                  forward_in_time=False,
                                                  increasing_range=False)

    iterators = {"time": time_interval_iterator,
                 "source": [
                     source_iterators.high_cutoff,
                     source_iterators.low_cutoff
                 ],
                 "success": [
                 ],
                 "target": [
                ],
                 "strategy": [

                 ]
                }

    gather_items = gather_overall.GatherPotential(
        sqlite_access_creator,
        (data_source_general, data_source_specific),
        reference_coin,
        ohlcv_field,
        iterators
    )

    narrowed_start = datetime(day=1, month=7, year=2018)
    narrowed_end = datetime(day=17, month=11, year=2020)
    gather_items.store_potential_coins_pickled(
        pickled_file_path=str(pathlib.Path(pathlib.Path(__file__).parents[1] /
                                           "database" /
                                           f"{interval}_{narrowed_start}_{narrowed_start}_potential_coins_overall.db")),
        narrowed_start_time=narrowed_start,
        narrowed_end_time=narrowed_end
    )


if __name__ == "__main__":
    main()

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
    overall_end = datetime(day=20, month=5, year=2021)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "1h"
    data_source_general = "sqlite"
    data_source_specific = "binance"

    table_name_list = [f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_1d",
                       f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_1h"]

    sqlite_access_creator = class_builders.get("access_xarray").get(data_source_general)()

    full_history_da_dict = store_largest_xarray(sqlite_access_creator,
                                                       overall_start=overall_start,
                                                       overall_end=overall_end,
                                                       candle=candle,
                                                       reference_coin=reference_coin,
                                                       ohlcv_field=ohlcv_field,
                                                       file_path="/Users/vikram/Documents/Personal/s3_sync/25_Jan_2017_TO_23_May_2021_BTC_1h_1d.db",
                                                       mapped_class=OversoldCoins,
                                                       table_name_list=table_name_list)

    source_iterators = ManualSourceIterators()

    interval = "1d"
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
        full_history_da_dict,
        reference_coin,
        ohlcv_field,
        iterators,
        potential_coin_path=None,
    )

    narrowed_start = datetime(day=1, month=7, year=2018)
    narrowed_end = datetime(day=20, month=5, year=2021)
    gather_items.store_potential_coins_pickled(
        pickled_file_path=str(pathlib.Path(pathlib.Path(__file__).parents[4] /
                                           "s3_sync" / "staging" /
                                           f"{interval}_"
                                           f"{narrowed_start.year}-{narrowed_start.month}-{narrowed_start.day}_"
                                           f"{narrowed_end.year}-{narrowed_end.month}-{narrowed_end.day}_"
                                           f"potential_coins_overall.pickle")),
        narrowed_start_time=narrowed_start,
        narrowed_end_time=narrowed_end
    )


if __name__ == "__main__":
    main()

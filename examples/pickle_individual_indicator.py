import logging
import pathlib
import pickle
from datetime import datetime

from crypto_history import class_builders, init_logger
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins

from backtest_crypto.verify.individual_indicator_calculator import MarketBuyLimitSellIndicatorCreator
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
    interval = "1d"
    data_source_general = "sqlite"
    data_source_specific = "binance"

    time_interval_iterator = TimeIntervalIterator(overall_start,
                                                  overall_end,
                                                  interval,
                                                  forward_in_time=False,
                                                  increasing_range=False)

    table_name_list = [f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_1d",
                       f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_1h"]

    sqlite_access_creator = class_builders.get("access_xarray").get(data_source_general)()

    full_dataarray = store_largest_xarray(sqlite_access_creator,
                                          overall_start=overall_start,
                                          overall_end=overall_end,
                                          candle=candle,
                                          reference_coin=reference_coin,
                                          ohlcv_field=ohlcv_field,
                                          file_path="/Users/vikram/Documents/Personal/s3_sync/25_Jan_2017_TO_23_May_2021_BTC_1h_1d.db",
                                          mapped_class=OversoldCoins,
                                          table_name_list=table_name_list)

    source_iterators = ManualSourceIterators()
    success_iterators = ManualSuccessIterators()

    iterators = {"time": time_interval_iterator,
                 "source": [
                     source_iterators.high_cutoff,
                     source_iterators.low_cutoff
                 ],
                 "success": [
                     success_iterators.percentage_increase,
                     success_iterators.days_to_run
                 ],
                 "target": [
                     "percentage_of_bought_coins_hit_target",
                     "end_of_run_value_of_bought_coins_if_not_sold",
                     "end_of_run_value_of_bought_coins_if_sold_on_target"
                 ],
                 "strategy":
                     [
                         MarketBuyLimitSellIndicatorCreator
                     ]
                 }

    pickled_potential_path = str(pathlib.Path(__file__).parents[4] /
                                 "s3_sync" /
                                 "staging" /
                                 "1d_2018-07-01_2021-05-20_potential_coins_overall.pickle")

    gather_items = gather_overall.GatherIndicator(
        full_dataarray,
        reference_coin,
        ohlcv_field,
        iterators,
        potential_coin_path=pickled_potential_path,
    )
    narrowed_start = datetime(day=25, month=8, year=2018)
    narrowed_end = datetime(day=17, month=11, year=2020)

    collective_ds = gather_items.overall_individual_indicator_calculator(narrowed_start,
                                                                         narrowed_end)
    with open(pathlib.Path(pathlib.Path(__file__).parents[2] /
                           "common_db" /
                           f"success_results_{interval}_"
                           f"{narrowed_start.strftime('%d-%b-%Y')}_"
                           f"{narrowed_end.strftime('%d-%b-%Y')}"),
              "wb") as fp:
        pickle.dump(collective_ds, fp)


if __name__ == "__main__":
    main()

import logging
import pathlib
import pickle
from datetime import datetime

from crypto_history import class_builders, init_logger
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins

from backtest_crypto.history_collect.gather_history import store_largest_xarray
from backtest_crypto.utilities.iterators import TimeIntervalIterator, \
    ManualSourceIterators, ManualSuccessIterators
from backtest_crypto.verify import gather_overall
from backtest_crypto.verify.simulate_timesteps import MarketBuyLimitSellSimulationCreator,\
    LimitBuyLimitSellSimulationCreator, MarketBuyTrailingSellSimulationCreator


def main():
    init_logger(logging.DEBUG)
    overall_start = datetime(day=25, month=8, year=2018)
    overall_end = datetime(day=20, month=5, year=2021)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "1h"
    interval = "100d"
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
    file_path = str(pathlib.Path(__file__).parents[4] /
                    "s3_sync" /
                    "25_Jan_2017_TO_23_May_2021_BTC_1h_1d.db")
    store_largest_xarray(sqlite_access_creator,
                         overall_start=overall_start,
                         overall_end=overall_end,
                         candle=candle,
                         reference_coin=reference_coin,
                         ohlcv_field=ohlcv_field,
                         file_path=file_path,
                         mapped_class=OversoldCoins,
                         table_name_list=table_name_list)

    source_iterators = ManualSourceIterators()
    success_iterators = ManualSuccessIterators()

    iterators = {"time": time_interval_iterator,
                 "source": [
                     source_iterators.cutoff_mean,
                     source_iterators.cutoff_deviation,
                     source_iterators.max_coins_to_buy
                 ],
                 "success": [
                     success_iterators.percentage_increase,
                     success_iterators.percentage_reduction,
                     success_iterators.days_to_run,
                     success_iterators.stop_price_sell,
                     success_iterators.limit_sell_adjust_trail
                 ],
                 "target": [
                     "calculate_end_of_run_value"
                 ],
                 "strategy":
                     [
                         MarketBuyTrailingSellSimulationCreator
                     ]
                 }
    gather_items = gather_overall.GatherSimulation(
        sqlite_access_creator,
        (data_source_general, data_source_specific),
        reference_coin,
        ohlcv_field,
        iterators
    )

    pickled_potential_path = str(pathlib.Path(__file__).parents[4] /
                    "s3_sync" /
                    "staging" /
                    "1d_2018-07-01_2021-05-20_potential_coins_overall.pickle")

    # pickled_potential_path = str(pathlib.Path(pathlib.Path(__file__).resolve().parents[3] /
    #                                           "common_db" /
    #                                           f"1h_2018_to_2020_potential_coins.pickled"))
    narrowed_start = datetime(day=25, month=8, year=2018)
    narrowed_end = datetime(day=20, month=5, year=2021)

    collective_ds = gather_items.simulation_calculator(narrowed_start,
                                                       narrowed_end,
                                                       loaded_potential_coins=pickled_potential_path)
    with open("/Users/vikram/Documents/Personal/s3_sync/result_temp_1",
              "wb") as fp:
        pickle.dump(collective_ds, fp)


if __name__ == "__main__":
    main()

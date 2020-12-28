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
from backtest_crypto.verify.simulate_timesteps import MarketBuyLimitSellSimulationCreator


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

    time_interval_iterator = TimeIntervalIterator(overall_start,
                                                  overall_end,
                                                  interval,
                                                  forward_in_time=False,
                                                  increasing_range=False)

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
    success_iterators = ManualSuccessIterators()

    iterators = {"time": time_interval_iterator,
                 "source": [
                     source_iterators.high_cutoff,
                     source_iterators.low_cutoff,
                     source_iterators.max_coins_to_buy
                 ],
                 "success": [
                     success_iterators.percentage_increase,
                     success_iterators.days_to_run
                 ],
                 "target": [
                     "calculate_end_of_run_value"
                 ],
                 "strategy":
                     [
                         MarketBuyLimitSellSimulationCreator
                     ]
                 }
    gather_items = gather_overall.GatherSimulation(
        sqlite_access_creator,
        (data_source_general, data_source_specific),
        reference_coin,
        ohlcv_field,
        iterators
    )

    pickled_potential_path = str(pathlib.Path(pathlib.Path(__file__).parents[2] /
                                              "common_db" /
                                              f"1h_2018_to_2020_potential_coins.pickled"))
    narrowed_start = datetime(day=25, month=8, year=2018)
    narrowed_end = datetime(day=17, month=11, year=2020)

    collective_ds = gather_items.simulation_calculator(narrowed_start,
                                                       narrowed_end,
                                                       loaded_potential_coins=pickled_potential_path)
    with open(pathlib.Path(pathlib.Path(__file__).parents[2] /
                           "common_db" /
                           f"simulate_results_{interval}_"
                           f"{narrowed_start.strftime('%d-%b-%Y')}_"
                           f"{narrowed_end.strftime('%d-%b-%Y')}_5_high_1_low"),
              "wb") as fp:
        pickle.dump(collective_ds, fp)


if __name__ == "__main__":
    main()

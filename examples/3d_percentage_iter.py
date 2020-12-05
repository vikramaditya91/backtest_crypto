import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from backtest_crypto.history_collect.gather_history import yield_split_coin_history, store_largest_xarray, get_history_between
from crypto_history import class_builders, init_logger
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins
from backtest_crypto.utilities.iterators import TimeIntervalIterator, \
    ManualSourceIterators, ManualSuccessIterators, Targets
from pprint import pprint
import logging
from backtest_crypto.verify.gather import Gather
from backtest_crypto.verify.simulate import validate_success, MarketBuyLimitSellCreator
from backtest_crypto.verify.identify import get_potential_coin_at, CryptoOversoldCreator

def main():
    init_logger(logging.DEBUG)
    overall_start = datetime(day=25, month=8, year=2018)
    overall_end = datetime(day=18, month=11, year=2020)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "1h"
    interval = "1h"
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
                         file_path=f"/home/vikramaditya/PycharmProjects/database/25_Jan_2017_TO_18_Nov_2020_BTC_1h.db",
                         mapped_class=OversoldCoins,
                         table_name=table_name)

    coin_history_yield = yield_split_coin_history(sqlite_access_creator,
                                                  time_interval_iterator=time_interval_iterator,
                                                  )

    potential_start = datetime(2018, 8, 25, 0, 0)
    potential_end = datetime(2020, 10, 5, 0, 0)
    potential_coins = get_potential_coin_at(CryptoOversoldCreator(),
                                     time_interval_iterator,
                                     data_source_general=data_source_general,
                                     data_source_specific=data_source_specific,
                                     lower_cutoff=0,
                                     higher_cutoff=0.7,
                                     reference_coin=reference_coin,
                                     ohlcv_field=ohlcv_field,
                                     start_time=potential_start,
                                     end_time=potential_end
                                     )

    validate_success(MarketBuyLimitSellCreator(),
                     sqlite_access_creator,
                     potential_coins,
                     potential_end,
                     simulation_timedelta=timedelta(days=20),
                     success_criteria=["number_of_bought_coins_hit_target"],
                     ohlcv_field=ohlcv_field,
                     percentage_increase=0.05)

    for known_history, masked_history in coin_history_yield:
        print("known_history.shape ", known_history.shape)
        print("masked_history.shape ", masked_history.shape)


if __name__ == "__main__":
    main()





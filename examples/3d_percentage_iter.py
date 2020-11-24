import matplotlib.pyplot as plt
from datetime import datetime
from backtest_crypto.history_collect.dataarray import yield_split_coin_history
from crypto_history import class_builders
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins
from backtest_crypto.utilities.iterators import TimeIntervalIterator, \
    ManualSourceIterators, ManualSuccessIterators, DataVars
from pprint import pprint
from backtest_crypto.verify.gather import Gather

def main():
    sqlite_access_creator = class_builders.get("access_xarray").get("sqlite")()

    start_date = datetime(day=25, month=1, year=2017)
    end_date = datetime(day=18, month=11, year=2020)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "3d"
    interval = "3d"

    time_interval_iterator = TimeIntervalIterator(start_date,
                                                  end_date,
                                                  interval,
                                                  forward_in_time=False,
                                                  increasing_range=False)

    table_name = f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_{interval}"
    coin_history_yield = yield_split_coin_history(sqlite_access_creator,
                                                  time_interval_iterator=time_interval_iterator,
                                                  candle=candle,
                                                  reference_coin=reference_coin,
                                                  ohlcv_field=ohlcv_field,
                                                  file_path=f"/home/vikramaditya/PycharmProjects/{candle}.db",
                                                  mapped_class=OversoldCoins,
                                                  table_name=table_name)
    manual_source_iterators = ManualSourceIterators()
    manual_success_iterators = ManualSuccessIterators()
    data_vars = DataVars()

    gather_all = Gather(time_interval_iterator,
                        coin_history_yield,
                        success_iterators=[manual_success_iterators.percentage_increase,
                                           manual_success_iterators.percentge_reduction],
                        source_iterators=[manual_source_iterators.high_cutoff,
                                          manual_source_iterators.low_cutoff,
                                          ],
                        data_vars=data_vars.data_vars())
    gather_all.gather_dataset()

    for known_history, masked_history in coin_history_yield:
        print("known_history.shape ", known_history.shape)
        print("masked_history.shape ", masked_history.shape)


if __name__ == "__main__":
    main()





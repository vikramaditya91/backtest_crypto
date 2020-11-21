import matplotlib.pyplot as plt
from datetime import datetime
from backtest_crypto.history_collect.dataarray import yield_split_coin_history
from crypto_history import class_builders
from crypto_oversold.emit_data.sqlalchemy_operations import OversoldCoins
from pprint import pprint


def main():
    sqlite_access_creator = class_builders.get("access_xarray").get("sqlite")()

    start_date = datetime(day=25, month=1, year=2015)
    end_date = datetime(day=18, month=11, year=2020)
    reference_coin = "BTC"
    ohlcv_field = "open"
    candle = "3d"
    interval = "3d"

    table_name = f"COIN_HISTORY_{ohlcv_field}_{reference_coin}_{interval}"
    coin_history_yield = yield_split_coin_history(sqlite_access_creator,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  interval=interval,
                                                  candle=candle,
                                                  reference_coin=reference_coin,
                                                  ohlcv_field=ohlcv_field,
                                                  file_path=f"/home/vikramaditya/PycharmProjects/{candle}.db",
                                                  mapped_class=OversoldCoins,
                                                  table_name=table_name)

    for known_history, masked_history in coin_history_yield:
        print("known_history.shape ", known_history.shape)
        print("masked_history.shape ", masked_history.shape)


if __name__ == "__main__":
    main()





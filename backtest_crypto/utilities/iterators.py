from datetime import datetime, timedelta
from typing import Union
import numpy as np
from crypto_history.stock_market.stock_market_factory import DateTimeOperations


class TimeIntervalIterator:
    def __init__(self,
                 start_time: datetime,
                 end_time: datetime,
                 interval: Union[timedelta, str],
                 forward_in_time: bool = True,
                 increasing_range=False):
        if isinstance(start_time, datetime) is False or isinstance(end_time, datetime) is False:
            raise TypeError("Start time and end time should be datetime objects")
        self.start_time = start_time
        self.end_time = end_time
        self.interval = self.init_interval(interval)
        self.forward_in_time = forward_in_time
        self.increasing_range = increasing_range
        self.current_start, self.current_end = self.init_current_start_end(start_time,
                                                                           end_time,
                                                                           forward_in_time,
                                                                           increasing_range)
        self._time_intervals = None

    @property
    def time_intervals(self):
        if self._time_intervals is None:
            self._time_intervals = self.get_time_intervals_list()
        return self._time_intervals

    def init_interval(self,
                      interval):
        if isinstance(interval, str):
            interval = self.string_to_datetime(interval)
        return interval

    @staticmethod
    def string_to_datetime(interval: str):
        datetime_operations = DateTimeOperations()
        return datetime_operations.map_string_to_timedelta(interval)

    @staticmethod
    def init_current_start_end(start_time,
                               end_time,
                               forward_in_time,
                               increasing_range):
        if increasing_range:
            if forward_in_time:
                return start_time, start_time
            else:
                return end_time, end_time
        else:
            return start_time, end_time

    def _skip_to_next(self):
        if self.increasing_range is True:
            if self.forward_in_time:
                self.current_start += self.interval
            else:
                self.current_start -= self.interval
        else:
            if self.forward_in_time:
                self.current_start += self.interval
            else:
                self.current_end -= self.interval

    def _get_time_intervals(self):
        while self.current_end >= self.current_start:
            yield self.current_start, self.current_end
            self._skip_to_next()

    def get_time_intervals_list(self):
        return list(self._get_time_intervals())

    # TODO Should be made static
    def get_list_time_intervals_str(self,
                                    delimiter="_"):
        time_intervals = self.time_intervals
        # Made set because there could be multiple coordinates with the same values.
        # Should probably be fixed in a better way
        return list(set(map(
            lambda x: f"{int(x[0].timestamp()*1000)}{delimiter}{int(x[1].timestamp()*1000)}", time_intervals
            )
        ))

    @staticmethod
    def get_datetime_objects_from_str(timestamps_separated,
                                      delimiter="_"):
        start, end = timestamps_separated.split(delimiter)
        return datetime.fromtimestamp(float(start)/1000), datetime.fromtimestamp(float(end)/1000)

    @classmethod
    def get_time_interval_list(cls,
                               time_interval_list):
        time_intervals_datetime = list(map(cls.get_datetime_objects_from_str, time_interval_list))
        _, end_list = zip(*time_intervals_datetime)
        return list(map(lambda x: x.timestamp(), end_list))

    @staticmethod
    def numpy_dt_to_timedelta(numpy_dt):
        return timedelta(
            seconds=int(numpy_dt / np.timedelta64(1, 's'))
        )

    @staticmethod
    def time_iterator(start_time: datetime,
                      end_time: datetime,
                      interval: timedelta):
        for n in range(int((end_time - start_time)/interval)):
            yield start_time + (interval * n)


class ManualSourceIterators:
    def high_cutoff(self):
        # When selecting potential coins, coins above this value are not selected
        return [5]
        # return np.arange(0.6, 1.2, 0.5)

        # return np.arange(0.6, 1.2, 0.05)

    def low_cutoff(self):
        # When selecting potential coins, coins below this value are not selected
        return [1]

    def cutoff_mean(self):
        # When selecting potential coins, potential coins are selected wth this mean.
        # Can only be used with cutoff_deviation
        return [1.5, 5]

    def cutoff_deviation(self):
        # When selecting potential coins, potential coins are deviated from the
        # mean with this value. Can only be used with cutoff_mean
        return [0.2]

    def max_coins_to_buy(self):
        return [4]


class ManualSuccessIterators:
    def percentage_increase(self):
        # When selling the coin, this is the expected profit percentage to make
        return [0.05]
        # return np.arange(0.025, 0.1, 0.05)

    def stop_price_sell(self):
        # When selling the coin, if a trailing-order is used,
        # this is the stop-limit for selling.
        # That is the price below which coin is sold to reduce losses
        return [0.01, 0.02]

    def limit_sell_adjust_trail(self):
        # In a trailing order, if the order is too close to the limit,
        # kill the sell-order `(1-this)*limit_price` is the trigger
        return [0.02]

    def percentage_reduction(self):
        # When buying an asset, set a buy order with
        # this much below current price
        return [0, 0.01]

    def days_to_run(self):
        # Manually set this from largest to smallest for cache purposes
        return [
            # timedelta(days=1),
                timedelta(days=20),
                # timedelta(days=12),
                # timedelta(days=24),
                # timedelta(days=28),
                # timedelta(days=32)
        ]


class Targets:
    def after_sell_targets(self):
        return [
            # Number of oversold coins that actually hit the target
            "number_of_bought_coins_hit_target",

            # Number of oversold coins that did not hit the target
            "number_of_bought_coins_did_not_hit_target",

            # Percentage of bought coins that hit the target
            "percentage_of_bought_coins_hit_target",

            # At the end of 20-days, value of bought coins if they were not sold
            "end_of_run_value_of_bought_coins_if_not_sold",

            # At the end of 20-days, value of bought coins if they were sold when the target hit
            "end_of_run_value_of_bought_coins_if_sold_on_target",
            ]

    def before_buy_targets(self):
        return [
            # Number of oversold coins that were actually bought out of all Binance coins
            "percentage_of_coins_bought",
        ]



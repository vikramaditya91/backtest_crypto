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
            raise TypeError("Start time and End time should be datetime objects")
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

    @staticmethod
    def init_interval(interval):
        if isinstance(interval, str):
            datetime_operations = DateTimeOperations()
            interval = datetime_operations.map_string_to_timedelta(interval)
        return interval

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
        return list(map(
            lambda x: f"{int(x[0].timestamp()*1000)}{delimiter}{int(x[1].timestamp()*1000)}", time_intervals
            )
        )

    @staticmethod
    def get_datetime_objects_from_str(timestamps_separated,
                                      delimiter="_"):
        start, end = timestamps_separated.split(delimiter)
        return datetime.fromtimestamp(float(start)/1000), \
               datetime.fromtimestamp(float(end)/1000)


class ManualSourceIterators:
    def high_cutoff(self):
        # return [0.7]
        return np.arange(0.6, 1, 0.25)

    def low_cutoff(self):
        return [0]


class ManualSuccessIterators:
    def percentage_increase(self):
        # return [0.05]
        return np.arange(0.025, 0.075, 0.001)

    def percentge_reduction(self):
        return [0]

    def days_to_run(self):
        # return [timedelta(days=20)]
        return [timedelta(days=12),
                timedelta(days=16),
                timedelta(days=20),
                timedelta(days=24),
                timedelta(days=28),
                timedelta(days=32)]


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



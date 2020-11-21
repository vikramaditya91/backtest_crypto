from datetime import datetime, timedelta
from typing import Union
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

    def get_time_intervals(self):
        while self.current_end >= self.current_start:
            yield self.current_start, self.current_end
            self._skip_to_next()

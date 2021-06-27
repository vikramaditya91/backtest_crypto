import matplotlib.pyplot as plt
from datetime import datetime
from backtest_crypto.utilities.iterators import TimeIntervalIterator


def main():
    start_date = datetime(day=25, month=1, year=2018)
    end_date = datetime(day=18, month=11, year=2020)
    interval = "3d"

    time_intervals_iterator = TimeIntervalIterator(start_date,
                                                   end_date,
                                                   interval,
                                                   forward_in_time=True,
                                                   increasing_range=False)

    start_dates = []
    end_dates = []

    for current_start, current_end in time_intervals_iterator.get_time_intervals_list():
        start_dates.append(current_start)
        end_dates.append(current_end)

    plt.plot(start_dates, marker=".", markersize=1)
    plt.plot(end_dates, marker=".", markersize=1)
    plt.text(0.5,
             0.5,
             f"Length of intervals: {len(start_dates)}",
             fontsize=100,
             horizontalalignment='center',
             verticalalignment='center')
    plt.show()


if __name__ == "__main__":
    main()





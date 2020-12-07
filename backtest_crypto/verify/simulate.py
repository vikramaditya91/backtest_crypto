from abc import ABC, abstractmethod

from backtest_crypto.history_collect.gather_history import get_history_between


class AbstractSimulationCreator(ABC):
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def validate_instance(self,
                          history_access,
                          potential_coins,
                          predicted_at,
                          simulation_timedelta,
                          success_criteria,
                          ohlcv_field,
                          *args,
                          **kwargs):
        concrete = self.factory_method(history_access,
                                       potential_coins,
                                       predicted_at,
                                       simulation_timedelta,
                                       ohlcv_field)
        criteria = {}
        for item in success_criteria:
            if concrete.confirm_check_valid():
                method = getattr(concrete, item)
                criteria[item] = method(*args, **kwargs)
            else:
                criteria[item] = None
        return criteria


class MarketBuyLimitSellCreator(AbstractSimulationCreator):
    def factory_method(self, *args, **kwargs):
        return MarketBuyLimitSellSimulatorConcrete(*args, **kwargs)


class AbstractSimulatorConcrete(ABC):
    def __init__(self,
                 history_access,
                 potential_coins,
                 predicted_at,
                 simulation_timedelta,
                 ohlcv_field
                 ):
        self.ohlcv_field = ohlcv_field
        self.potential_coins = potential_coins
        self.predicted_at = predicted_at
        self.simulation_timedelta = simulation_timedelta
        history_future, _ = get_history_between(history_access,
                                                start_time=predicted_at,
                                                end_time=predicted_at + simulation_timedelta,
                                                available=True,
                                                masked=False)
        self.history_future = history_future.fillna(0)

    @abstractmethod
    def number_of_bought_coins_hit_target(self, *args, **kwargs):
        pass

    def confirm_check_valid(self):
        if self.history_future.timestamp.__len__() == 0:
            return False
        return True


class MarketBuyLimitSellSimulatorConcrete(AbstractSimulatorConcrete):
    def number_of_bought_coins_hit_target(self, percentage_increase):
        max_relevant = self.history_future.sel(base_assets=self.potential_coins)
        max_values = max_relevant.max(axis=2).loc[
            {"ohlcv_fields": self.ohlcv_field}
        ]
        current_values = self.history_future.loc[
            {"ohlcv_fields": self.ohlcv_field}
        ].sel(timestamp=self.history_future.timestamp[0])
        truth_values = current_values * (1 + percentage_increase) < max_values
        return sum(truth_values.values.flatten()) / truth_values.base_assets.__len__()


def validate_success(creator: AbstractSimulationCreator,
                     history_access,
                     potential_coins,
                     predicted_at,
                     simulation_timedelta,
                     success_criteria,
                     ohlcv_field,
                     *args,
                     **kwargs):
    return creator.validate_instance(history_access,
                                     potential_coins,
                                     predicted_at,
                                     simulation_timedelta,
                                     success_criteria,
                                     ohlcv_field,
                                     *args,
                                     **kwargs)

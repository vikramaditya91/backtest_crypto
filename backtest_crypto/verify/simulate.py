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
    def percentage_of_bought_coins_hit_target(self, *args, **kwargs):
        pass

    @abstractmethod
    def end_of_run_value_of_bought_coins_if_not_sold(self, *args, **kwargs):
        pass

    def confirm_check_valid(self):
        if not self.potential_coins:
            return False
        if self.history_future.timestamp.__len__() == 0:
            return False
        return True


class MarketBuyLimitSellSimulatorConcrete(AbstractSimulatorConcrete):
    def percentage_of_bought_coins_hit_target(self,
                                              percentage_increase,
                                              days_to_run):
        relevant_coins = self.history_future.sel(base_assets=self.potential_coins,
                                               ohlcv_fields=[self.ohlcv_field])
        max_values = relevant_coins.max(axis=2)
        current_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[0]}]
        truth_values = current_values * (1 + percentage_increase) < max_values
        return sum(truth_values.values.flatten()) / truth_values.base_assets.__len__()

    def end_of_run_value_of_bought_coins_if_not_sold(self, *args, **kwargs):
        relevant_coins = self.history_future.sel(base_assets=self.potential_coins,
                                                 ohlcv_fields=[self.ohlcv_field])
        current_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[0]}]
        current_values = current_values.where(lambda x: x != 0, drop=True)
        quantity_bought = 1/current_values
        last_day_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[-1],
                                              "base_assets":current_values.base_assets}]
        return sum((last_day_values * quantity_bought).values.flatten())/len(relevant_coins.base_assets)

    def end_of_run_value_of_bought_coins_if_sold_on_target(self,
                                                           percentage_increase,
                                                           days_to_run):
        relevant_coins = self.history_future.sel(base_assets=self.potential_coins,
                                                 ohlcv_fields=[self.ohlcv_field])
        current_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[0]}]
        max_values = relevant_coins.max(axis=2)
        truth_values = current_values * (1 + percentage_increase) < max_values
        quantity_bought = 1/current_values
        bought_eth_worth = current_values * quantity_bought

        success_coins = truth_values.where(lambda x: x == True, drop=True).base_assets.values.tolist()
        sold_value = bought_eth_worth.sel(base_assets=success_coins) * (1+percentage_increase)

        unsuccessful_coins = list(set(relevant_coins.base_assets.values.tolist()) - set(success_coins))
        unsold_value = (relevant_coins.sel(timestamp=relevant_coins.timestamp[-1]) * quantity_bought).sel(base_assets=unsuccessful_coins)

        total_value = (sum(sold_value.values.flatten()) + sum(unsold_value.values.flatten())) / \
                        len(relevant_coins.base_assets)
        return total_value


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

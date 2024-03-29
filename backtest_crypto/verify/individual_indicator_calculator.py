import logging
from abc import ABC, abstractmethod

from backtest_crypto.history_collect.gather_history import get_simple_history
from backtest_crypto.utilities.general import InsufficientHistory

logger = logging.getLogger(__name__)


class AbstractIndicatorCreator(ABC):
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def validate_instance(self,
                          full_history_da_dict,
                          potential_coins,
                          predicted_at,
                          simulation_timedelta,
                          success_criteria,
                          ohlcv_field,
                          simulation_input_dict,
                          ):
        criteria = {}
        try:
            concrete = self.factory_method(full_history_da_dict,
                                           potential_coins,
                                           predicted_at,
                                           simulation_timedelta,
                                           ohlcv_field)
            for item in success_criteria:
                if concrete.confirm_check_valid():
                    method = getattr(concrete, item)
                    criteria[item] = method(simulation_input_dict)
                else:
                    criteria[item] = None
        except InsufficientHistory:
            logger.warning(f"Insufficient history on {predicted_at}")
        return criteria


class MarketBuyLimitSellIndicatorCreator(AbstractIndicatorCreator):
    def factory_method(self, *args, **kwargs):
        return MarketBuyLimitSellIndicatorConcrete(*args, **kwargs)


class AbstractIndicatorConcrete(ABC):
    _shared_state = {}

    def __init__(self,
                 full_history_da_dict,
                 potential_coins_dict,
                 predicted_at,
                 simulation_timedelta,
                 ohlcv_field
                 ):
        self.__dict__ = self._shared_state
        if not self._shared_state:
            self.overall_history_dict = {}
            self.maximum_history_dict = {}
        self.ohlcv_field = ohlcv_field
        self.potential_coins_list = list(potential_coins_dict.keys())
        self.predicted_at = predicted_at
        self.simulation_timedelta = simulation_timedelta

        if not (predicted_at, simulation_timedelta) in self.overall_history_dict.keys():
            future = get_simple_history(full_history_da_dict,
                                        start_time=predicted_at,
                                        end_time=predicted_at + simulation_timedelta,
                                        # TODO This should be parametrized
                                        candle="1h"
                                        )
            self.overall_history_dict[(predicted_at, simulation_timedelta)] = future.fillna(0)
        self.history_future = self.overall_history_dict[(predicted_at, simulation_timedelta)]

    @abstractmethod
    def percentage_of_bought_coins_hit_target(self, *args, **kwargs):
        pass

    @abstractmethod
    def end_of_run_value_of_bought_coins_if_not_sold(self, *args, **kwargs):
        pass

    @abstractmethod
    def end_of_run_value_of_bought_coins_if_sold_on_target(self, *args, **kwargs):
        pass

    def confirm_check_valid(self):
        if not self.potential_coins_list:
            return False
        if self.history_future.timestamp.__len__() == 0:
            return False
        return True


class MarketBuyLimitSellIndicatorConcrete(AbstractIndicatorConcrete):
    def percentage_of_bought_coins_hit_target(self,
                                              simulation_input_dict,
                                              ):
        percentage_increase = simulation_input_dict["percentage_increase"]
        relevant_coins = self.history_future.sel(base_assets=self.potential_coins_list,
                                                 ohlcv_fields=[self.ohlcv_field])
        max_values = relevant_coins.max(dim="timestamp")
        current_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[0]}]
        truth_values = current_values * (1 + percentage_increase) < max_values
        return sum(truth_values.values.flatten()) / truth_values.base_assets.__len__()

    def end_of_run_value_of_bought_coins_if_not_sold(self, *args, **kwargs):
        relevant_coins = self.history_future.sel(base_assets=self.potential_coins_list,
                                                 ohlcv_fields=[self.ohlcv_field])
        current_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[0]}]
        current_values = current_values.where(lambda x: x != 0, drop=True)
        quantity_bought = 1 / current_values
        last_day_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[-1],
                                              "base_assets": current_values.base_assets}]
        return sum((last_day_values * quantity_bought).values.flatten()) / len(relevant_coins.base_assets)

    def end_of_run_value_of_bought_coins_if_sold_on_target(self,
                                                           simulation_input_dict,
                                                           ):
        percentage_increase = simulation_input_dict["percentage_increase"]
        relevant_coins = self.history_future.sel(base_assets=self.potential_coins_list,
                                                 ohlcv_fields=[self.ohlcv_field])
        current_values = relevant_coins.loc[{"timestamp": relevant_coins.timestamp[0]}]
        current_values = current_values.where(lambda x: x != 0, drop=True)
        valid_coins = current_values.base_assets.values.tolist()
        if not valid_coins:
            return 0

        relevant_coins = relevant_coins.sel(base_assets=valid_coins)

        max_values = relevant_coins.sel(base_assets=valid_coins).max(axis=2)
        truth_values = current_values * (1 + percentage_increase) < max_values
        quantity_bought = 1 / current_values
        bought_eth_worth = current_values * quantity_bought

        success_coins = truth_values.where(lambda x: x == True, drop=True).base_assets.values.tolist()
        sold_value = bought_eth_worth.sel(base_assets=success_coins) * (1 + percentage_increase)

        unsuccessful_coins = list(set(valid_coins) - set(success_coins))
        unsold_value = (relevant_coins.sel(timestamp=relevant_coins.timestamp[-1]) * quantity_bought).sel(
            base_assets=unsuccessful_coins)

        total_value = (sum(sold_value.values.flatten()) + sum(unsold_value.values.flatten())) / \
                        len(relevant_coins.base_assets)
        return total_value


def calculate_indicator(creator: AbstractIndicatorCreator,
                        history_access,
                        potential_coins,
                        predicted_at,
                        simulation_timedelta,
                        success_criteria,
                        ohlcv_field,
                        simulation_input_dict,
                        ):
    return creator.validate_instance(history_access,
                                     potential_coins,
                                     predicted_at,
                                     simulation_timedelta,
                                     success_criteria,
                                     ohlcv_field,
                                     simulation_input_dict,
                                     )

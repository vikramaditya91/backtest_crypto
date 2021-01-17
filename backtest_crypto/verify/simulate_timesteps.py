import datetime
import functools
import logging
import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List

from backtest_crypto.history_collect.gather_history import get_instantaneous_history, get_simple_history
from backtest_crypto.utilities.general import InsufficientHistory, MissingPotentialCoinTimeIndexError
from backtest_crypto.utilities.iterators import TimeIntervalIterator

logger = logging.getLogger(__name__)


@dataclass
class HoldingCoin:
    coin_name: str
    quantity: float
    bought_time: datetime.datetime
    bought_price: float
    locked: bool


class AbstractTimeStepSimulateCreator(ABC):
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def simulate_timesteps(self,
                           history_access,
                           ohlcv_field,
                           simulation_input_dict,
                           potential_coin_client,
                           simulate_criteria
                           ):
        criteria = {}
        concrete = self.factory_method(history_access,
                                       ohlcv_field,
                                       potential_coin_client)
        for simulate_criterion in simulate_criteria:
            method = getattr(concrete, simulate_criterion)
            criteria[simulate_criterion] = method(simulation_input_dict)
        return criteria


class MarketBuyLimitSellSimulationCreator(AbstractTimeStepSimulateCreator):
    def factory_method(self, *args, **kwargs):
        return MarketBuyLimitSellSimulatorConcrete(*args, **kwargs)


class ValidPotentialCoins:
    def __init__(self):
        a = 1


class AbstractTimestepSimulatorConcrete(ABC):
    _shared_state = {}

    def __init__(self,
                 history_access,
                 ohlcv_field,
                 potential_coin_client,
                 ):
        self.__dict__ = self._shared_state
        if not self._shared_state:
            self.overall_history_dict = {}
            self.maximum_history_dict = {}
            self.coins_with_valid_history = {}
        self.ohlcv_field = ohlcv_field
        self.history_access = history_access
        self.potential_coin_client = potential_coin_client
        self.reference_coin = "BTC"
        self.candle = "1h"
        self.tolerance = 0.001
        self.trade_executed = 0
        self.banned_coins = {}

    def should_buy_altcoin(self,
                           holdings,
                           ):
        reference_qty = self.get_coin_qty(holdings,
                                          self.reference_coin)
        if reference_qty:
            return reference_qty > self.tolerance
        return False

    @staticmethod
    def get_coin_qty(holdings,
                     coin_name):
        for holding in holdings:
            if holding.coin_name == coin_name:
                return holding.quantity
        return 0

    def log_holding_value(self,
                          holdings,
                          simulation_time):
        try:
            if (simulation_time.day % 5) == 0 and (simulation_time.hour == 1):
                logger.debug(f"Holdings are worth"
                             f" {self.get_total_holding_worth(holdings, simulation_time)} "
                             f"at {simulation_time}")
        except InsufficientHistory as e:
            pass

    def get_total_holding_worth(self,
                                holding,
                                current_time):
        instant_price_da = get_instantaneous_history(self.history_access,
                                                     current_time,
                                                     candle=self.candle
                                                     )
        instant_price_da[self.reference_coin] = 1
        total_holding = functools.reduce(
            lambda x, y: x + y.quantity * instant_price_da[y.coin_name],
            holding, 0)
        return total_holding

    def has_holding_reached_timeout(self,
                                    holding: HoldingCoin,
                                    current_time: datetime.datetime,
                                    days_to_run: datetime.timedelta):
        if holding.coin_name == self.reference_coin:
            return False
        return (holding.bought_time + days_to_run) < current_time

    def calculate_end_of_run_value(self, simulation_input_dict):
        simulation_start, simulation_end = TimeIntervalIterator.get_datetime_objects_from_str(
            simulation_input_dict["time_intervals"]
        )
        holdings = [HoldingCoin(
            coin_name=self.reference_coin,
            quantity=1,
            bought_time=simulation_start,
            bought_price=0,
            locked=False
        )]
        for simulation_at in TimeIntervalIterator.time_iterator(simulation_start,
                                                                simulation_end,
                                                                interval=TimeIntervalIterator.string_to_datetime(
                                                                    self.candle)):
            holdings = self.manage_simulation_per_timestep(holdings,
                                                           simulation_start,
                                                           simulation_at,
                                                           simulation_input_dict)

        return self.get_total_holding_worth(holdings,
                                            simulation_end)

    @abstractmethod
    def manage_simulation_per_timestep(self, *args, **kwargs):
        pass

    def limit_sell_altcoins_that_hit_target(self,
                                            holdings,
                                            current_time,
                                            simulation_input_dict):
        try:
            instance_price_dict = get_instantaneous_history(self.history_access,
                                                            current_time,
                                                            candle=self.candle
                                                            )
        except InsufficientHistory as e:
            logger.debug(f"History not present in {current_time}")
        else:
            holdings_copy = holdings.copy()
            for holding in holdings_copy:
                if self.has_holding_reached_timeout(holding,
                                                    current_time,
                                                    simulation_input_dict["days_to_run"]) or \
                        self.has_holding_reached_sell_limit_price(holding,
                                                                  instance_price_dict,
                                                                  simulation_input_dict["percentage_increase"]
                                                                  ):
                    holdings = self._limit_sell_individual_altcoin(holdings,
                                                                   holding,
                                                                   instance_price_dict,
                                                                   current_time)
        return holdings

    def _limit_sell_individual_altcoin(self,
                                       holdings,
                                       holding,
                                       instance_price_dict,
                                       current_time):
        holdings.remove(holding)
        reference_coin_qty_added = holding.quantity * instance_price_dict[holding.coin_name]
        self.trade_executed += reference_coin_qty_added
        return self._append_reference_coin(holdings,
                                           reference_coin_qty_added,
                                           current_time)

    def has_holding_reached_sell_limit_price(self,
                                             holding: HoldingCoin,
                                             instant_price_dict: Dict,
                                             expected_price_increase: float):
        if holding.coin_name == self.reference_coin:
            return False
        return ((1 + expected_price_increase) * holding.bought_price) < \
               (instant_price_dict[holding.coin_name])

    def _append_reference_coin(self,
                               holdings,
                               reference_coin_qty,
                               current_time):
        for holding in holdings:
            if holding.coin_name == self.reference_coin:
                holding.quantity = holding.quantity + reference_coin_qty
                break
        else:
            holdings.append(HoldingCoin(coin_name=self.reference_coin,
                                        quantity=reference_coin_qty,
                                        bought_time=current_time,
                                        bought_price=1,
                                        locked=False))
        return holdings

    def get_potential_valid_altcoins_no_held(self,
                                             potential_coins: List,
                                             instant_price_dict,
                                             holdings
                                             ):
        potential_coins_set = set(potential_coins)
        potential_valid_altcoin = list(potential_coins_set.intersection(instant_price_dict.keys()))
        potential_valid_altcoin_not_held = list(set(potential_valid_altcoin) -
                                                set(map(lambda x: x.coin_name, holdings)) -
                                                set(self.banned_coins))
        return potential_valid_altcoin_not_held

    def market_buy_altcoin_from_reference_coin_overall(self,
                                                       current_time,
                                                       holdings,
                                                       potential_coins,
                                                       max_types_of_coins):
        try:
            instant_price_dict = get_instantaneous_history(self.history_access,
                                                           current_time,
                                                           candle=self.candle
                                                           )
        except InsufficientHistory as e:
            return holdings

        max_ref_coin_in_order = 1 / max_types_of_coins
        reference_coin_qty = self.get_coin_qty(holdings,
                                               coin_name=self.reference_coin)
        altcoins_number_to_buy = math.ceil(reference_coin_qty / max_ref_coin_in_order)

        potential_valid_altcoin_not_held = self.get_potential_valid_altcoins_no_held(potential_coins,
                                                                                     instant_price_dict,
                                                                                     holdings)
        for altcoin_index in range(altcoins_number_to_buy):
            if not potential_valid_altcoin_not_held:
                continue
            holdings = self._buy_random_altcoin_individual(holdings,
                                                           max_ref_coin_in_order,
                                                           potential_valid_altcoin_not_held,
                                                           instant_price_dict,
                                                           current_time)
        return holdings

    def _buy_random_altcoin_individual(self,
                                       holdings,
                                       max_ref_coin_in_order,
                                       potential_valid_altcoin_not_held,
                                       instant_price_dict,
                                       current_time):
        reference_coin_qty_to_sell = min(self.get_coin_qty(holdings,
                                                           self.reference_coin),
                                         max_ref_coin_in_order)
        coin_to_buy = random.choice(potential_valid_altcoin_not_held)
        potential_valid_altcoin_not_held.remove(coin_to_buy)
        altcoin_price = instant_price_dict[coin_to_buy]
        qty_of_altcoin = reference_coin_qty_to_sell / altcoin_price

        holdings = self._pop_reference_from_holding(holdings,
                                                    reference_coin_qty_to_sell
                                                    )
        holdings.append(HoldingCoin(coin_name=coin_to_buy,
                                    quantity=qty_of_altcoin,
                                    bought_time=current_time,
                                    bought_price=altcoin_price,
                                    locked=False))
        self.trade_executed += reference_coin_qty_to_sell
        return holdings

    def _pop_reference_from_holding(self,
                                    holdings,
                                    reference_coin_qty_to_sell):
        holdings_copy = holdings.copy()
        for holding in holdings_copy:
            try:
                if self.reference_coin == holding.coin_name:
                    if holding.quantity == reference_coin_qty_to_sell:
                        holdings.remove(holding)
                    else:
                        holding.quantity = holding.quantity - reference_coin_qty_to_sell
            except IndexError:
                pass
        return holdings

    def add_valid_coins_with_history(self,
                                     start_time,
                                     end_time):
        simple_history = get_simple_history(self.history_access,
                                            start_time,
                                            end_time,
                                            self.candle)
        particular_history = simple_history.sel({"ohlcv_fields": self.ohlcv_field})
        nan_values = particular_history.isnull().sum(axis=1)
        sufficient_history_coins = nan_values.where(lambda x: x == 0, drop=True).base_assets.values.tolist()
        self.coins_with_valid_history[start_time, end_time] = sufficient_history_coins

    def get_cached_history(self,
                                history_start,
                                history_end,
                                ):
        for start, end in self.coins_with_valid_history.keys():
            if (start <= history_start) and (end >= history_end):
                return self.coins_with_valid_history[start, end]
        raise ValueError

    def get_coins_with_sufficient_history(self,
                                          history_start,
                                          history_end,
                                          cache_padding=datetime.timedelta(days=5)):
        try:
            return self.get_cached_history(history_start,
                                           history_end)
        except ValueError:
            self.add_valid_coins_with_history(history_start,
                                              history_end+cache_padding)
        return self.get_coins_with_sufficient_history(history_start,
                                                      history_end)

    def filter_coins_with_history(self,
                                  coins: List,
                                  history_start: datetime.datetime,
                                  history_end: datetime.datetime,
                                  ) -> List:
        sufficient_history_coins = self.get_coins_with_sufficient_history(history_start,
                                                                          history_end)
        return list(set(coins).intersection(set(sufficient_history_coins)))

    def get_valid_potential_coin_to_buy(self,
                                        simulation_input_dict: Dict,
                                        simulation_start: datetime.datetime,
                                        simulation_at:datetime.datetime) -> List:
        try:
            potential_coins = self.potential_coin_client.get_potential_coin_at(
                consider_history=(simulation_start, simulation_at),
                potential_coin_strategy={**simulation_input_dict,
                                         "ohlcv_field": self.ohlcv_field,
                                         "reference_coin": self.reference_coin}
            )
        except MissingPotentialCoinTimeIndexError:
            return []
        else:
            filtered_coins = self.filter_coins_with_history(
                coins=list(potential_coins),
                history_start=simulation_at,
                history_end=simulation_at+simulation_input_dict["days_to_run"],
            )
            return filtered_coins


class MarketBuyLimitSellSimulatorConcrete(AbstractTimestepSimulatorConcrete):
    def manage_simulation_per_timestep(self,
                                       holdings: List,
                                       simulation_start: datetime.datetime,
                                       simulation_at: datetime.datetime,
                                       simulation_input_dict: Dict) -> List:
        if not ((len(holdings) == 1) and (holdings[0].coin_name == self.reference_coin)):
            holdings = self.limit_sell_altcoins_that_hit_target(holdings,
                                                                simulation_at,
                                                                simulation_input_dict)
        if self.should_buy_altcoin(holdings):
            potential_coins = self.get_valid_potential_coin_to_buy(simulation_input_dict,
                                                                   simulation_start,
                                                                   simulation_at)
            if potential_coins:
                holdings = self.market_buy_altcoin_from_reference_coin_overall(simulation_at,
                                                                               holdings,
                                                                               potential_coins,
                                                                               simulation_input_dict[
                                                                                   "max_coins_to_buy"], )
        self.log_holding_value(holdings,
                               simulation_at)
        return holdings


def calculate_simulation(creator: AbstractTimeStepSimulateCreator,
                         *args,
                         **kwargs):
    return creator.simulate_timesteps(*args,
                                      **kwargs)

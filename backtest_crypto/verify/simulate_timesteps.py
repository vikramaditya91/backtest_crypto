import logging
import datetime
import math
import random
import functools
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Dict
from backtest_crypto.history_collect.gather_history import get_instantaneous_history
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from backtest_crypto.utilities.general import InsufficientHistory

logger = logging.getLogger(__name__)


@dataclass
class HoldingCoin:
    coin_name: str
    quantity: float
    bought_time: datetime.datetime
    bought_price: float


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
        self.ohlcv_field = ohlcv_field
        self.history_access = history_access
        self.potential_coin_client = potential_coin_client
        self.reference_coin = "BTC"
        self.candle = "1h"
        self.tolerance = 0.001
        self.trade_executed = 0
        self.banned_coins = {"NPXS", "DENT", "KEY", "NCASH", "MFT"}

    @abstractmethod
    def calculate_end_of_run_value(self, *args, **kwargs):
        raise NotImplementedError

    def obtain_potential(self,
                         potential_coin_client,
                         coordinate_dict,
                         potential_start,
                         potential_end):
        potential_coin_strategy = dict(low_cutoff=coordinate_dict["low_cutoff"],
                                       high_cutoff=coordinate_dict["high_cutoff"],
                                       reference_coin=self.reference_coin,
                                       ohlcv_field=self.ohlcv_field)
        consider_history = (potential_start, potential_end)
        return potential_coin_client.get_potential_coin_at(
            consider_history=consider_history,
            potential_coin_strategy=potential_coin_strategy,
        )

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


class MarketBuyLimitSellSimulatorConcrete(AbstractTimestepSimulatorConcrete):
    def calculate_end_of_run_value(self, simulation_input_dict):
        simulation_start, simulation_end = TimeIntervalIterator.get_datetime_objects_from_str(
            simulation_input_dict["time_intervals"]
        )
        holdings = [HoldingCoin(
            coin_name=self.reference_coin,
            quantity=1,
            bought_time=simulation_start,
            bought_price=0
        )]
        for simulation_at in TimeIntervalIterator.time_iterator(simulation_start,
                                                                simulation_end,
                                                                interval=TimeIntervalIterator.string_to_datetime(self.candle)):
            if not ((len(holdings) == 1) and (holdings[0].coin_name == self.reference_coin)):
                holdings = self.sell_altcoins_that_hit_target(holdings,
                                                         simulation_at,
                                                             simulation_input_dict)
            if self.should_buy_altcoin(holdings):
                try:
                    potential_coins = self.obtain_potential(self.potential_coin_client,
                                                         coordinate_dict=simulation_input_dict,
                                                         potential_start=simulation_start,
                                                         potential_end=simulation_at)
                except KeyError as e:
                    continue

                holdings = self.buy_altcoin_from_reference_coin_overall(simulation_at,
                                                                       holdings,
                                                                       potential_coins,
                                                                       simulation_input_dict["max_coins_to_buy"], )
        return self.get_total_holding_worth(holdings,
                                            simulation_end)

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
        return (holding.bought_time + days_to_run) > current_time

    def has_holding_reached_target_price(self,
                                         holding: HoldingCoin,
                                         instant_price_dict: Dict,
                                         expected_price_increase: float):
        if holding.coin_name == self.reference_coin:
            return False
        return ((1 + expected_price_increase) * holding.bought_price) < \
               (instant_price_dict[holding.coin_name])

    def sell_altcoins_that_hit_target(self,
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
                                                    simulation_input_dict["days_to_run"]) or\
                        self.has_holding_reached_target_price(holding,
                                                              instance_price_dict,
                                                              simulation_input_dict["percentage_increase"]
                                                              ):
                    holdings.remove(holding)
                    reference_coin_qty_added = holding.quantity * instance_price_dict[holding.coin_name]
                    self.trade_executed += reference_coin_qty_added
                    holdings = self._buy_reference_coin(holdings,
                                                        reference_coin_qty_added,
                                                        current_time)
        return holdings

    def _buy_reference_coin(self,
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
                                        bought_price=1))
        return holdings


    def buy_altcoin_from_reference_coin_overall(self,
                                                current_time,
                                                holdings,
                                                potential_coins,
                                                max_types_of_coins):
        reference_coin_qty = self.get_coin_qty(holdings,
                                               coin_name=self.reference_coin)
        instant_price_dict = get_instantaneous_history(self.history_access,
                                                  current_time,
                                                  candle=self.candle
                                                  )

        max_ref_coin_in_order = 1/max_types_of_coins
        altcoins_number_to_buy = math.ceil(reference_coin_qty/max_ref_coin_in_order)
        potential_coins_set = set(potential_coins.keys())

        potential_valid_altcoin = list(potential_coins_set.intersection(instant_price_dict.keys()))
        potential_valid_altcoin_not_held = list(set(potential_valid_altcoin) -
                                                set(map(lambda x: x.coin_name, holdings)) -
                                                # TODO Better way to do it
                                                set(self.banned_coins))

        for altcoin_index in range(altcoins_number_to_buy):
            if not potential_valid_altcoin_not_held:
                continue
            reference_coin_qty_to_sell = min(self.get_coin_qty(holdings,
                                                               self.reference_coin),
                                             max_ref_coin_in_order)
            coin_to_buy = random.choice(potential_valid_altcoin_not_held)
            potential_valid_altcoin_not_held.remove(coin_to_buy)
            altcoin_price = instant_price_dict[coin_to_buy]
            qty_of_altcoin = reference_coin_qty_to_sell/altcoin_price

            holdings = self._sell_reference_coin(holdings,
                                                 reference_coin_qty_to_sell
                                                 )
            holdings.append(HoldingCoin(coin_name=coin_to_buy,
                                        quantity=qty_of_altcoin,
                                        bought_time=current_time,
                                        bought_price=altcoin_price))
            self.trade_executed += reference_coin_qty_to_sell
        return holdings

    def _sell_reference_coin(self,
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


def calculate_simulation(creator: AbstractTimeStepSimulateCreator,
                         *args,
                         **kwargs):
    return creator.simulate_timesteps(*args,
                                      **kwargs)

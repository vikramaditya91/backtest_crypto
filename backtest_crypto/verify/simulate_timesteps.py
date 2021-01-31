import datetime
import functools
import logging
import math
import random
from abc import ABC, abstractmethod
from typing import Dict, List

from backtest_crypto.history_collect.gather_history import get_instantaneous_history, get_simple_history
from backtest_crypto.utilities.general import InsufficientHistory, \
    MissingPotentialCoinTimeIndexError, InsufficientBalance, Order, HoldingCoin, OrderType, OrderSide, \
    OrderFill
from backtest_crypto.utilities.iterators import TimeIntervalIterator

logger = logging.getLogger(__name__)


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


class LimitBuyLimitSellSimulationCreator(AbstractTimeStepSimulateCreator):
    def factory_method(self, *args, **kwargs):
        return LimitBuyLimitSellSimulatorConcrete(*args, **kwargs)


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
        self.live_orders = []
        self.banned_coins = {}
        self.insignificant_dust = 0.001
        self.holding_operations = HoldingOperations(self.reference_coin,
                                                    self.tolerance,
                                                    self.history_access,
                                                    self.candle)
        self.potential_identification = PotentialIdentification(self.history_access,
                                                                self.potential_coin_client,
                                                                self.candle,
                                                                self.ohlcv_field,
                                                                self.reference_coin,
                                                                self.coins_with_valid_history)

    @abstractmethod
    def manage_simulation_per_timestep(self, *args, **kwargs):
        pass

    def calculate_end_of_run_value(self, simulation_input_dict):
        simulation_start, simulation_end = TimeIntervalIterator.get_datetime_objects_from_str(
            simulation_input_dict["time_intervals"]
        )
        holdings = [HoldingCoin(
            coin_name=self.reference_coin,
            quantity=1,
            locked=False
        )]
        self.live_orders = []
        for simulation_at in TimeIntervalIterator.time_iterator(simulation_start,
                                                                simulation_end,
                                                                interval=TimeIntervalIterator.string_to_datetime(
                                                                    self.candle)):
            holdings = self.manage_simulation_per_timestep(holdings,
                                                           simulation_start,
                                                           simulation_at,
                                                           simulation_input_dict)
        self.live_orders = []
        return self.holding_operations.get_total_holding_worth(holdings,
                                                               simulation_end)

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

    def set_buy_orders_reference_to_alt(self,
                                        holdings,
                                        current_time,
                                        potential_coins,
                                        simulation_input_dict,
                                        order_type):
        try:
            instant_price_dict = get_instantaneous_history(self.history_access,
                                                           current_time,
                                                           candle=self.candle
                                                           )
        except InsufficientHistory:
            return

        max_ref_coin_in_order = 1 / simulation_input_dict["max_coins_to_buy"]
        quantity_of_ref_avl = self.holding_operations.unlocked_coin_avl(holdings,
                                                     self.reference_coin)
        altcoin_holdings_held = len(list(filter(lambda x: x.coin_name != self.reference_coin, holdings)))

        altcoins_number_to_buy = math.ceil(quantity_of_ref_avl / max_ref_coin_in_order) - altcoin_holdings_held

        potential_valid_altcoin_not_held = self.get_potential_valid_altcoins_no_held(potential_coins,
                                                                                     instant_price_dict,
                                                                                     holdings)

        for altcoin_index in range(altcoins_number_to_buy):
            if not potential_valid_altcoin_not_held:
                continue
            self._set_individual_buy_order(holdings,
                                           simulation_input_dict,
                                           potential_valid_altcoin_not_held,
                                           instant_price_dict,
                                           current_time,
                                           order_type)

    def _set_individual_buy_order(self,
                                  holdings,
                                  simulation_input_dict,
                                  potential_valid_altcoin_not_held,
                                  instant_price_dict,
                                  current_time,
                                  order_type):
        if order_type == OrderType.Limit:
            self._set_limit_buy_order(holdings,
                                      simulation_input_dict,
                                      potential_valid_altcoin_not_held,
                                      instant_price_dict,
                                      current_time)
        elif order_type == OrderType.Market:
            self._set_limit_buy_order(holdings,
                                      simulation_input_dict,
                                      potential_valid_altcoin_not_held,
                                      instant_price_dict,
                                      current_time)
        else:
            raise NotImplementedError

    def _set_limit_buy_order(self,
                             holdings,
                             simulation_input_dict,
                             potential_valid_altcoin_not_held,
                             instant_price_dict,
                             current_time
                             ):
        max_ref_coin_in_order = 1 / simulation_input_dict["max_coins_to_buy"]

        reference_coin_qty_to_sell = min(self.holding_operations.get_coin_qty(holdings,
                                                                              self.reference_coin),
                                         max_ref_coin_in_order)
        coin_to_buy = random.choice(potential_valid_altcoin_not_held)
        potential_valid_altcoin_not_held.remove(coin_to_buy)
        altcoin_current_price = instant_price_dict[coin_to_buy]
        qty_of_altcoin_to_buy = reference_coin_qty_to_sell / altcoin_current_price
        for holding in holdings:
            if holding.coin_name == self.reference_coin:
                holding.locked = True

        self.live_orders.append(
            Order(order_side=OrderSide.Buy,
                  order_type=OrderType.Limit,
                  quantity=qty_of_altcoin_to_buy,
                  base_asset=coin_to_buy,
                  reference_coin=self.reference_coin,
                  stop_price=-1,
                  timeout=current_time,
                  limit_price=altcoin_current_price * (1 - simulation_input_dict["percentage_reduction"]),
                  complete=OrderFill.Fresh
                  )
        )

    def try_execute_open_orders(self,
                                holdings,
                                current_time):
        if not self.live_orders:
            return holdings

        try:
            instant_price_dict = get_instantaneous_history(self.history_access,
                                                           current_time,
                                                           candle=self.candle
                                                           )
        except InsufficientHistory:
            return holdings
        order_operations = OrderOperations()
        for order in self.live_orders:
            instance_price = instant_price_dict[f'{order.base_asset}']
            holdings = order_operations._execute_individual_order(holdings,
                                                                  order,
                                                                  instance_price,
                                                                  current_time)
        return holdings

    def remove_dead_orders(self,
                           holdings,
                           current_time):
        self.live_orders = list(filter(lambda x: not ((x != OrderFill.Filled) or (x.timeout > current_time)),
                                       self.live_orders))
        holding_names_held = list(holding.coin_name for holding in holdings)
        for order in self.live_orders:
            if order.base_asset not in holding_names_held:
                if order.complete != OrderFill.Filled:
                    a = 1

    def buy_altcoin_overall(self,
                            holdings,
                            simulation_input_dict,
                            simulation_start,
                            simulation_at,
                            order_type
                            ):
        if self.holding_operations.should_buy_altcoin(holdings):
            potential_coins = self.potential_identification.get_valid_potential_coin_to_buy(simulation_input_dict,
                                                                                            simulation_start,
                                                                                            simulation_at)
            if potential_coins and \
                    (len(holdings) <= simulation_input_dict["max_coins_to_buy"]):
                self.set_buy_orders_reference_to_alt(holdings,
                                                     simulation_at,
                                                     potential_coins,
                                                     simulation_input_dict,
                                                     order_type
                                                     )

    def _set_sell_order_on_holding(self,
                                   holding,
                                   current_price,
                                   simulation_input_dict,
                                   current_time,
                                   order_type):
        if order_type == OrderType.Limit:
            order = Order(order_side=OrderSide.Sell,
                          order_type=OrderType.Limit,
                          base_asset=holding.coin_name,
                          reference_coin=self.reference_coin,
                          quantity=holding.quantity,
                          limit_price=current_price * (1 + simulation_input_dict['percentage_increase']),
                          timeout=simulation_input_dict['days_to_run'] + current_time,
                          stop_price=-1,
                          complete=OrderFill.Fresh
                          )
        else:
            raise NotImplementedError
        self.live_orders.append(order)

    def sell_altcoin_overall(self,
                             holdings,
                             current_time,
                             simulation_input_dict,
                             order_type):
        if self.holding_operations.if_altcoins_held(holdings):
            try:
                instance_price_dict = get_instantaneous_history(self.history_access,
                                                                current_time,
                                                                candle=self.candle
                                                                )
            except InsufficientHistory as e:
                logger.debug(f"History not present in {current_time}")
            else:
                for holding in holdings:
                    if holding.coin_name != self.reference_coin:
                        if holding.locked is False:
                            self._set_sell_order_on_holding(holding,
                                                            instance_price_dict[holding.coin_name],
                                                            simulation_input_dict,
                                                            current_time,
                                                            order_type)
                            holding.locked = True
                a = 1


class PotentialIdentification:
    def __init__(self,
                 history_access,
                 potential_coin_client,
                 candle,
                 ohlcv_field,
                 reference_coin,
                 coins_with_valid_history):
        self.history_access = history_access
        self.potential_coin_client = potential_coin_client
        self.candle = candle
        self.ohlcv_field = ohlcv_field
        self.reference_coin = reference_coin
        self.coins_with_valid_history = coins_with_valid_history

    def get_cached_history(self,
                           history_start: datetime.datetime,
                           history_end: datetime.datetime,
                           ) -> List:
        for start, end in self.coins_with_valid_history.keys():
            if (start <= history_start) and (end >= history_end):
                return self.coins_with_valid_history[start, end]
        raise ValueError

    def get_valid_potential_coin_to_buy(self,
                                        simulation_input_dict: Dict,
                                        simulation_start: datetime.datetime,
                                        simulation_at: datetime.datetime) -> List:
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
                history_end=simulation_at + simulation_input_dict["days_to_run"],
            )
            return filtered_coins

    def filter_coins_with_history(self,
                                  coins: List,
                                  history_start: datetime.datetime,
                                  history_end: datetime.datetime,
                                  ) -> List:
        sufficient_history_coins = self.get_coins_with_sufficient_history(history_start,
                                                                          history_end)
        return list(set(coins).intersection(set(sufficient_history_coins)))

    def get_coins_with_sufficient_history(self,
                                          history_start: datetime.datetime,
                                          history_end: datetime.datetime,
                                          cache_padding: datetime.timedelta = datetime.timedelta(days=5)):
        try:
            return self.get_cached_history(history_start,
                                           history_end)
        except ValueError:
            self.add_valid_coins_with_history(history_start,
                                              history_end + cache_padding)
        return self.get_coins_with_sufficient_history(history_start,
                                                      history_end)

    def add_valid_coins_with_history(self,
                                     start_time: datetime.datetime,
                                     end_time: datetime.datetime) -> None:
        simple_history = get_simple_history(self.history_access,
                                            start_time,
                                            end_time,
                                            self.candle)
        particular_history = simple_history.sel({"ohlcv_fields": self.ohlcv_field})
        nan_values = particular_history.isnull().sum(axis=1)
        sufficient_history_coins = nan_values.where(lambda x: x == 0, drop=True).base_assets.values.tolist()
        self.coins_with_valid_history[start_time, end_time] = sufficient_history_coins


class HoldingOperations:
    def __init__(self, reference_coin, tolerance, history_access, candle):
        self.reference_coin = reference_coin
        self.tolerance = tolerance
        self.history_access = history_access
        self.candle = candle

    def unlocked_coin_avl(self,
                          holdings,
                          coin_name):
        for holding in holdings:
            if holding.coin_name == coin_name:
                if holding.locked is False:
                    return holding.quantity
        return 0

    def if_altcoins_held(self,
                         holdings):
        return not ((len(holdings) == 1) and (holdings[0].coin_name == self.reference_coin))

    def should_buy_altcoin(self,
                           holdings,
                           ):
        for holding in holdings:
            if holding.coin_name == self.reference_coin:
                if holding.locked is False:
                    if holding.quantity > self.tolerance:
                        return True
        return False

    @staticmethod
    def get_coin_qty(holdings,
                     coin_name):
        for holding in holdings:
            if holding.coin_name == coin_name:
                return holding.quantity
        raise InsufficientBalance("Coin not in the list")

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
                                holdings,
                                current_time):
        try:
            instance_price_dict = get_instantaneous_history(self.history_access,
                                                            current_time,
                                                            candle=self.candle
                                                            )
            instance_price_dict[self.reference_coin] = 1
        except InsufficientHistory as e:
            return None
        else:
            total_holding = functools.reduce(
                lambda x, y: x + y.quantity * instance_price_dict[y.coin_name],
                holdings, 0)
        return total_holding


class OrderOperations:
    insignificant_dust = 0.001

    def _execute_individual_order(self,
                                  holdings,
                                  order: Order,
                                  current_price: float,
                                  current_time: datetime.datetime):
        add, remove = self._get_add_remove_holdings(order,
                                                    current_price,
                                                    current_time)
        try:
            if (add is not None) and (remove is not None):
                self._remove_item_from_holdings(holdings,
                                                remove)
                self._add_item_from_holdings(holdings,
                                             add,
                                             )
                order.complete = OrderFill.Filled
        except InsufficientBalance as e:
            pass
            # logger.warning(f"Insufficient balance {e}")
        return holdings

    @staticmethod
    def has_order_reached_timeout(order,
                                  current_time):
        return order.timeout < current_time

    def _get_add_remove_holdings(self,
                                 order: Order,
                                 current_price: float,
                                 current_time: datetime.datetime):
        remove = None
        add = None
        if (order.order_type == OrderType.Market) or \
                (self.has_order_reached_timeout(order,
                                                current_time)):
            if order.order_side == OrderSide.Buy:
                remove = (order.reference_coin, order.quantity * current_price, 0)
                add = (order.base_asset, order.quantity, current_price)
            else:
                add = (order.reference_coin, order.quantity * current_price, 0)
                remove = (order.base_asset, order.quantity, current_price)
        elif order.order_type == OrderType.Limit:
            if order.order_side == OrderSide.Buy:
                if order.limit_price >= current_price:
                    remove = (order.reference_coin, order.quantity * current_price, 0)
                    add = (order.base_asset, order.quantity, current_price)
            elif order.order_side == OrderSide.Sell:
                if order.limit_price <= current_price:
                    add = (order.reference_coin, order.quantity * current_price, 0)
                    remove = (order.base_asset, order.quantity, current_price)
        return add, remove

    @staticmethod
    def _add_item_from_holdings(holdings,
                                add
                                ):
        for holding in holdings:
            if holding.coin_name == add[0]:
                if holding.locked is False:
                    holding.quantity = holding.quantity + add[1]
                    return
        holdings.append(HoldingCoin(coin_name=add[0],
                                    quantity=add[1],
                                    locked=False))

    def _remove_item_from_holdings(self,
                                   holdings,
                                   remove):
        for holding_index in range(len(holdings)):
            if (holdings[holding_index].coin_name == remove[0]) and \
                    (holdings[holding_index].quantity >= remove[1]):
                if abs(holdings[holding_index].quantity - remove[1]) < self.insignificant_dust:
                    holdings.pop(holding_index)
                else:
                    holdings[holding_index].quantity = holdings[holding_index].quantity - remove[1]
                return
        raise InsufficientBalance(f"Did not have {remove} to remove from holdings {holdings}")


class MarketBuyLimitSellSimulatorConcrete(AbstractTimestepSimulatorConcrete):
    def manage_simulation_per_timestep(self,
                                       holdings: List,
                                       simulation_start: datetime.datetime,
                                       simulation_at: datetime.datetime,
                                       simulation_input_dict: Dict) -> List:

        self.holding_operations.log_holding_value(holdings,
                                                  simulation_at)

        self.buy_altcoin_overall(holdings,
                                 simulation_input_dict,
                                 simulation_start,
                                 simulation_at,
                                 order_type=OrderType.Market
                                 )
        holdings = self.try_execute_open_orders(holdings,
                                                simulation_at)
        self.remove_dead_orders(holdings,
                                current_time=simulation_at)
        self.sell_altcoin_overall(holdings,
                                  simulation_at,
                                  simulation_input_dict,
                                  order_type=OrderType.Limit)

        holdings = self.try_execute_open_orders(holdings,
                                                simulation_at)
        self.remove_dead_orders(holdings,
                                current_time=simulation_at)
        return holdings


class LimitBuyLimitSellSimulatorConcrete(AbstractTimestepSimulatorConcrete):
    def manage_simulation_per_timestep(self,
                                       holdings: List,
                                       simulation_start: datetime.datetime,
                                       simulation_at: datetime.datetime,
                                       simulation_input_dict: Dict) -> List:
        self.holding_operations.log_holding_value(holdings,
                               simulation_at)

        self.buy_altcoin_overall(holdings,
                                 simulation_input_dict,
                                 simulation_start,
                                 simulation_at,
                                 order_type=OrderType.Limit
                                 )
        holdings = self.try_execute_open_orders(holdings,
                                                simulation_at)
        self.sell_altcoin_overall(holdings,
                                  simulation_at,
                                  simulation_input_dict,
                                  order_type=OrderType.Limit
                                  )

        self.remove_dead_orders(holdings,
                                current_time=simulation_at)
        holdings = self.try_execute_open_orders(holdings,
                                                simulation_at)
        return holdings


def calculate_simulation(creator: AbstractTimeStepSimulateCreator,
                         *args,
                         **kwargs):
    return creator.simulate_timesteps(*args,
                                      **kwargs)

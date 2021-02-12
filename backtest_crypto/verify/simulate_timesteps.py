import datetime
import functools
import logging
import math
import random
from abc import ABC, abstractmethod
from typing import Dict, List

from backtest_crypto.history_collect.gather_history import get_instantaneous_history
from backtest_crypto.utilities.general import InsufficientHistory,\
    InsufficientBalance, Order, HoldingCoin, OrderType, OrderSide, OrderFill
from backtest_crypto.utilities.iterators import TimeIntervalIterator
from backtest_crypto.verify.identify_potential_coins import PotentialIdentification

logger = logging.getLogger(__name__)
Holdings = List[HoldingCoin]


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
            self.dust = {}
            self.standard_prices = {}
        self.ohlcv_field = ohlcv_field
        self.history_access = history_access
        self.potential_coin_client = potential_coin_client
        self.reference_coin = "BTC"
        self.candle = "1h"
        self.tolerance = 0.001
        self.trade_executed = 0
        self.live_orders = []
        self.banned_coins = {}
        self.order_operations = OrderOperations()
        self.holding_operations = HoldingOperations(self.reference_coin,
                                                    self.tolerance,
                                                    self.history_access,
                                                    self.candle,
                                                    self.dust,
                                                    self.standard_prices)
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
            order_instance=None
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

    def get_altcoins_numbers_to_buy(self,
                                    simulation_input_dict,
                                    holdings
                                    ):
        max_ref_coin_in_order = 1 / simulation_input_dict["max_coins_to_buy"]

        quantity_of_ref_avl = self.holding_operations.unlocked_coin_avl(holdings,
                                                                        self.reference_coin)
        altcoin_holdings_held = len(list(filter(lambda x: x.coin_name != self.reference_coin, holdings)))
        return math.ceil(quantity_of_ref_avl / max_ref_coin_in_order) - altcoin_holdings_held

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

        altcoins_number_to_buy = self.get_altcoins_numbers_to_buy(simulation_input_dict,
                                                                  holdings,
                                                                  )
        potential_valid_altcoin_not_held = self.get_potential_valid_altcoins_no_held(potential_coins,
                                                                                     instant_price_dict,
                                                                                     holdings)

        for _ in range(altcoins_number_to_buy):
            if not potential_valid_altcoin_not_held:
                return

            if self.holding_operations.unlocked_coin_avl(holdings,
                                                         self.reference_coin) > self.tolerance:
                buy_order = self._set_individual_buy_order(holdings,
                                                           simulation_input_dict,
                                                           potential_valid_altcoin_not_held,
                                                           instant_price_dict,
                                                           current_time,
                                                           order_type)
                self.holding_operations.lock_holding(holdings,
                                                     buy_order)
                self.live_orders.append(buy_order)

    def _set_individual_buy_order(self,
                                  holdings: Holdings,
                                  simulation_input_dict: Dict,
                                  potential_valid_altcoin_not_held: List,
                                  instant_price_dict: Dict,
                                  current_time: datetime.datetime,
                                  order_type: OrderType):
        random_coin = self._random_coin_to_buy(potential_valid_altcoin_not_held)
        ref_qty_available = self._ref_qty_available(holdings,
                                                    simulation_input_dict["max_coins_to_buy"])
        if order_type == OrderType.Limit:
            buy_price = self._get_limit_buy_price_coin(instant_price_dict,
                                                       random_coin,
                                                       simulation_input_dict["percentage_reduction"])
        elif order_type == OrderType.Market:
            buy_price = self._get_limit_buy_price_coin(instant_price_dict,
                                                       random_coin,
                                                       0)
        else:
            raise NotImplementedError

        return self._set_limit_buy_order(random_coin,
                                         buy_price,
                                         ref_qty_available,
                                         current_time)

    @staticmethod
    def _get_limit_buy_price_coin(
            instant_price_dict,
            coin_to_buy,
            percentage_reduction):
        altcoin_current_price = instant_price_dict[coin_to_buy]
        return altcoin_current_price * (1 - percentage_reduction)

    def _ref_qty_available(self,
                           holdings,
                           max_coins_to_buy):
        max_ref_coin_in_order = 1 / max_coins_to_buy

        ref_qty_available = self.holding_operations.get_coin_qty(holdings,
                                                                 self.reference_coin)
        if ref_qty_available > max_ref_coin_in_order:
            ref_qty_available = max_ref_coin_in_order
        return ref_qty_available

    @staticmethod
    def _random_coin_to_buy(coin_list):
        coin_to_buy = random.choice(coin_list)
        coin_list.remove(coin_to_buy)
        return coin_to_buy

    def _set_limit_buy_order(self,
                             random_coin,
                             buy_price,
                             ref_qty_available,
                             current_time
                             ):
        qty_of_altcoin_to_buy = ref_qty_available / buy_price

        return Order(order_side=OrderSide.Buy,
                     order_type=OrderType.Limit,
                     quantity=qty_of_altcoin_to_buy,
                     base_asset=random_coin,
                     reference_coin=self.reference_coin,
                     stop_price=-1,
                     timeout=current_time,
                     limit_price=buy_price,
                     complete=OrderFill.Fresh
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
        for order in self.live_orders:
            if order.complete != OrderFill.Filled:
                instance_price = instant_price_dict[f'{order.base_asset}']
                holdings = self.order_operations.execute_individual_order(holdings,
                                                                          order,
                                                                          instance_price,
                                                                          current_time)
        return holdings

    def remove_dead_orders(self,
                           holdings,
                           current_time):
        all_orders = self.live_orders.copy()
        self.live_orders = []
        for order in all_orders:
            if order.complete == OrderFill.Filled:
                continue

            if self.order_operations.has_order_reached_timeout(order,
                                                               current_time):
                self.holding_operations.unlock_holding(holdings,
                                                       order)
            else:
                self.live_orders.append(order)

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

        return order

    def sell_altcoin_overall(self,
                             holdings: Holdings,
                             current_time,
                             simulation_input_dict,
                             order_type):
        if self.holding_operations.if_altcoins_held(holdings):
            try:
                instance_price_dict = get_instantaneous_history(self.history_access,
                                                                current_time,
                                                                candle=self.candle
                                                                )
            except InsufficientHistory:
                logger.debug(f"History not present in {current_time}")
            else:
                for holding in holdings:
                    if holding.coin_name != self.reference_coin:
                        if holding.order_instance is None:
                            sell_order = self._set_sell_order_on_holding(holding,
                                                                         instance_price_dict[holding.coin_name],
                                                                         simulation_input_dict,
                                                                         current_time,
                                                                         order_type)

                            self.holding_operations.lock_holding(holdings,
                                                                 sell_order,
                                                                 )
                            self.live_orders.append(sell_order)


class HoldingOperations:
    def __init__(self, reference_coin, tolerance, history_access, candle, dust, standard_prices):
        self.reference_coin = reference_coin
        self.tolerance = tolerance
        self.history_access = history_access
        self.candle = candle
        self.dust = dust
        self.order_operations = OrderOperations()
        self.standard_prices = standard_prices

    def get_standard_price(self,
                           coin_name,
                           current_time):
        try:
            return self.standard_prices[coin_name]
        except KeyError:
            self.standard_prices = self.get_instant_price_dict(current_time)
            try:
                return self.standard_prices[coin_name]
            except KeyError:
                return 1

    def remove_insignificant_dust(self,
                                  holdings: Holdings,
                                  current_time):
        significant_holdings = []
        for holding in holdings:
            equivalent_value = holding.quantity * self.get_standard_price(holding.coin_name,
                                                                          current_time)
            if equivalent_value > self.tolerance:
                significant_holdings.append(holding)
            else:
                if holding.coin_name in self.dust.keys():
                    self.dust[holding.coin_name] += holding.quantity
                else:
                    self.dust[holding.coin_name] = holding.quantity
        return significant_holdings

    def lock_holding(self,
                     holdings: Holdings,
                     order):
        if order.order_side == OrderSide.Buy:
            replace = [self.reference_coin, order.quantity * order.limit_price]
        else:
            replace = [order.base_asset, order.quantity]

        for holding_index in range(len(holdings)):
            holding = holdings[holding_index]
            if (holding.coin_name == replace[0]) and \
                    ((holding.quantity + self.tolerance) > replace[1]) and \
                    (holding.order_instance is None):
                holding.quantity = holding.quantity - replace[1]
                added_holding = self.order_operations.add_item_to_holdings(holdings,
                                                                           replace,
                                                                           order_instance=order)
                return
        raise InsufficientBalance(f"Could not lock {order} as the holdings are only {holdings}")

    def unlock_holding(self,
                       holdings: Holdings,
                       order):
        for holding_index in range(len(holdings)):
            holding = holdings[holding_index]
            if holding.order_instance == order:
                holding.order_instance = None
                holdings.remove(holding)
                if order.order_side == OrderSide.Buy:
                    replace = [self.reference_coin, order.quantity * order.limit_price]
                else:
                    replace = [order.base_asset, order.quantity]
                self.order_operations.add_item_to_holdings(holdings,
                                                           replace,
                                                           order_instance=None)
                return
        raise InsufficientBalance(f"Could not unlock {order} as the holdings are only {holdings}")

    @staticmethod
    def unlocked_coin_avl(holdings: Holdings,
                          coin_name: str):
        for holding in holdings:
            if holding.coin_name == coin_name:
                if holding.order_instance is None:
                    return holding.quantity
        return 0

    def if_altcoins_held(self,
                         holdings: Holdings):
        return not ((len(holdings) == 1) and (holdings[0].coin_name == self.reference_coin))

    def should_buy_altcoin(self,
                           holdings: Holdings,
                           ) -> bool:
        for holding in holdings:
            if holding.coin_name == self.reference_coin:
                if holding.order_instance is None:
                    if holding.quantity > self.tolerance:
                        return True
        return False

    @staticmethod
    def get_coin_qty(holdings: Holdings,
                     coin_name: str):
        for holding in holdings:
            if holding.coin_name == coin_name:
                return holding.quantity
        raise InsufficientBalance("Coin not in the list")

    def log_holding_value(self,
                          holdings,
                          simulation_time,
                          simulation_input_dict):
        try:
            if (simulation_time.day % 5) == 0 and (simulation_time.hour == 1):
                logger.debug(f"Holdings are worth"
                             f" {self.get_total_holding_worth(holdings, simulation_time)} "
                             f"at {simulation_time} ------- {holdings} --- {simulation_input_dict}")
        except InsufficientHistory as e:
            pass

    def get_instant_price_dict(self,
                               current_time):
        try:
            instance_price_dict = get_instantaneous_history(self.history_access,
                                                            current_time,
                                                            candle=self.candle
                                                            )
            instance_price_dict[self.reference_coin] = 1
        except InsufficientHistory:
            return {}
        return instance_price_dict

    def get_total_holding_worth(self,
                                holdings,
                                current_time):
        instant_price_dict = self.get_instant_price_dict(current_time)
        try:
            return functools.reduce(
                lambda x, y: x + y.quantity * instant_price_dict[y.coin_name],
                holdings, 0)
        except KeyError as e:
            return 0


class OrderOperations:
    def execute_individual_order(self,
                                 holdings,
                                 order: Order,
                                 current_price: float,
                                 current_time: datetime.datetime):
        add, remove = self._get_add_remove_holdings(order,
                                                    current_price,
                                                    current_time)
        try:
            if (add is not None) and (remove is not None):
                self.remove_item_from_holdings(holdings,
                                               order)
                self.add_item_to_holdings(holdings,
                                          add,
                                          order_instance=None
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
                remove = (order.reference_coin, order.quantity * current_price)
                add = (order.base_asset, order.quantity)
            elif order.order_side == OrderSide.Sell:
                add = (order.reference_coin, order.quantity * current_price)
                remove = (order.base_asset, order.quantity)
        elif order.order_type == OrderType.Limit:
            if order.order_side == OrderSide.Buy:
                if order.limit_price >= current_price:
                    remove = (order.reference_coin, order.quantity * current_price)
                    add = (order.base_asset, order.quantity)
            elif order.order_side == OrderSide.Sell:
                if order.limit_price <= current_price:
                    add = (order.reference_coin, order.quantity * current_price)
                    remove = (order.base_asset, order.quantity)
        return add, remove

    @staticmethod
    def add_item_to_holdings(holdings: Holdings,
                             add,
                             order_instance=None
                             ):
        if order_instance is None:
            for holding in holdings:
                if holding.coin_name == add[0]:
                    if holding.order_instance is None:
                        holding.quantity = holding.quantity + add[1]
                        return holding
        added_holding = HoldingCoin(coin_name=add[0],
                                    quantity=add[1],
                                    order_instance=order_instance)
        holdings.append(added_holding)
        return added_holding

    @staticmethod
    def remove_item_from_holdings(holdings: Holdings,
                                  order_instance):
        for holding_index in range(len(holdings)):
            if holdings[holding_index].order_instance == order_instance:
                holdings.pop(holding_index)
                return
        raise InsufficientBalance(f"Did not have {order_instance} to remove from holdings {holdings}")


class MarketBuyLimitSellSimulatorConcrete(AbstractTimestepSimulatorConcrete):
    def manage_simulation_per_timestep(self,
                                       holdings: List,
                                       simulation_start: datetime.datetime,
                                       simulation_at: datetime.datetime,
                                       simulation_input_dict: Dict) -> List:
        self.holding_operations.log_holding_value(holdings,
                                                  simulation_at,
                                                  simulation_input_dict)
        self.buy_altcoin_overall(holdings,
                                 simulation_input_dict,
                                 simulation_start,
                                 simulation_at,
                                 order_type=OrderType.Market
                                 )

        holdings = self.try_execute_open_orders(holdings,
                                                simulation_at)
        self.sell_altcoin_overall(holdings,
                                  simulation_at,
                                  simulation_input_dict,
                                  order_type=OrderType.Limit)

        holdings = self.try_execute_open_orders(holdings,
                                                simulation_at)

        holdings = self.holding_operations.remove_insignificant_dust(holdings,
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
                                                  simulation_at,
                                                  simulation_input_dict)

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

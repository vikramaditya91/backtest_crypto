import datetime
from enum import Enum
from dataclasses import dataclass


class Singleton (type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OrderSide(Enum):
    Buy = 0
    Sell = 1


class OrderType(Enum):
    Market = 0
    Limit = 1


@dataclass
class HoldingCoin:
    coin_name: str
    quantity: float
    locked: bool


class OrderFill(Enum):
    Fresh = 0
    Partial = 1
    Filled = 2


@dataclass
class Order:
    order_side: OrderSide
    order_type: OrderType
    base_asset: str
    reference_coin: str
    quantity: float
    limit_price: float
    stop_price: float
    timeout: datetime.datetime
    complete: OrderFill


class InsufficientHistory(ValueError):
    pass


class MissingPotentialCoinTimeIndexError(KeyError):
    pass


class InsufficientBalance(ValueError):
    pass


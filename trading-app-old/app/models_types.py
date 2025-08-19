from typing import TypedDict
from typing_extensions import NotRequired
from enum import Enum
import datetime


class Status(str, Enum):
    active = "active"
    stopping = "stopping"
    inactive = "inactive"


class OptionType(str, Enum):
    C = "C"
    P = "P"


class NotificationDictPrimaryKeys(TypedDict):
    title: str


class NotificationDict(NotificationDictPrimaryKeys):
    body: str
    alert_type: str


class NotificationDictUpdateKeys(NotificationDictPrimaryKeys):
    body: NotRequired[str]
    alert_type: NotRequired[str]


class StrategyDictPrimaryKeys(TypedDict):
    strategy: str


class StrategyDict(StrategyDictPrimaryKeys):
    capital: float
    initial_capital: float
    status: Status


class StrategyDictUpdateKeys(StrategyDictPrimaryKeys):
    capital: NotRequired[float]
    initial_capital: NotRequired[float]
    status: NotRequired[Status]


class CurrentStockPositionsDictPrimaryKeys(TypedDict):
    stock: str
    strategy: str


class CurrentStockPositionsDict(CurrentStockPositionsDictPrimaryKeys):
    avg_price: float
    quantity: float
    stop_limit: float


class CurrentStockPositionsDictUpdateKeys(CurrentStockPositionsDictPrimaryKeys):
    avg_price: NotRequired[float]
    quantity: NotRequired[float]
    stop_limit: NotRequired[float]


class CurrentOptionPositionsDictPrimaryKeys(TypedDict):
    stock: str
    strategy: str
    expiry: str
    strike: float
    multiplier: float
    option_type: OptionType


class CurrentOptionPositionsDict(CurrentOptionPositionsDictPrimaryKeys):
    avg_price: float
    quantity: float


class CurrentOptionPositionsDictUpdateKeys(CurrentOptionPositionsDictPrimaryKeys):
    avg_price: NotRequired[float]
    quantity: NotRequired[float]


class TargetStockPositionsDictPrimaryKeys(TypedDict):
    stock: str
    strategy: str


class TargetStockPositionsDict(TargetStockPositionsDictPrimaryKeys):
    stop_limit: float
    avg_price: float
    quantity: float


class TargetStockPositionsDictUpdateKeys(TargetStockPositionsDictPrimaryKeys):
    stop_limit: NotRequired[float]
    avg_price: NotRequired[float]
    quantity: NotRequired[float]


class TargetOptionPositionsDictPrimaryKeys(TypedDict):
    stock: str
    strategy: str
    expiry: str
    strike: float
    multiplier: float
    option_type: OptionType


class TargetOptionPositionsDict(TargetOptionPositionsDictPrimaryKeys):
    avg_price: float
    quantity: float


class TargetOptionPositionsDictUpdateKeys(TargetOptionPositionsDictPrimaryKeys):
    avg_price: NotRequired[float]
    quantity: NotRequired[float]


class OpenStockOrdersDictPrimaryKeys(TypedDict):
    order_id: int
    stock: str
    strategy: str
    time: datetime.datetime


class OpenStockOrdersDict(OpenStockOrdersDictPrimaryKeys):
    quantity: float


class OpenStockOrdersDictUpdateKeys(OpenStockOrdersDictPrimaryKeys):
    quantity: NotRequired[float]


class OpenOptionOrdersDictPrimaryKeys(TypedDict):
    order_id: int
    stock: str
    strategy: str
    expiry: str
    strike: float
    option_type: OptionType
    multiplier: float
    time: datetime.datetime


class OpenOptionOrdersDict(OpenOptionOrdersDictPrimaryKeys):
    quantity: float


class OpenOptionOrdersDictUpdateKeys(OpenOptionOrdersDictPrimaryKeys):
    quantity: NotRequired[float]


class StockTransactionsDictPrimaryKeys(TypedDict):
    stock: str
    strategy: str
    time: datetime.datetime


class StockTransactionsDict(StockTransactionsDictPrimaryKeys):
    price_transacted: float
    fees: float
    quantity: float


class StockTransactionsDictUpdateKeys(StockTransactionsDictPrimaryKeys):
    price_transacted: NotRequired[float]
    fees: NotRequired[float]
    quantity: NotRequired[float]


class OptionTransactionsDictPrimaryKeys(TypedDict):
    stock: str
    strategy: str
    expiry: str
    strike: float
    multiplier: float
    option_type: OptionType
    time: datetime.datetime


class OptionTransactionsDict(OptionTransactionsDictPrimaryKeys):
    price_transacted: float
    fees: float
    quantity: float


class OptionTransactionsDictUpdateKeys(OptionTransactionsDictPrimaryKeys):
    price_transacted: NotRequired[float]
    fees: NotRequired[float]
    quantity: NotRequired[float]


class HistoricalDataDictPrimaryKeys(TypedDict):
    stock: str
    time: datetime.datetime


class HistoricalDataDict(HistoricalDataDictPrimaryKeys):
    open: float
    high: float
    low: float
    close: float
    volume: int


class HistoricalDataDictUpdateKeys(HistoricalDataDictPrimaryKeys):
    open: NotRequired[float]
    high: NotRequired[float]
    low: NotRequired[float]
    close: NotRequired[float]
    volume: NotRequired[int]


class HistoricalVolatilityDataDictPrimaryKeys(TypedDict):
    stock: str
    time: datetime.datetime


class HistoricalVolatilityDataDict(HistoricalVolatilityDataDictPrimaryKeys):
    open: float
    high: float
    low: float
    close: float


class HistoricalVolatilityDataDictUpdateKeys(HistoricalVolatilityDataDictPrimaryKeys):
    open: NotRequired[float]
    high: NotRequired[float]
    low: NotRequired[float]
    close: NotRequired[float]


class HistoricalOptionsDataDictPrimaryKeys(TypedDict):
    stock: str
    expiry: str
    strike: float
    multiplier: float
    option_type: OptionType
    time: datetime.datetime


class HistoricalOptionsDataDict(HistoricalOptionsDataDictPrimaryKeys):
    open: float
    high: float
    low: float
    close: float
    volume: float


class HistoricalOptionsDataDictUpdateKeys(HistoricalOptionsDataDictPrimaryKeys):
    open: NotRequired[float]
    high: NotRequired[float]
    low: NotRequired[float]
    close: NotRequired[float]
    volume: NotRequired[float]


class PhantomPortfolioValueDictPrimaryKeys(TypedDict):
    time: datetime.datetime


class PhantomPortfolioValueDict(PhantomPortfolioValueDictPrimaryKeys):
    cash_portfolio_value: float
    option_portfolio_value: float
    bought_price: float
    strike: float
    peak: float
    paused: bool
    resume_trades: int


class PhantomPortfolioValueDictUpdateKeys(PhantomPortfolioValueDictPrimaryKeys):
    cash_portfolio_value: NotRequired[float]
    option_portfolio_value: NotRequired[float]
    bought_price: NotRequired[float]
    strike: NotRequired[float]
    peak: NotRequired[float]
    paused: NotRequired[bool]
    resume_trades: NotRequired[int]

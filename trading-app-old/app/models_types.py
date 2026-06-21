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


class HistoricalThresholdRebalancingDictPrimaryKeys(TypedDict):
    time: datetime.datetime


class HistoricalThresholdRebalancingDict(HistoricalThresholdRebalancingDictPrimaryKeys):
    threshold_equity_prop_000: float
    threshold_equity_prop_001: float
    threshold_equity_prop_002: float
    threshold_equity_prop_003: float
    threshold_equity_prop_004: float
    threshold_equity_prop_005: float
    threshold_equity_prop_006: float
    threshold_equity_prop_007: float
    threshold_equity_prop_008: float
    threshold_equity_prop_009: float
    threshold_equity_prop_010: float
    threshold_equity_prop_011: float
    threshold_equity_prop_012: float
    threshold_equity_prop_013: float
    threshold_equity_prop_014: float
    threshold_equity_prop_015: float
    threshold_equity_prop_016: float
    threshold_equity_prop_017: float
    threshold_equity_prop_018: float
    threshold_equity_prop_019: float
    threshold_equity_prop_020: float
    threshold_equity_prop_021: float
    threshold_equity_prop_022: float
    threshold_equity_prop_023: float
    threshold_equity_prop_024: float
    threshold_equity_prop_025: float


class HistoricalThresholdRebalancingDictUpdateKeys(HistoricalThresholdRebalancingDictPrimaryKeys):
    threshold_equity_prop_000: NotRequired[float]
    threshold_equity_prop_001: NotRequired[float]
    threshold_equity_prop_002: NotRequired[float]
    threshold_equity_prop_003: NotRequired[float]
    threshold_equity_prop_004: NotRequired[float]
    threshold_equity_prop_005: NotRequired[float]
    threshold_equity_prop_006: NotRequired[float]
    threshold_equity_prop_007: NotRequired[float]
    threshold_equity_prop_008: NotRequired[float]
    threshold_equity_prop_009: NotRequired[float]
    threshold_equity_prop_010: NotRequired[float]
    threshold_equity_prop_011: NotRequired[float]
    threshold_equity_prop_012: NotRequired[float]
    threshold_equity_prop_013: NotRequired[float]
    threshold_equity_prop_014: NotRequired[float]
    threshold_equity_prop_015: NotRequired[float]
    threshold_equity_prop_016: NotRequired[float]
    threshold_equity_prop_017: NotRequired[float]
    threshold_equity_prop_018: NotRequired[float]
    threshold_equity_prop_019: NotRequired[float]
    threshold_equity_prop_020: NotRequired[float]
    threshold_equity_prop_021: NotRequired[float]
    threshold_equity_prop_022: NotRequired[float]
    threshold_equity_prop_023: NotRequired[float]
    threshold_equity_prop_024: NotRequired[float]
    threshold_equity_prop_025: NotRequired[float]


class HistoricalCalendarRebalancingDictPrimaryKeys(TypedDict):
    time: datetime.datetime


class HistoricalCalendarRebalancingDict(HistoricalCalendarRebalancingDictPrimaryKeys):
    calendar_equity_prop: float


class HistoricalCalendarRebalancingDictUpdateKeys(HistoricalCalendarRebalancingDictPrimaryKeys):
    calendar_equity_prop: NotRequired[float]


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

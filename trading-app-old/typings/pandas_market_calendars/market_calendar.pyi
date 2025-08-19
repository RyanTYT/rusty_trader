import pandas as pd
from . import calendar_utils as u
from .class_registry import ProtectedDict as ProtectedDict, RegisteryMeta as RegisteryMeta  # type: ignore
from _typeshed import Incomplete
from abc import ABCMeta, abstractmethod
from typing import Literal

MONDAY: Incomplete
TUESDAY: Incomplete
WEDNESDAY: Incomplete
THURSDAY: Incomplete
FRIDAY: Incomplete
SATURDAY: Incomplete
SUNDAY: Incomplete
WEEKMASK_ABBR: Incomplete


class DEFAULT:
    ...


class MarketCalendarMeta(ABCMeta, RegisteryMeta):  # type: ignore
    ...


class MarketCalendar(metaclass=MarketCalendarMeta):
    regular_market_times: Incomplete
    open_close_map: Incomplete
    @classmethod
    def valid_days(self, start_date: str, end_date: str, tz: str = 'UTC') -> pd.DatetimeIndex: ...

import pandas as pd
from _typeshed import Incomplete
from pandas.tseries.holiday import AbstractHolidayCalendar as AbstractHolidayCalendar, Holiday as Holiday
from pandas.tseries.offsets import CustomBusinessDay as CustomBusinessDay
from typing import Any, Iterable, Literal

DEFAULT_LABEL_MAP: Incomplete

SESSIONS: Incomplete
MKT_TIMES: Incomplete

class DateRangeWarning(UserWarning): ...
class OverlappingSessionWarning(DateRangeWarning): ...
class DisappearingSessionWarning(DateRangeWarning): ...
class MissingSessionWarning(DateRangeWarning): ...
class InsufficientScheduleWarning(DateRangeWarning): ...

def parse_missing_session_warning(err: MissingSessionWarning) -> tuple[set[SESSIONS], set[MKT_TIMES]]: ...
def parse_insufficient_schedule_warning(err: InsufficientScheduleWarning) -> tuple[bool, pd.Timestamp, pd.Timestamp]: ...
def date_range(schedule: pd.DataFrame, frequency: str | pd.Timedelta | int | float, closed: Literal['left', 'right', 'both'] | None = 'right', force_close: bool | None = True, session: SESSIONS | Iterable[SESSIONS] = {'RTH'}, merge_adjacent: bool = True, start: str | pd.Timestamp | int | float | None = None, end: str | pd.Timestamp | int | float | None = None, periods: int | None = None) -> pd.DatetimeIndex: ...

PeriodCode: Incomplete
Day_Anchor: Incomplete
Month_Anchor: Incomplete
days_rolled: Incomplete
weekly_roll_map: Incomplete
months_rolled: Incomplete
yearly_roll_map: Incomplete

def date_range_htf(cal: CustomBusinessDay, frequency: str | pd.Timedelta | int | float, start: str | pd.Timestamp | int | float | None = None, end: str | pd.Timestamp | int | float | None = None, periods: int | None = None, closed: Literal['left', 'right'] | None = 'right', *, day_anchor: Day_Anchor = 'SUN', month_anchor: Month_Anchor = 'JAN') -> pd.DatetimeIndex: ...

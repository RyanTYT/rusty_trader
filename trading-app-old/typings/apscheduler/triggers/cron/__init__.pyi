from _typeshed import Incomplete
from apscheduler.triggers.base import BaseTrigger as BaseTrigger
from apscheduler.triggers.cron.fields import BaseField as BaseField, DEFAULT_VALUES as DEFAULT_VALUES, DayOfMonthField as DayOfMonthField, DayOfWeekField as DayOfWeekField, MonthField as MonthField, WeekField as WeekField  # type: ignore
from apscheduler.util import astimezone as astimezone, convert_to_datetime as convert_to_datetime, datetime_ceil as datetime_ceil, datetime_repr as datetime_repr  # type: ignore


class CronTrigger(BaseTrigger):
    FIELD_NAMES: Incomplete
    FIELDS_MAP: Incomplete
    timezone: Incomplete
    start_date: Incomplete
    end_date: Incomplete
    jitter: Incomplete
    fields: Incomplete
    def __init__(self, year: Incomplete | None = None, month: Incomplete | None = None, day: Incomplete | None = None, week: Incomplete | None = None, day_of_week: Incomplete | None = None, hour: Incomplete | None = None, minute: Incomplete | None = None, second: Incomplete | None = None, start_date: Incomplete | None = None, end_date: Incomplete | None = None, timezone: Incomplete | None = None, jitter: Incomplete | None = None) -> None: ...
    @classmethod
    def from_crontab(cls, expr, timezone: Incomplete | None = None) -> None: ...  # type: ignore
    def get_next_fire_time(self, previous_fire_time, now): ...  # type: ignore

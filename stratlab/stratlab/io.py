"""IO + wide-grid layer for stratlab.

Loads the ``raw_bars`` parquet emitted by the Rust ``StatsLogger``
(``trading/trading-app/src/strategy/stats_logger.rs``)::

    raw_bars: ts_epoch_ms (i64, UTC ms) | asset (str)
              | open | high | low | close | volume (all f64)
              5-min bars, 78/day, 9:30-16:00 ET

into a long DataFrame and a ``Universe`` of cached *wide* grids indexed by
``(et_date, minutes_since_open)`` with one column per asset. ``Universe.resample(k)``
returns a coarser ``Universe`` (k source bars per aggregated bar) so the whole
indicator / engine stack is timeframe-agnostic — the same functions run on 5m,
15m, 60m or daily by swapping the source.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime, timezone
from enum import Enum
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ET_TZ = "America/New_York"
_ET = ZoneInfo(ET_TZ)

NUM_BARS_PER_DAY: int = 78
BAR_MINUTES: int = 5
MARKET_OPEN_MIN: int = 570  # 9:30 ET in minutes-from-midnight
DAILY_PPY: float = 252.0
INTRADAY_PPY: float = DAILY_PPY * NUM_BARS_PER_DAY  # 19656

RAW_BAR_COLS = ["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"]

DEFAULT_RAW_BARS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "data", "backtest-results", "stats", "raw_bars.parquet",
)


# ────────────────────────────────────────────────────────────────────────────
# Bar — the atomic unit (the Python twin of a raw_bars row / Rust AggBar)
# ────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Bar:
    """One OHLCV bar. `ts_epoch_ms` is the source bar's timestamp in UTC ms."""

    ts_epoch_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def ts_ms_to_et(ms: int) -> datetime:
    """UTC ms → aware datetime in America/New_York (handles DST)."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(_ET)


def ts_ms_to_et_date(ms: int) -> _date:
    return ts_ms_to_et(ms).date()


def ts_ms_to_mso(ms: int) -> int:
    """minutes_since_open (0..385) for a 5-min bar. Mirrors stats_logger:
    ``(sec_from_midnight // 60) - 570``."""
    t = ts_ms_to_et(ms)
    return t.hour * 60 + t.minute - MARKET_OPEN_MIN


class CalendarUnit(str, Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


@dataclass(frozen=True)
class Timeframe:
    """Aggregation granularity, driven by CALENDAR boundaries — not a bar count.

    Build via the factories ``Timeframe.minutes(15)`` / ``.hourly()`` /
    ``.daily()`` / ``.weekly()`` / ``.monthly()`` / ``.yearly()``, or parse a
    string with ``Timeframe.parse("15m" | "1h" | "1d" | "1w" | "1mo" | "1y")``.

    ``bucket(ts_epoch_ms)`` returns a canonical, order-preserving tuple key so
    two bars land in the same bucket iff they share a calendar boundary. For
    ``Minutes(n)`` the key floors the minute-of-day: a 9:40 bar is in bucket
    ``570 // 15`` … a 9:45 bar is in bucket ``585 // 15`` (one later).
    """

    unit: CalendarUnit
    n: int = 1  # multiplier for MINUTE (e.g. 15 → 15-minute buckets)

    @classmethod
    def minutes(cls, n: int) -> Timeframe:
        if n <= 0:
            raise ValueError("Timeframe.minutes(n) requires n > 0")
        return cls(CalendarUnit.MINUTE, int(n))

    @classmethod
    def hourly(cls) -> Timeframe:
        return cls(CalendarUnit.HOUR, 1)

    @classmethod
    def daily(cls) -> Timeframe:
        return cls(CalendarUnit.DAY, 1)

    @classmethod
    def weekly(cls) -> Timeframe:
        return cls(CalendarUnit.WEEK, 1)

    @classmethod
    def monthly(cls) -> Timeframe:
        return cls(CalendarUnit.MONTH, 1)

    @classmethod
    def yearly(cls) -> Timeframe:
        return cls(CalendarUnit.YEAR, 1)

    @classmethod
    def parse(cls, s: str) -> Timeframe:
        s = s.strip().lower()
        if s.endswith("mo"):
            return cls.monthly()
        if s.endswith("min"):
            return cls.minutes(int(s[:-3] or "1"))
        if s.endswith("m"):
            return cls.minutes(int(s[:-1] or "1"))
        if s.endswith("d"):
            return cls.daily()
        if s.endswith("w"):
            return cls.weekly()
        if s.endswith("y"):
            return cls.yearly()
        raise ValueError(f"unparseable timeframe: {s!r}")

    def bucket(self, ms: int) -> tuple:
        """Order-preserving bucket key for a UTC-ms timestamp (ET-local)."""
        et = ts_ms_to_et(ms)
        y, mo, d = et.year, et.month, et.day
        u = self.unit
        if u is CalendarUnit.MINUTE:
            return (y, mo, d, (et.hour * 60 + et.minute) // self.n)
        if u is CalendarUnit.HOUR:
            return (y, mo, d, (et.hour * 60 + et.minute) // 60)
        if u is CalendarUnit.DAY:
            return (y, mo, d)
        if u is CalendarUnit.WEEK:
            iso = et.isocalendar()  # (iso_year, iso_week, iso_weekday)
            return (iso[0], iso[1])
        if u is CalendarUnit.MONTH:
            return (y, mo)
        if u is CalendarUnit.YEAR:
            return (y,)
        raise ValueError(f"unknown calendar unit {u!r}")

    def __str__(self) -> str:
        names = {
            CalendarUnit.MINUTE: f"{self.n}m",
            CalendarUnit.HOUR: "1h",
            CalendarUnit.DAY: "1d",
            CalendarUnit.WEEK: "1w",
            CalendarUnit.MONTH: "1mo",
            CalendarUnit.YEAR: "1y",
        }
        return names[self.unit]


def _attach_et(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["ts_epoch_ms"], unit="ms", utc=True)
    et = ts.dt.tz_convert(ET_TZ)
    out = df.copy()
    out["et_ts"] = et
    out["et_date"] = et.dt.date
    out["minutes_since_open"] = (et.dt.hour * 60 + et.dt.minute) - MARKET_OPEN_MIN
    return out


# ────────────────────────────────────────────────────────────────────────────
# Loader
# ────────────────────────────────────────────────────────────────────────────
def load_raw_bars(
    path: str | os.PathLike,
    *,
    assets: Iterable[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Load a ``raw_bars`` parquet into a long DataFrame (sorted by asset, time).

    Only the 7 raw columns are read; the engineered per-bar features are not.
    `start`/`end` are ET date bounds ("YYYY-MM-DD"), inclusive.
    """
    path = os.fspath(path)
    try:
        df = pd.read_parquet(path, columns=[c for c in RAW_BAR_COLS])
    except Exception:
        df = pd.read_parquet(path)
        df = df[[c for c in RAW_BAR_COLS if c in df.columns]]

    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts_epoch_ms"] = pd.to_numeric(df["ts_epoch_ms"], errors="coerce").astype("int64")
    df = df.dropna(subset=["ts_epoch_ms", "asset", "close"]).reset_index(drop=True)

    if assets is not None:
        want = set(assets)
        df = df[df["asset"].isin(want)]

    df = _attach_et(df)

    if start is not None:
        df = df[df["et_date"] >= pd.Timestamp(start).date()]
    if end is not None:
        df = df[df["et_date"] <= pd.Timestamp(end).date()]

    return df.sort_values(["asset", "ts_epoch_ms"]).reset_index(drop=True)


def load_default(**kw) -> pd.DataFrame:
    return load_raw_bars(os.path.abspath(DEFAULT_RAW_BARS_PATH), **kw)


# ────────────────────────────────────────────────────────────────────────────
# Wide grids
# ────────────────────────────────────────────────────────────────────────────
def _pivot_wide(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    g = df[["et_date", "minutes_since_open", "asset", value_col]].drop_duplicates(
        subset=["et_date", "minutes_since_open", "asset"]
    )
    wide = g.pivot_table(
        index=["et_date", "minutes_since_open"], columns="asset",
        values=value_col, sort=True,
    )
    wide = wide.sort_index()
    wide.columns.name = None
    return wide


@dataclass
class Universe:
    """Cached panel view of a ``raw_bars`` long frame.

    Wide grids are indexed by ``(et_date, minutes_since_open)`` with one column
    per asset; access via the ``.grid_*`` properties (lazy + cached).
    """

    long: pd.DataFrame
    assets: list[str] = field(default_factory=list)
    dates: list = field(default_factory=list)
    n_bars_per_day: int = NUM_BARS_PER_DAY
    _cache: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_long(cls, df: pd.DataFrame) -> Universe:
        df = df.sort_values(["asset", "ts_epoch_ms"]).reset_index(drop=True)
        assets = sorted(pd.unique(df["asset"]).tolist())
        dates = sorted(pd.unique(df["et_date"]).tolist())
        return cls(long=df, assets=assets, dates=dates, n_bars_per_day=NUM_BARS_PER_DAY)

    @classmethod
    def from_parquet(cls, path: str | os.PathLike, **kw) -> Universe:
        return cls.from_long(load_raw_bars(path, **kw))

    # ── metadata ──
    @property
    def ppy_intraday(self) -> float:
        return DAILY_PPY * self.n_bars_per_day

    @property
    def ppy_daily(self) -> float:
        return DAILY_PPY

    @property
    def n_days(self) -> int:
        return len(self.dates)

    # ── wide-grid accessors ──
    def _wide(self, key: str, value_col: str) -> pd.DataFrame:
        if key not in self._cache:
            self._cache[key] = _pivot_wide(self.long, value_col)
        return self._cache[key]

    @property
    def grid_close(self) -> pd.DataFrame:
        return self._wide("close", "close")

    @property
    def grid_open(self) -> pd.DataFrame:
        return self._wide("open", "open")

    @property
    def grid_high(self) -> pd.DataFrame:
        return self._wide("high", "high")

    @property
    def grid_low(self) -> pd.DataFrame:
        return self._wide("low", "low")

    @property
    def grid_volume(self) -> pd.DataFrame:
        return self._wide("volume", "volume")

    @property
    def grid_dollar_vol(self) -> pd.DataFrame:
        if "dollar_vol" not in self._cache:
            self._cache["dollar_vol"] = self.grid_close * self.grid_volume
        return self._cache["dollar_vol"]

    @property
    def grid_logret(self) -> pd.DataFrame:
        """Bar-to-bar log returns, wide, INTRA-day only.

        The first bar of each session is NaN (no overnight crossing). NOTE: the
        Rust ``stats_logger`` per_bar ``log_ret`` instead carries the overnight
        return on the first bar of a day (its ``last_close`` persists across
        days); this intraday version is the no-look-ahead research convention.
        """
        if "logret" not in self._cache:
            self._cache["logret"] = np.log(self.grid_close).groupby(level=0).diff()
        return self._cache["logret"]

    # ── aggregation: fine bars → coarse calendar-boundary bars ──
    def resample(self, timeframe: Timeframe) -> Universe:
        """Aggregate to a coarser calendar-boundary grid -> a new Universe.

        `timeframe` is a `Timeframe` (e.g. `Timeframe.minutes(15)`, `.daily()`);
        buckets are calendar boundaries, emitted when the next bucket begins.
        """
        from stratlab.primitives._vec import aggregate_bars

        coarse = aggregate_bars(self.long, timeframe)
        return Universe.from_long(coarse)

    # ── per-asset long slice ──
    def asset_long(self, asset: str) -> pd.DataFrame:
        return self.long[self.long["asset"] == asset].sort_values("ts_epoch_ms").reset_index(drop=True)

    # ── chronological H1/H2 split ──
    def split_dates(self, frac: float = 0.5) -> tuple[list, list]:
        d = list(self.dates)
        k = int(round(len(d) * frac))
        return d[:k], d[k:]

    def grid_view(self, grid: pd.DataFrame, dates: Sequence) -> pd.DataFrame:
        return grid.loc[pd.Index(dates)].dropna(how="all")

    def summary(self) -> str:
        return (
            f"Universe(assets={len(self.assets)}, days={self.n_days}, "
            f"bars/day={self.n_bars_per_day}, bars_total={len(self.long)}, "
            f"date_range={self.dates[0]}..{self.dates[-1] if self.dates else '?'})"
        )

    def __repr__(self) -> str:
        return f"Universe({len(self.assets)} assets, {self.n_days} days)"
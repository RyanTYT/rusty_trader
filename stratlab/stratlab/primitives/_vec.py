"""Vectorized (fast) twins of the Rust rolling helpers — the research path.

Every function is strictly causal (trailing window, no centered window, no
future peek) and returns ``NaN`` until the window is full, matching Rust
``rolling_fn.rs`` (which returns ``None``).  They operate on a ``Series``/wide
``DataFrame`` (indexed ``(et_date, minute_of_day)`` for session-aware helpers)
and are the efficient counterpart to the streaming classes in ``reference.py``.

Key parity choices (mirror Rust exactly):
* ``rolling_std`` / ``rolling_z`` use SAMPLE std (ddof=1, divisor n-1).
* ``ema`` is adjust=False EWMA with alpha = 2/(span+1) — emits from bar 0.
* ``rolling_rank_pct`` = count(values <= last) / window (ties count fully).
* ``session_vwap`` is CLOSE-weighted (close*volume / volume), resets per day.
* ``fracdiff`` uses the standard (1-B)^d weight kernel.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from stratlab.io import Timeframe, _attach_et


# ────────────────────────────────────────────────────────────────────────────
# Simple trailing-window primitives (exact pandas == Rust incremental)
# ────────────────────────────────────────────────────────────────────────────
def rolling_sum(x: pd.Series | pd.DataFrame, w: int):
    return x.rolling(w, min_periods=w).sum()


def rolling_mean(x: pd.Series | pd.DataFrame, w: int):
    return x.rolling(w, min_periods=w).mean()


def rolling_std(x: pd.Series | pd.DataFrame, w: int):
    """Sample std (ddof=1) — matches Rust `RollingStd`."""
    return x.rolling(w, min_periods=w).std(ddof=1)


def rolling_max(x: pd.Series | pd.DataFrame, w: int):
    return x.rolling(w, min_periods=w).max()


def rolling_min(x: pd.Series | pd.DataFrame, w: int):
    return x.rolling(w, min_periods=w).min()


def rolling_z(x: pd.Series | pd.DataFrame, w: int):
    """(x - rolling_mean) / rolling_std, sample std. The causal primitive."""
    m = rolling_mean(x, w)
    s = rolling_std(x, w)
    return (x - m) / s


def rolling_rank_pct(x: pd.Series | pd.DataFrame, w: int):
    """Exact percentile = #(values <= current) / w (ties count fully). Matches
    Rust `RollingRankPct`; pandas `.rank(pct=True)` uses average ties, so this
    is done explicitly. O(w) per window — the slow reference; use sparingly or
    extend with an efficient algorithm if it becomes hot."""
    return x.rolling(w, min_periods=w).apply(lambda a: (a <= a[-1]).sum() / len(a), raw=True)


def ema(x: pd.Series | pd.DataFrame, span: int):
    """adjust=False EWMA, alpha = 2/(span+1). Emits from the first bar (Rust
    `EwmMean` returns immediately)."""
    return x.ewm(span=span, adjust=False, min_periods=1).mean()


def rolling_roc(x: pd.Series | pd.DataFrame, diff: int):
    """Rate-of-change over `diff` bars: (x[t] - x[t-diff]) / x[t-diff]."""
    return x.pct_change(diff)


def session_vwap(close: pd.Series | pd.DataFrame, volume: pd.Series | pd.DataFrame):
    """Cumulative CLOSE-weighted session VWAP, resets each day. `close`/`volume`
    must be indexed with the date at level 0 (the wide-grid convention)."""
    pv = close * volume
    return pv.groupby(level=0).cumsum() / volume.groupby(level=0).cumsum()


def rolling_beta(y: pd.Series | pd.DataFrame, x: pd.Series | pd.DataFrame, w: int):
    """Rolling OLS beta of y on x. Degenerate x-variance -> 0.0 (Rust)."""
    cov = y.rolling(w, min_periods=w).cov(x)
    var = x.rolling(w, min_periods=w).var()
    beta = cov / var
    return beta.where(var.abs() > 1e-12, 0.0)


# ────────────────────────────────────────────────────────────────────────────
# Fractional differentiation
# ────────────────────────────────────────────────────────────────────────────
def fracdiff_weights(d: float, tol: float = 1e-5, max_lag: int = 2000) -> np.ndarray:
    """Binomial-expansion weights w_k = prod_{i=1..k} (i-1-d)/i, w_0 = 1
    (non-reversed). Truncated once |w_k| < tol."""
    w = [1.0]
    for k in range(1, max_lag):
        wk = w[-1] * ((k - 1.0) - d) / k
        if abs(wk) < tol:
            break
        w.append(wk)
    return np.asarray(w, dtype=np.float64)


def fracdiff(x: pd.Series | pd.DataFrame, d: float = 0.4, tol: float = 1e-5):
    """Fractional differentiation (Lopez de Prado): a stationary, memory-
    preserving feature. FIR via scipy lfilter with the (1-B)^d kernel."""
    from scipy.signal import lfilter

    weights = fracdiff_weights(d, tol)
    b = weights  # lfilter b[k] multiplies x[n-k]; we want sum_k w_k x[n-k]
    L = len(weights)

    def _filt(col: pd.Series) -> pd.Series:
        out = pd.Series(lfilter(b, [1.0], col.to_numpy(dtype=np.float64)), index=col.index)
        out.iloc[: L - 1] = np.nan
        return out

    if isinstance(x, pd.DataFrame):
        return x.apply(_filt)
    return _filt(x)


# ────────────────────────────────────────────────────────────────────────────
# Calendar-boundary aggregation (Timeframe-driven)
# ────────────────────────────────────────────────────────────────────────────
def aggregate_bars(long: pd.DataFrame, timeframe: Timeframe) -> pd.DataFrame:
    """Aggregate a long ``raw_bars`` frame (output of ``load_raw_bars``) to
    coarser calendar-boundary bars. One row per (asset, bucket); buckets are
    calendar boundaries (`Timeframe.bucket`), emitted even if partial (a
    boundary-based bucket need not have a fixed bar count)."""
    if not isinstance(timeframe, Timeframe):
        raise TypeError("aggregate_bars expects a Timeframe")
    tmp = long.copy()
    tmp["_bucket"] = tmp["ts_epoch_ms"].map(timeframe.bucket)

    agg = (
        tmp.groupby(["asset", "_bucket"], sort=True)
        .agg(
            ts_epoch_ms=("ts_epoch_ms", "last"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
        .drop(columns=["_bucket"])
    )
    return _attach_et(agg).sort_values(["asset", "ts_epoch_ms"]).reset_index(drop=True)


__all__ = [
    "rolling_sum", "rolling_mean", "rolling_std", "rolling_max", "rolling_min",
    "rolling_z", "rolling_rank_pct", "ema", "rolling_roc", "session_vwap",
    "rolling_beta", "fracdiff", "fracdiff_weights", "aggregate_bars",
]
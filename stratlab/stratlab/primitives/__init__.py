"""Rolling primitives: reference (streaming) + vectorized + verification.

Public surface is intentionally two parallel namespaces:
* ``reference`` — streaming classes, the Rust-spec twins (for verification and
  for layering on ``RollingAggregator`` output).
* ``_vec`` — vectorized fast functions (the research path).

`verify` checks the two agree and that everything is causal.
"""

from . import reference, verify, _vec
from .reference import (
    RollingSum, RollingMean, RollingStd, RollingZScore,
    RollingMax, RollingMin, RollingRoc, EwmMean,
    RollingRankPct, RollingDayVwap, RollingBeta, RollingFracDiff,
    RollingAggregator,
)
from ._vec import (
    rolling_sum, rolling_mean, rolling_std, rolling_max, rolling_min,
    rolling_z, rolling_rank_pct, ema, rolling_roc, session_vwap,
    rolling_beta, fracdiff, fracdiff_weights, aggregate_bars,
)

__all__ = [
    "reference", "verify", "_vec",
    "RollingSum", "RollingMean", "RollingStd", "RollingZScore",
    "RollingMax", "RollingMin", "RollingRoc", "EwmMean",
    "RollingRankPct", "RollingDayVwap", "RollingBeta", "RollingFracDiff",
    "RollingAggregator",
    "rolling_sum", "rolling_mean", "rolling_std", "rolling_max", "rolling_min",
    "rolling_z", "rolling_rank_pct", "ema", "rolling_roc", "session_vwap",
    "rolling_beta", "fracdiff", "fracdiff_weights", "aggregate_bars",
]
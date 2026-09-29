"""stratlab — causal research + backtesting toolkit for ``raw_bars.parquet``,
parity-ported from the Rust trading-app backtester
(``trading/trading-app/src/backtester`` + ``strategy/helpers/rolling_fn.rs``).

Layers
------
* ``io`` — loader + wide-grid ``Universe`` + ``Timeframe`` / ``Bar``.
* ``primitives`` — rolling helpers (streaming reference + vectorized) and
  ``RollingAggregator`` (calendar-boundary multi-timeframe adapter).
* ``cost`` — IBKR Pro commission + fill/slippage model (Rust `fill_model.rs`).
* ``metrics`` — Rust `results.rs` parity port.
"""

from __future__ import annotations

__version__ = "0.1.0"

from . import cost, engine, io, metrics, parity, primitives, study
from .cost import CommissionModel, commission, decide_fill
from .engine import EngineConfig, Order, Outcome, Strategy, backtest_fast, run_thorough
from .io import Bar, CalendarUnit, Timeframe, Universe, load_default, load_raw_bars
from .metrics import compute_results
from .metrics.dynamic import conditional_by_score, forward_bps, mean_bps_to_eod
from .metrics.full import full_report
from .primitives import RollingAggregator

__all__ = [
    "Bar",
    "CalendarUnit",
    "CommissionModel",
    "EngineConfig",
    "Order",
    "Outcome",
    "RollingAggregator",
    "Strategy",
    "Timeframe",
    "Universe",
    "__version__",
    "backtest_fast",
    "commission",
    "compute_results",
    "cost",
    "decide_fill",
    "engine",
    "io",
    "load_default",
    "load_raw_bars",
    "metrics",
    "primitives",
    "run_thorough",
    "study",
]
"""Backtest engine — two modes:

* `fast` — vectorized screening (signal grid -> pnl). NOT the parity target.
* `thorough` — bar-by-bar event replay matching the Rust backtester (population
  Sharpe/Sortino, IBKR commission, OCA brackets, exact fills, SGD FX).
"""

from . import contract, fast, thorough
from .contract import (
    MKT, LMT, STP, MIDPRICE, MIDCLOSE,
    FIXED_FX_TO_SGD, fx_to_sgd, good_after_time_to_mso,
    Order, Outcome, Clock, Context, Strategy, EngineConfig,
)
from .fast import FastResult, backtest_fast
from .thorough import MarketState, ThoroughResult, run_thorough

__all__ = [
    "MKT", "LMT", "STP", "MIDPRICE", "MIDCLOSE",
    "FIXED_FX_TO_SGD", "fx_to_sgd", "good_after_time_to_mso",
    "Order", "Outcome", "Clock", "Context", "Strategy", "EngineConfig",
    "FastResult", "backtest_fast",
    "MarketState", "ThoroughResult", "run_thorough",
    "contract", "fast", "thorough",
]
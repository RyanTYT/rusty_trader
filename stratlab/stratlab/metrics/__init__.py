"""Metrics — Rust backtester `results.rs` parity port (and later, the full-stats
battery)."""

from . import dynamic, full, rust
from .rust import (
    BacktestResults,
    bars_per_year,
    compute_results,
    max_drawdown_pct,
    realized_per_trade_pnl,
)

__all__ = [
    "BacktestResults",
    "bars_per_year",
    "compute_results",
    "max_drawdown_pct",
    "realized_per_trade_pnl",
    "rust",
]
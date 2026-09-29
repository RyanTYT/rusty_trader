"""Rust-backtester parity harness.

Validates Python numerics against a committed Rust ``backtest_results_*.json``
golden (the output of `trading/trading-app/src/backtester/output/results.rs`).

Two levels:

* **metric parity** (runnable today) — reproduce ``max_drawdown_pct``, ``sharpe``,
  ``sortino``, ``final_equity``, ``total_pnl``, ``total_return_pct`` and
  ``bars_per_year`` from the golden's ``equity_curve`` *alone*, using
  ``stratlab.metrics.rust``. These are expected to match to machine precision
  (validated: Sharpe/Sortino differ only by ~1e-14 float-summation noise).

* **full parity** (needs a Rust transaction dump — NOT in the results JSON) —
  compare exact ``num_trades`` / ``num_closed_trades`` / ``num_winning_trades``,
  final equity (within ~1bp) and Sharpe (within ~0.01) from the Python
  transaction list + equity curve.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field

from stratlab.metrics import rust as R


@dataclass
class RustGolden:
    path: str
    starting_capital: float
    final_equity: float
    total_pnl: float
    total_return_pct: float
    max_drawdown_pct: float
    num_trades: int
    num_closed_trades: int
    num_winning_trades: int
    win_rate_pct: float
    sharpe: float
    sortino: float
    bar_interval_minutes: int
    bars_per_year: float
    equity_curve: list[dict] = field(default_factory=list)

    @classmethod
    def load(cls, path: str) -> "RustGolden":
        with open(path) as f:
            d = json.load(f)
        kw = {
            "path": path,
            **{k: d.get(k) for k in (
                "starting_capital", "final_equity", "total_pnl", "total_return_pct",
                "max_drawdown_pct", "num_trades", "num_closed_trades",
                "num_winning_trades", "win_rate_pct", "sharpe", "sortino",
                "bar_interval_minutes", "bars_per_year",
            )},
            "equity_curve": d.get("equity_curve", []),
        }
        return cls(**kw)


def default_golden_path() -> str:
    """The repo's canonical VP-touch golden."""
    here = os.path.dirname(os.path.abspath(__file__))       # .../stratlab/stratlab
    repo = os.path.dirname(os.path.dirname(here))            # .../rusty_trader
    return os.path.join(repo, "trading", "trading-app", "backtest_results_vp_touch.json")


def recompute_from_equity(golden: RustGolden) -> dict:
    """Recompute the parity metrics purely from the equity_curve (Rust math)."""
    eq = [p["equity"] for p in golden.equity_curve]
    mdd = R.max_drawdown_pct(eq)
    sharpe_bar, sortino_bar = R._sharpe_sortino(eq)
    annual = math.sqrt(R.bars_per_year(golden.bar_interval_minutes))
    final = eq[-1]
    total_pnl = final - golden.starting_capital
    return {
        "max_drawdown_pct": mdd,
        "sharpe": sharpe_bar * annual,
        "sortino": sortino_bar * annual,
        "final_equity": final,
        "total_pnl": total_pnl,
        "total_return_pct": total_pnl / golden.starting_capital * 100.0 if golden.starting_capital else 0.0,
        "bars_per_year": R.bars_per_year(golden.bar_interval_minutes),
    }


def metric_parity_problems(golden: RustGolden, *, rtol: float = 1e-8, atol: float = 1e-8) -> list[str]:
    """List of metric-parity discrepancies (empty = pass)."""
    got = recompute_from_equity(golden)
    problems = []
    for f, value in got.items():
        want = getattr(golden, f)
        if not math.isclose(value, want, rel_tol=rtol, abs_tol=atol):
            problems.append(f"{f}: python={value!r} rust={want!r}")
    return problems


def full_parity_problems(
    golden: RustGolden,
    py_transactions: list[tuple],
    py_equity_curve: list[dict],
    *,
    equity_rtol: float = 1e-4,
    sharpe_atol: float = 0.01,
) -> list[str]:
    """Full trade-level parity (needs a Rust transaction dump). Tolerances are
    the locked acceptance gate: exact trade count, equity within ~1bp, Sharpe
    within ~0.01. `py_transactions` rows are `(stock, exchange, currency,
    quantity, price, fees)` in chronological order (the engine's format)."""
    problems: list[str] = []
    if len(py_transactions) != golden.num_trades:
        problems.append(f"num_trades: python={len(py_transactions)} rust={golden.num_trades}")

    per_trade = R.realized_per_trade_pnl(py_transactions)
    n_closed = len(per_trade)
    if n_closed != golden.num_closed_trades:
        problems.append(f"num_closed_trades: python={n_closed} rust={golden.num_closed_trades}")

    n_win = sum(1 for p in per_trade if p > 0)
    if n_win != golden.num_winning_trades:
        problems.append(f"num_winning_trades: python={n_win} rust={golden.num_winning_trades}")

    if py_equity_curve:
        final = py_equity_curve[-1]["equity"]
        if not math.isclose(final, golden.final_equity, rel_tol=equity_rtol):
            problems.append(f"final_equity: python={final!r} rust={golden.final_equity!r}")
        eq = [p["equity"] for p in py_equity_curve]
        sharpe_bar, _ = R._sharpe_sortino(eq)
        annual = math.sqrt(R.bars_per_year(golden.bar_interval_minutes))
        py_sharpe = sharpe_bar * annual
        if abs(py_sharpe - golden.sharpe) > sharpe_atol:
            problems.append(f"sharpe: python={py_sharpe!r} rust={golden.sharpe!r}")

    return problems


__all__ = [
    "RustGolden", "default_golden_path", "recompute_from_equity",
    "metric_parity_problems", "full_parity_problems",
]
"""Backtest results & metrics — verbatim port of
``trading/trading-app/src/backtester/output/results.rs``.

These are the metrics whose *numerics* must match the Rust backtester for a
parity check. Important, easy-to-miss details (different from ``stratkit``'s
old cost-aware backtester):

* Sharpe/Sortino use **population** variance (divisor n), NOT ddof=1.
* Returns are **arithmetic** ``(cur - prev) / prev``, NOT log returns.
* Sortino downside is ``mean(r^2 for r < 0) / n`` (all n, not the downside
  count, and about zero, not about the mean).
* Max drawdown is ``(peak - equity) / peak * 100`` (a positive percent).
* Per-trade PnL uses a running avg-cost blend; fees are deducted on sells only.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


def bars_per_year(bar_interval_minutes: float) -> float:
    if bar_interval_minutes >= 1440:
        return 252.0
    return (390.0 * 252.0) / bar_interval_minutes


def max_drawdown_pct(equities: list[float]) -> float:
    peak = float("-inf")
    max_dd = 0.0
    for e in equities:
        if e > peak:
            peak = e
        if peak > 0.0:
            dd = (peak - e) / peak * 100.0
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _sharpe_sortino(equities: list[float]) -> tuple[float, float]:
    """Per-bar (NOT annualized) Sharpe & Sortino from equity snapshots."""
    if len(equities) < 2:
        return 0.0, 0.0
    returns = [
        (cur - prev) / prev if prev > 0.0 else 0.0
        for prev, cur in zip(equities, equities[1:])
    ]
    n = len(returns)
    mean = sum(returns) / n
    var = sum((r - mean) ** 2 for r in returns) / n  # population
    std = math.sqrt(var)
    downside_var = sum(r * r for r in returns if r < 0.0) / n
    downside_std = math.sqrt(downside_var)
    sharpe = mean / std if std > 0.0 else 0.0
    sortino = mean / downside_std if downside_std > 0.0 else 0.0
    return sharpe, sortino


def realized_per_trade_pnl(transactions: list[tuple[str, str, str, float, float, float]]) -> list[float]:
    """Running avg-cost per-trade PnL. `transactions` rows are
    ``(stock, exchange, currency, quantity, price, fees)``, quantity +buy/-sell."""
    state: dict[str, list[float]] = {}  # key -> [qty, cost]
    pnls: list[float] = []
    for stock, pe, currency, quantity, price, fees in transactions:
        key = f"{stock}:{pe}:{currency}"
        qty, cost = state.setdefault(key, [0.0, 0.0])
        if quantity >= 0.0:  # buy — blend avg cost
            new_qty = qty + quantity
            if abs(new_qty) > 1e-9:
                cost = (qty * cost + quantity * price) / new_qty
            qty = new_qty
            state[key] = [qty, cost]
        else:  # sell — realize
            sell_qty = abs(quantity)
            realized_qty = min(sell_qty, abs(qty))
            pnls.append((price - cost) * realized_qty - fees)
            qty -= sell_qty
            if abs(qty) < 1e-9:
                cost = 0.0
            state[key] = [qty, cost]
    return pnls


@dataclass
class BacktestResults:
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

    def to_dict(self) -> dict:
        return {
            "starting_capital": self.starting_capital,
            "final_equity": self.final_equity,
            "total_pnl": self.total_pnl,
            "total_return_pct": self.total_return_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "num_trades": self.num_trades,
            "num_closed_trades": self.num_closed_trades,
            "num_winning_trades": self.num_winning_trades,
            "win_rate_pct": self.win_rate_pct,
            "sharpe": self.sharpe,
            "sortino": self.sortino,
            "bar_interval_minutes": self.bar_interval_minutes,
            "bars_per_year": self.bars_per_year,
            "equity_curve": self.equity_curve,
        }


def compute_results(
    *,
    equity_curve: list[dict],
    transactions: list[tuple[str, str, str, float, float, float]],
    starting_capital: float,
    bar_interval_minutes: int = 5,
) -> BacktestResults:
    """Build the full results dict the Rust `BacktestResults::build` produces.

    `equity_curve` = list of dicts with at least `equity` (e.g. the per-bar
    snapshots ``{time, cash, positions_value, equity}``). `transactions` rows
    are ``(stock, exchange, currency, quantity, price, fees)``.
    """
    equities = [s["equity"] for s in equity_curve]
    per_trade = realized_per_trade_pnl(transactions)
    num_closed = len(per_trade)
    num_winning = sum(1 for p in per_trade if p > 0.0)
    win_rate = (num_winning / num_closed * 100.0) if num_closed > 0 else 0.0

    final_equity = equities[-1] if equities else starting_capital
    total_pnl = final_equity - starting_capital
    total_return_pct = (total_pnl / starting_capital * 100.0) if starting_capital != 0.0 else 0.0
    mdd = max_drawdown_pct(equities)

    sharpe_bar, sortino_bar = _sharpe_sortino(equities)
    bpy = bars_per_year(bar_interval_minutes)
    annualization = math.sqrt(bpy)
    sharpe = sharpe_bar * annualization
    sortino = sortino_bar * annualization

    return BacktestResults(
        starting_capital=starting_capital,
        final_equity=final_equity,
        total_pnl=total_pnl,
        total_return_pct=total_return_pct,
        max_drawdown_pct=mdd,
        num_trades=len(transactions),
        num_closed_trades=num_closed,
        num_winning_trades=num_winning,
        win_rate_pct=win_rate,
        sharpe=sharpe,
        sortino=sortino,
        bar_interval_minutes=bar_interval_minutes,
        bars_per_year=bpy,
        equity_curve=equity_curve,
    )


__all__ = [
    "bars_per_year", "max_drawdown_pct", "realized_per_trade_pnl",
    "BacktestResults", "compute_results",
]
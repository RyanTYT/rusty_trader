"""Fast (vectorized) backtest mode.

This is the *screening* mode, NOT the parity target. Conventions (no look-ahead):

    signal[t] = target position computed with info up to bar t
    pos[t]    = position HELD during bar t = signal[t-1]  (acts next bar)
    gross[t]  = pos[t] * logret[t]
    cost[t]   = |pos[t] - pos[t-1]| * per_side_cost
    net[t]    = gross[t] - cost[t]

Metrics here use log returns + sample std (ddof=1) annualized — different from
the Rust `results.rs` parity metric (population-variance arithmetic) in
`stratlab.metrics.rust`, which the thorough mode uses. Use `fast` to scan and
`thorough` to get the number you believe.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd


@dataclass
class FastResult:
    equity: pd.Series                 # starts at starting_equity
    net_ret: pd.Series
    gross_ret: pd.Series
    per_asset_net: pd.DataFrame | None
    metrics: dict


def _fast_metrics(net: pd.Series, pos, ppy: float, bps_per_side: float) -> dict:
    n = len(net)
    mean = net.mean()
    std = net.std(ddof=1)
    sharpe = (mean / std) * np.sqrt(ppy) if std > 0 else np.nan
    downside = net[net < 0]
    dstd = downside.std(ddof=1) if len(downside) > 1 else np.nan
    sortino = (mean / dstd) * np.sqrt(ppy) if dstd and dstd > 0 else np.nan
    equity = (1.0 + net).cumprod()
    max_dd = float((equity / equity.cummax() - 1.0).min())
    turnover = float(pos.diff().abs().sum().sum() if isinstance(pos, pd.DataFrame) else pos.diff().abs().sum())
    n_trades = int((pos.diff().abs() > 0).sum().sum() if isinstance(pos, pd.DataFrame) else (pos.diff().abs() > 0).sum())
    win_rate = float((net > 0).sum() / max(int((net != 0).sum()), 1))
    return {
        "sharpe_ann": float(sharpe),
        "sortino_ann": float(sortino),
        "max_dd": max_dd,
        "total_ret": float(equity.iloc[-1] - 1.0) if n else np.nan,
        "vol_ann": float(std * np.sqrt(ppy)),
        "turnover_total": turnover,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "mean_ret_bps": float(mean * 1e4),
        "n_bars": n,
        "bps_per_side": bps_per_side,
    }


def backtest_fast(
    signal: pd.DataFrame | pd.Series,
    uni,
    *,
    starting_equity: float = 1.0,
    bps_per_side: float = 5.0,
    ppy: float | None = None,
    aggregate: str = "equal",
    cost_fn: Callable[[pd.DataFrame | pd.Series], pd.DataFrame | pd.Series] | None = None,
) -> FastResult:
    """Run a cost-aware vectorized backtest.

    Parameters
    ----------
    signal : wide target-position grid (one column per asset) or 1-D series.
    uni : a `Universe` (used for `grid_logret`).
    starting_equity : initial capital; `equity` starts here.
    bps_per_side : flat cost per side (default 5 bps); ignored if `cost_fn`.
    ppy : periods per year; defaults to `uni.ppy_intraday`.
    aggregate : 'equal' (mean across assets) or 'sum'.
    cost_fn : optional callable `(pos) -> cost_ret` overriding the flat bps.
    """
    pos = signal.shift(1).fillna(0.0)
    logret = uni.grid_logret

    if isinstance(signal, pd.DataFrame):
        gross = pos * logret
    else:
        gross = pos * logret

    if cost_fn is not None:
        cost = cost_fn(pos)
    else:
        turnover = pos.diff().abs()
        cost = turnover * (bps_per_side / 1e4)

    net = gross - cost

    if isinstance(signal, pd.DataFrame):
        agg_gross = gross.mean(axis=1) if aggregate == "equal" else gross.sum(axis=1)
        agg_cost = cost.mean(axis=1) if aggregate == "equal" else cost.sum(axis=1)
        agg_net = agg_gross - agg_cost
        per_asset = net
    else:
        agg_net = net
        agg_gross = gross
        per_asset = None

    equity = (1.0 + agg_net).cumprod() * starting_equity
    ppy = ppy or uni.ppy_intraday
    metrics = _fast_metrics(agg_net, pos, ppy, bps_per_side)
    return FastResult(equity=equity, net_ret=agg_net, gross_ret=agg_gross,
                      per_asset_net=per_asset, metrics=metrics)


__all__ = ["backtest_fast", "FastResult"]
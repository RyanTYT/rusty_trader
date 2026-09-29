"""Full-stats battery for the thorough engine (the research layer — NOT the
Rust parity metric, which lives in `stratlab.metrics.rust`).

Consumes the rich fill ledger (`run_thorough`'s `state.fills`) + the Universe
(for close/high/low price paths) and produces:

* per-trade summary (win rate, PnL distribution, skew/kurt, streaks,
  profit factor, hold time, MFE/MAE);
* **positive-PnL-by-N** — for every entry (a new directional lot), the forward
  close-to-close return at horizons of N **bars** and N **trading days**, with
  P(PnL>0), mean/median/P5/P95;
* portfolio risk (Calmar, VaR/CVaR, tail ratio) from the equity curve.

Notes on convention:
  * "trade" = one round-trip lot (entry -> exit), matched FIFO per symbol.
  * forward positive-PnL uses the entry bar's CLOSE -> horizon CLOSE (signal
    quality, independent of exit); MFE/MAE use the actual fill price + intrabar
    high/low within the holding window.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from stratlab.metrics.rust import max_drawdown_pct


@dataclass
class Trade:
    symbol: str
    side: int                 # +1 long, -1 short
    qty: float
    entry_time: int
    entry_price: float
    exit_time: int | None = None
    exit_price: float | None = None
    gross_pnl: float | None = None
    net_pnl: float | None = None
    hold_bars: int | None = None
    hold_days: int | None = None
    mfe: float | None = None  # max favorable excursion (per share, >=0)
    mae: float | None = None  # max adverse excursion (per share, <=0)


# ────────────────────────────────────────────────────────────────────────────
# Trade matching (FIFO per symbol)
# ────────────────────────────────────────────────────────────────────────────
def close_trades(fills: list[dict]) -> list[Trade]:
    """Match fills into round-trip `Trade`s, FIFO per symbol. `fills` rows are
    `{time_ms, symbol, qty (signed), price, fees}` from `MarketState.fills`."""
    open_lots: dict[str, list[dict]] = {}
    trades: list[Trade] = []

    for f in fills:
        sym = f["symbol"]
        q = f["qty"]
        if q == 0:
            continue
        price, fees, t = f["price"], f["fees"], f["time_ms"]
        fill_dir = 1 if q > 0 else -1
        remaining = abs(q)
        lots = open_lots.setdefault(sym, [])

        # close opposite-side lots FIFO
        while remaining > 1e-12 and lots and lots[0]["side"] != fill_dir:
            lot = lots[0]
            matched = min(remaining, lot["qty"])
            gross = (price - lot["entry_price"]) * lot["side"] * matched
            entry_fee = lot["entry_fees"] * (matched / lot["qty_orig"]) if lot["qty_orig"] > 0 else 0.0
            exit_fee = fees * (matched / abs(q)) if abs(q) > 0 else 0.0
            trades.append(Trade(
                symbol=sym, side=lot["side"], qty=matched,
                entry_time=lot["entry_time"], entry_price=lot["entry_price"],
                exit_time=t, exit_price=price,
                gross_pnl=gross, net_pnl=gross - entry_fee - exit_fee,
            ))
            lot["qty"] -= matched
            remaining -= matched
            if lot["qty"] < 1e-12:
                lots.pop(0)

        # open a new lot for any leftover
        if remaining > 1e-12:
            entry_fee = fees * (remaining / abs(q)) if abs(q) > 0 else 0.0
            lots.append({
                "side": fill_dir, "qty": remaining, "entry_time": t,
                "entry_price": price, "entry_fees": entry_fee, "qty_orig": remaining,
            })

    # leftover open lots -> open trades
    for sym, lots in open_lots.items():
        for lot in lots:
            trades.append(Trade(sym, lot["side"], lot["qty"], lot["entry_time"], lot["entry_price"]))

    trades.sort(key=lambda t: (t.entry_time, t.symbol))
    return trades


# ────────────────────────────────────────────────────────────────────────────
# Small stats (bias-corrected, matching stats_math.rs)
# ────────────────────────────────────────────────────────────────────────────
def skewness(x: np.ndarray) -> float:
    x = np.asarray(x, dtype=np.float64)
    n = len(x)
    if n < 3:
        return float("nan")
    s = x.std(ddof=1)
    if s == 0 or not math.isfinite(s):
        return float("nan")
    return (n / ((n - 1) * (n - 2))) * np.sum(((x - x.mean()) / s) ** 3)


def kurtosis_excess(x: np.ndarray) -> float:
    x = np.asarray(x, dtype=np.float64)
    n = len(x)
    if n < 4:
        return float("nan")
    s = x.std(ddof=1)
    if s == 0 or not math.isfinite(s):
        return float("nan")
    z = (x - x.mean()) / s
    return (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * np.sum(z ** 4) \
        - 3.0 * (n - 1) ** 2 / ((n - 2) * (n - 3))


def _fn(x, default=float("nan")):
    x = [v for v in x if v is not None and math.isfinite(v)]
    return x if x else default


def trade_summary(trades: list[Trade]) -> dict:
    closed = [t for t in trades if t.exit_time is not None]
    open_ = [t for t in trades if t.exit_time is None]
    out = {"n_trades": len(trades), "n_closed": len(closed), "n_open": len(open_)}

    if closed:
        closed_sorted = sorted(closed, key=lambda t: (t.exit_time, t.symbol))
        pnls = np.asarray([t.net_pnl for t in closed_sorted], dtype=np.float64)
        wins = pnls[pnls > 0]
        losses = pnls[pnls <= 0]
        gross_gain = float(wins.sum())
        gross_loss = float(-losses.sum())

        out.update({
            "total_net": float(pnls.sum()),
            "win_rate": float((pnls > 0).mean()),
            "mean_net": float(pnls.mean()),
            "median_net": float(np.median(pnls)),
            "std_net": float(pnls.std(ddof=1)) if len(pnls) > 1 else 0.0,
            "skew_net": skewness(pnls) if len(pnls) >= 3 else float("nan"),
            "kurt_net": kurtosis_excess(pnls) if len(pnls) >= 4 else float("nan"),
            "profit_factor": float(gross_gain / gross_loss) if gross_loss > 0 else float("inf"),
            "expectancy": float(pnls.mean()),
        })

        # streaks
        max_win = max_loss = cur_win = cur_loss = 0
        for p in pnls:
            if p > 0:
                cur_win += 1
                cur_loss = 0
                max_win = max(max_win, cur_win)
            else:
                cur_loss += 1
                cur_win = 0
                max_loss = max(max_loss, cur_loss)
        out["max_win_streak"] = max_win
        out["max_loss_streak"] = max_loss

        hb = _fn([t.hold_bars for t in closed])
        hd = _fn([t.hold_days for t in closed])
        mfe = _fn([t.mfe for t in closed])
        mae = _fn([t.mae for t in closed])
        if hb:
            out["avg_hold_bars"] = float(np.mean(hb))
        if hd:
            out["avg_hold_days"] = float(np.mean(hd))
        if mfe:
            out["avg_mfe"] = float(np.mean(mfe))
        if mae:
            out["avg_mae"] = float(np.mean(mae))

    return out


# ────────────────────────────────────────────────────────────────────────────
# Price path index
# ────────────────────────────────────────────────────────────────────────────
def _price_index(uni):
    long = uni.long[["ts_epoch_ms", "asset", "close", "high", "low", "et_date"]].sort_values(
        ["asset", "ts_epoch_ms"]).reset_index(drop=True)
    idx = {}
    for sym, g in long.groupby("asset", sort=False):
        ts = g["ts_epoch_ms"].to_numpy(dtype=np.int64)
        dates_arr = g["et_date"].to_numpy()
        uniq_dates = []
        seen = set()
        for d in dates_arr:
            if d not in seen:
                seen.add(d)
                uniq_dates.append(d)
        date_id = {d: k for k, d in enumerate(uniq_dates)}
        di = np.array([date_id[d] for d in dates_arr], dtype=np.int64)
        date_last_idx = {}
        for j, d in enumerate(dates_arr):
            date_last_idx[d] = j
        idx[sym] = {
            "close": g["close"].to_numpy(dtype=np.float64),
            "high": g["high"].to_numpy(dtype=np.float64),
            "low": g["low"].to_numpy(dtype=np.float64),
            "di": di,
            "dates": uniq_dates,
            "date_last_idx": date_last_idx,
            "ts_to_i": {int(t): j for j, t in enumerate(ts)},
        }
    return idx


def _enrich_trades(trades: list[Trade], uni) -> None:
    px = _price_index(uni)
    for tr in trades:
        sm = px.get(tr.symbol)
        if sm is None or tr.exit_time is None:
            continue
        i = sm["ts_to_i"].get(int(tr.entry_time))
        j = sm["ts_to_i"].get(int(tr.exit_time))
        if i is None or j is None or j <= i:
            continue
        tr.hold_bars = j - i
        tr.hold_days = int(sm["di"][j] - sm["di"][i])
        hi = sm["high"][i + 1: j + 1]
        lo = sm["low"][i + 1: j + 1]
        entry = tr.entry_price
        if hi.size:
            if tr.side > 0:
                tr.mfe = float(hi.max() - entry)
                tr.mae = float(lo.min() - entry)
            else:
                tr.mfe = float(entry - lo.min())
                tr.mae = float(entry - hi.max())


# ────────────────────────────────────────────────────────────────────────────
# Positive-PnL-by-N (forward close-to-close; both bars and trading days)
# ────────────────────────────────────────────────────────────────────────────
def _horizon_stats(acc: dict[int, list]) -> dict:
    out = {}
    for n, vals in acc.items():
        if not vals:
            out[n] = {"p_positive": None, "mean": None, "median": None, "p05": None, "p95": None, "count": 0}
            continue
        a = np.asarray(vals, dtype=np.float64)
        out[n] = {
            "p_positive": float((a > 0).mean()),
            "mean": float(a.mean()),
            "median": float(np.median(a)),
            "p05": float(np.percentile(a, 5)),
            "p95": float(np.percentile(a, 95)),
            "count": int(len(a)),
        }
    return out


def positive_pnl_by_n(entries, uni, *, by_bars=(1, 2, 3, 5, 10, 21),
                      by_days=(1, 2, 3, 5, 10, 21)) -> dict:
    """Forward close-to-close return for each entry (time, symbol, side) at
    horizons of N bars and N trading days. Returns P(PnL>0) + mean/median/P5/P95."""
    px = _price_index(uni)
    bars_acc = {n: [] for n in by_bars}
    days_acc = {n: [] for n in by_days}

    for t, sym, side in entries:
        sm = px.get(sym)
        if sm is None:
            continue
        i = sm["ts_to_i"].get(int(t))
        if i is None:
            continue
        close = sm["close"]
        ci = close[i]
        if not (ci > 0 and math.isfinite(ci)):
            continue

        nbars = len(close)
        for n in by_bars:
            j = i + n
            if j < nbars and close[j] > 0 and math.isfinite(close[j]):
                bars_acc[n].append((close[j] / ci - 1.0) * side)

        di_now = sm["di"][i]
        dates = sm["dates"]
        last_idx = sm["date_last_idx"]
        for n in by_days:
            k = di_now + n
            if k < len(dates):
                j = last_idx[dates[k]]
                if close[j] > 0 and math.isfinite(close[j]):
                    days_acc[n].append((close[j] / ci - 1.0) * side)

    return {
        "n_entries": len(entries),
        "by_bars": _horizon_stats(bars_acc),
        "by_days": _horizon_stats(days_acc),
    }


# ────────────────────────────────────────────────────────────────────────────
# Portfolio risk (from the equity curve)
# ────────────────────────────────────────────────────────────────────────────
def _historical_var(xs, pct):
    s = sorted(xs)
    idx = min(int(math.floor(pct * (len(s) - 1))), len(s) - 1)
    return s[idx]


def _historical_cvar(xs, pct):
    var = _historical_var(xs, pct)
    tail = [x for x in xs if x <= var]
    return sum(tail) / len(tail) if tail else var


def _tail_ratio(xs):
    if len(xs) < 20:
        return float("nan")
    qhi = _historical_var(xs, 0.95)
    qlo = _historical_var(xs, 0.05)
    gains = [x for x in xs if x >= qhi]
    losses = [x for x in xs if x <= qlo]
    mg = sum(gains) / len(gains)
    ml = sum(losses) / len(losses)
    return mg / abs(ml) if ml and abs(ml) > 1e-18 else float("nan")


def portfolio_risk(equity_curve, *, ppy: float = 252.0) -> dict:
    eq = [p["equity"] for p in equity_curve]
    if len(eq) < 2:
        return {}
    rets = [(cur - prev) / prev for prev, cur in zip(eq, eq[1:]) if prev > 0]

    start, end = eq[0], eq[-1]
    n = len(eq) - 1
    ann_ret = (end / start) ** (ppy / n) - 1.0 if start > 0 else float("nan")
    mdd_frac = _max_drawdown_frac(eq)
    calmar = ann_ret / abs(mdd_frac) if mdd_frac < 0 and math.isfinite(ann_ret) else float("nan")

    return {
        "calmar": calmar,
        "var_95": _historical_var(rets, 0.05),
        "var_99": _historical_var(rets, 0.01),
        "cvar_95": _historical_cvar(rets, 0.05),
        "tail_ratio": _tail_ratio(rets),
    }


def _max_drawdown_frac(equities) -> float:
    peak = float("-inf")
    mdd = 0.0
    for e in equities:
        if e > peak:
            peak = e
        if peak > 0:
            mdd = min(mdd, e / peak - 1.0)
    return mdd


# ────────────────────────────────────────────────────────────────────────────
# One-shot
# ────────────────────────────────────────────────────────────────────────────
def full_report(uni, fills: list[dict], equity_curve: list[dict] | None = None, *,
                by_bars=(1, 2, 3, 5, 10, 21), by_days=(1, 2, 3, 5, 10, 21)) -> dict:
    trades = close_trades(fills)
    _enrich_trades(trades, uni)
    entries = [(t.entry_time, t.symbol, t.side) for t in trades]
    report = {
        "trade_summary": trade_summary(trades),
        "positive_pnl_by_n": positive_pnl_by_n(entries, uni, by_bars=by_bars, by_days=by_days),
    }
    if equity_curve is not None:
        report["portfolio_risk"] = portfolio_risk(equity_curve)
    report["trades"] = trades
    return report


__all__ = [
    "Trade", "close_trades", "trade_summary", "positive_pnl_by_n",
    "portfolio_risk", "full_report", "skewness", "kurtosis_excess",
]
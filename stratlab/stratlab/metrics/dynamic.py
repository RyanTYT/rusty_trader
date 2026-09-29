"""Dynamic, interpretable model-output goals.

The point: a model's output should be evaluated as a **dynamic, horizon-indexed
target** — the forward bps move over N bars, N trading days, and especially to
**End-of-Day** — rather than a single static label. This surfaces what a
prediction buys in real execution terms:

    "enter here -> mean +X bp by EoD, P(win) = Y%"

`forward_bps` returns the full curve; `conditional_by_score` buckets entries by
a model score to show how confidence maps to realized move (calibration /
monotonicity). Lots are signed, so a long counts `(close_f/close_i - 1) * +1`
and a short the inverse.
"""

from __future__ import annotations

import math

import numpy as np

from stratlab.metrics.full import _price_index


def _forward_bps_one(px, sym: str, t: int, side: int, horizon) -> float | None:
    sm = px.get(sym)
    if sm is None:
        return None
    i = sm["ts_to_i"].get(int(t))
    if i is None:
        return None
    close = sm["close"]
    ci = close[i]
    if not (ci > 0 and math.isfinite(ci)):
        return None

    if horizon == "eod":
        j = sm["date_last_idx"][sm["dates"][sm["di"][i]]]
    elif isinstance(horizon, tuple) and horizon[0] == "bar":
        j = i + horizon[1]
    elif isinstance(horizon, tuple) and horizon[0] == "day":
        k = sm["di"][i] + horizon[1]
        if k >= len(sm["dates"]):
            return None
        j = sm["date_last_idx"][sm["dates"][k]]
    else:
        raise ValueError(f"bad horizon {horizon!r} (use 'eod', ('bar', n), ('day', n))")

    if j <= i or j >= len(close):
        return None
    cj = close[j]
    if not (cj > 0 and math.isfinite(cj)):
        return None
    return (cj / ci - 1.0) * side * 1e4


def _agg(vals) -> dict:
    if not vals:
        return {"mean_bps": None, "p_positive": None, "median_bps": None,
                "p05": None, "p95": None, "count": 0}
    a = np.asarray(vals, dtype=np.float64)
    return {
        "mean_bps": float(a.mean()),
        "p_positive": float((a > 0).mean()),
        "median_bps": float(np.median(a)),
        "p05": float(np.percentile(a, 5)),
        "p95": float(np.percentile(a, 95)),
        "count": int(len(a)),
    }


def forward_bps(entries, uni, *, by_bars=(1, 2, 3, 5, 10, 21),
                by_days=(1, 2, 3, 5, 10, 21), to_eod: bool = True) -> dict:
    """Forward bps move (signed by side) for each entry (time, symbol, side)."""
    px = _price_index(uni)
    eod_acc = [] if to_eod else None
    bar_acc = {n: [] for n in by_bars}
    day_acc = {n: [] for n in by_days}

    for t, sym, side in entries:
        if to_eod:
            v = _forward_bps_one(px, sym, t, side, "eod")
            if v is not None:
                eod_acc.append(v)
        for n in by_bars:
            v = _forward_bps_one(px, sym, t, side, ("bar", n))
            if v is not None:
                bar_acc[n].append(v)
        for n in by_days:
            v = _forward_bps_one(px, sym, t, side, ("day", n))
            if v is not None:
                day_acc[n].append(v)

    out: dict = {"n_entries": len(entries)}
    if to_eod:
        out["eod"] = _agg(eod_acc)
    out["by_bars"] = {n: _agg(bar_acc[n]) for n in by_bars}
    out["by_days"] = {n: _agg(day_acc[n]) for n in by_days}
    return out


def mean_bps_to_eod(entries, uni) -> dict:
    """Aggregated mean bps to End-of-Day (the single most interpretable number)."""
    px = _price_index(uni)
    vals = [v for v in (_forward_bps_one(px, s, t, d, "eod") for t, s, d in entries) if v is not None]
    return _agg(vals)


def conditional_by_score(entries, scores, uni, *, horizon="eod", n_buckets: int = 5) -> list[dict]:
    """Bucket entries by model score (rank-split) and report the realized forward
    bps per bucket — the calibration/monotonicity view of the model's output."""
    px = _price_index(uni)
    pairs = []
    for (t, sym, side), s in zip(entries, scores):
        v = _forward_bps_one(px, sym, t, side, horizon)
        if v is not None:
            pairs.append((float(s), v))
    if not pairs or n_buckets < 1:
        return []

    pairs.sort(key=lambda x: x[0])
    n = len(pairs)
    buckets = []
    for k in range(n_buckets):
        lo = k * n // n_buckets
        hi = (k + 1) * n // n_buckets
        if lo >= hi:
            continue
        chunk = pairs[lo:hi]
        sc = [s for s, _ in chunk]
        vals = [v for _, v in chunk]
        agg = {kk: vv for kk, vv in _agg(vals).items() if kk != "count"}
        buckets.append({
            "bucket": k,
            "count": len(chunk),
            "score_low": sc[0],
            "score_high": sc[-1],
            "mean_score": float(np.mean(sc)),
            **agg,
        })
    return buckets


# ────────────────────────────────────────────────────────────────────────────
# Readable rendering
# ────────────────────────────────────────────────────────────────────────────
def format_forward(report: dict) -> str:
    lines = []
    eod = report.get("eod")
    if eod and eod.get("count"):
        lines.append(
            f"EoD: mean {eod['mean_bps']:+.1f}bp  P(win)={eod['p_positive']:.1%}  "
            f"median {eod['median_bps']:+.1f}bp  n={eod['count']}")
    for unit in ("by_bars", "by_days"):
        d = report.get(unit)
        if not d:
            continue
        lines.append(f"{unit}:")
        for n, row in d.items():
            if row.get("count"):
                lines.append(
                    f"  {n:>3}: mean {row['mean_bps']:+.1f}bp  P(win)={row['p_positive']:.1%}  "
                    f"median {row['median_bps']:+.1f}bp  n={row['count']}")
    return "\n".join(lines)


def format_conditional(buckets: list[dict]) -> str:
    lines = ["score bucket -> forward bps:"]
    for b in buckets:
        if b.get("count"):
            lines.append(
                f"  [{b['score_low']:.3f},{b['score_high']:.3f}] n={b['count']:<4} "
                f"mean {b['mean_bps']:+.1f}bp  P(win)={b['p_positive']:.1%}")
    return "\n".join(lines)


__all__ = [
    "forward_bps", "mean_bps_to_eod", "conditional_by_score",
    "format_forward", "format_conditional",
]
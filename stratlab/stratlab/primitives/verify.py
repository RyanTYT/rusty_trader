"""Verification harness for the rolling primitives.

Two things are guaranteed here:
1. **Causality** — a vectorized helper's output at time `t` depends only on
   inputs `<= t` (truncating the future leaves past outputs unchanged).
2. **Streaming == vectorized** — the ``reference.py`` classes (the Rust-spec
   twits) agree with the ``_vec.py`` functions to within floating-point noise.
3. **Streaming aggregator == vectorized aggregate_bars** — calendar-boundary
   grouping (with a final flush) reproduces the groupby result exactly.

An LLM extending the library should run ``check_causal(new_fn)`` and
``check_streaming_vs_vec(cls, fn)`` rather than trust a hand-rolled indicator.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from stratlab import primitives
from stratlab.io import Bar, Timeframe
from stratlab.primitives import reference as R
from stratlab.primitives import _vec


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────
def _allclose(a, b, rtol=1e-9, atol=1e-10) -> bool:
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)
    # NaN == NaN for our purposes (warm-up regions)
    both_nan = np.isnan(a) & np.isnan(b)
    return bool(np.allclose(np.where(both_nan, 0.0, a), np.where(both_nan, 0.0, b),
                            rtol=rtol, atol=atol, equal_nan=True))


def check_causal(f, rtol=1e-9, atol=1e-10) -> bool:
    """Assert `f` is causal: `f(x)[:t] == f(x[:t])` for a sample of cut points."""
    rng = np.random.default_rng(0)
    x = pd.Series(rng.standard_normal(300) + 100.0)
    y = f(x)
    for t in (10, 50, 123, 250):
        y_t = f(x.iloc[:t])
        lhs = y.iloc[:t].to_numpy(dtype=float)
        rhs = y_t.to_numpy(dtype=float)
        if not _allclose(lhs, rhs, rtol=rtol, atol=atol):
            return False
    return True


def _stream(cls, xs):
    obj = cls
    out = []
    for v in xs:
        r = obj.push(v)
        out.append(math.nan if r is None else r)
    return out


def check_streaming_vs_vec(cls, fn, *, kwargs=None, vec_kwargs=None, n=300,
                           rtol=1e-9, atol=1e-10) -> bool:
    """Compare a streaming `reference` class against its `_vec` twin on random
    data. `fn` is the `_vec` function; `kwargs`/`vec_kwargs` carry ctor/vector
    args (e.g. window)."""
    rng = np.random.default_rng(1)
    xs = list(rng.standard_normal(n) + 100.0)
    kwargs = kwargs or {}
    vec_kwargs = vec_kwargs or {}
    streaming = np.array(_stream(cls(**kwargs), xs), dtype=float)
    vectorized = fn(pd.Series(xs), **vec_kwargs).to_numpy(dtype=float)
    return _allclose(streaming, vectorized, rtol=rtol, atol=atol)


# ────────────────────────────────────────────────────────────────────────────
# Synthetic raw_bars (5-min, 78/day) — for the aggregator parity test
# ────────────────────────────────────────────────────────────────────────────
def make_synthetic_raw_bars(n_days=5, assets=("A", "B"), seed=7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    days = pd.bdate_range("2024-01-02", periods=n_days)
    mso = np.arange(0, 78) * 5  # 0,5,...,385
    rows = []
    for d in days:
        for m in mso:
            et_naive = d + pd.Timedelta(minutes=570 + m)
            et = et_naive.tz_localize("America/New_York")
            ts = int(et.tz_convert("UTC").value // 1_000_000)  # ms
            for a in assets:
                px = 100.0 * (1 + rng.normal(0, 0.001))
                o = px * (1 + rng.normal(0, 0.0005))
                h = max(o, px) + abs(rng.normal(0, 0.001))
                l = min(o, px) - abs(rng.normal(0, 0.001))
                v = float(rng.integers(1000, 50000))
                rows.append((ts, a, o, h, l, px, v))
    df = pd.DataFrame(rows, columns=["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"])
    from stratlab.io import _attach_et
    return _attach_et(df).sort_values(["asset", "ts_epoch_ms"]).reset_index(drop=True)


def check_aggregator_vs_vec(specs: list[Timeframe] | None = None, rtol=1e-9, atol=1e-10) -> bool:
    """Streaming `RollingAggregator` (with flush) must equal `aggregate_bars`."""
    specs = specs or [Timeframe.minutes(15), Timeframe.minutes(30), Timeframe.hourly(), Timeframe.daily()]
    long = make_synthetic_raw_bars()
    for tf in specs:
        # streaming
        got = {}
        for asset, g in long.groupby("asset"):
            agg = R.RollingAggregator(tf)
            acc = []
            for _, row in g.iterrows():
                bar = Bar(ts_epoch_ms=int(row["ts_epoch_ms"]), open=row["open"], high=row["high"],
                          low=row["low"], close=row["close"], volume=row["volume"])
                out = agg.push(bar)
                if out is not None:
                    acc.append(out)
            out = agg.flush()
            if out is not None:
                acc.append(out)
            got[asset] = acc

        # vectorized
        vec = _vec.aggregate_bars(long, tf)
        for asset in long["asset"].unique():
            want = vec[vec["asset"] == asset].sort_values("ts_epoch_ms")
            stream_bars = got[asset]
            if len(stream_bars) != len(want):
                return False
            for sb, (_, row) in zip(stream_bars, want.iterrows()):
                for fld in ("open", "high", "low", "close", "volume"):
                    if not math.isclose(getattr(sb, fld), row[fld], rel_tol=rtol, abs_tol=atol):
                        return False
                if sb.ts_epoch_ms != int(row["ts_epoch_ms"]):
                    return False
    return True


# ────────────────────────────────────────────────────────────────────────────
# The one-shot runner
# ────────────────────────────────────────────────────────────────────────────
def run_all() -> dict[str, bool]:
    """Run every check; return {name: passed}. Raises nothing."""
    results: dict[str, bool] = {}

    # causality of the vectorized twins
    causal = {
        "rolling_sum_w20": lambda x: _vec.rolling_sum(x, 20),
        "rolling_mean_w20": lambda x: _vec.rolling_mean(x, 20),
        "rolling_std_w20": lambda x: _vec.rolling_std(x, 20),
        "rolling_z_w20": lambda x: _vec.rolling_z(x, 20),
        "rolling_max_w20": lambda x: _vec.rolling_max(x, 20),
        "rolling_min_w20": lambda x: _vec.rolling_min(x, 20),
        "rolling_rank_pct_w20": lambda x: _vec.rolling_rank_pct(x, 20),
        "ema_20": lambda x: _vec.ema(x, 20),
        "rolling_roc_5": lambda x: _vec.rolling_roc(x, 5),
        "fracdiff": lambda x: _vec.fracdiff(x, d=0.4),
    }
    for name, f in causal.items():
        results[f"causal:{name}"] = check_causal(f)

    # streaming == vectorized
    stream_vec = [
        ("sum", R.RollingSum, _vec.rolling_sum, {"window": 20}, {"w": 20}),
        ("mean", R.RollingMean, _vec.rolling_mean, {"window": 20}, {"w": 20}),
        ("std", R.RollingStd, _vec.rolling_std, {"window": 20}, {"w": 20}),
        ("z", R.RollingZScore, _vec.rolling_z, {"window": 20}, {"w": 20}),
        ("max", R.RollingMax, _vec.rolling_max, {"window": 20}, {"w": 20}),
        ("min", R.RollingMin, _vec.rolling_min, {"window": 20}, {"w": 20}),
        ("roc", R.RollingRoc, _vec.rolling_roc, {"diff": 5}, {"diff": 5}),
        ("ema", R.EwmMean, _vec.ema, {"span": 20}, {"span": 20}),
        ("rank_pct", R.RollingRankPct, _vec.rolling_rank_pct, {"window": 20}, {"w": 20}),
    ]
    for name, cls, fn, ctor_kw, vec_kw in stream_vec:
        try:
            results[f"stream==vec:{name}"] = check_streaming_vs_vec(
                cls, fn, kwargs=ctor_kw, vec_kwargs=vec_kw)
        except Exception:
            results[f"stream==vec:{name}"] = False

    results["aggregator==vec"] = check_aggregator_vs_vec()
    return results


def main() -> int:
    results = run_all()
    n_fail = 0
    for name, ok in results.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
        n_fail += 0 if ok else 1
    print(f"\n{len(results) - n_fail}/{len(results)} passed")
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
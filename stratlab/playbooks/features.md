# Feature engineering

Build inputs to the signal. Every feature must be **causal** (see root
invariant #1) and **verified** before use.

## Primitives (the verification spec, not a menu)

`stratlab.primitives` has two parallel namespaces:

- `reference` — streaming classes, the Python twins of Rust `rolling_fn.rs`
  (`RollingMean`, `RollingStd`, `RollingZScore`, `RollingMax/Min`, `RollingRoc`,
  `EwmMean`, `RollingRankPct`, `RollingDayVwap`, `RollingBeta`, `RollingFracDiff`,
  `RollingAggregator`). Each docstring states the exact Rust math + "None until
  full" semantics.
- `_vec` — the vectorized, efficient twins (`rolling_mean`, `rolling_z`, `ema`,
  `session_vwap`, `fracdiff`, `aggregate_bars`, …), strictly causal.

They are deliberately **not** exhaustive. If you need a feature that isn't here,
write it yourself as a `_vec`-style function over the wide deck — don't feel
constrained to this list.

## Verify before you trust

```python
from stratlab.primitives import verify
verify.check_causal(lambda x: my_feature(x))            # output[t] depends only on x[<=t]
verify.check_streaming_vs_vec(MyStreamingCls, my_vec_fn, kwargs={...}, vec_kwargs={...})
```

`check_causal` truncates the future and asserts past outputs unchanged.
`check_streaming_vs_vec` proves a streaming class and its vectorized twin agree.

## Key parity details that silently bite

- **std** is sample (ddof=1) — matches `RollingStd`.
- **EWMA** `alpha = 2/(span+1)`, `adjust=False` — emits from bar 0.
- **rank pct** = count(values <= current)/window (ties count fully), NOT
  pandas `.rank(pct=True)`.
- **VWAP** is close-weighted (`Σ close·vol / Σ vol`), NOT typical-price.

## Wide deck

`Universe` caches wide grids indexed `(et_date, minutes_since_open)` with one
column per asset: `.grid_close`, `.grid_open`, `.grid_high`, `.grid_low`,
`.grid_volume`, `.grid_dollar_vol`, `.grid_logret` (intraday only). Session-aware
helpers (e.g. `session_vwap`) expect the date at level 0.

## Multi-timeframe

Aggregate to a coarser bar (calendar boundary — see root invariant, and
`playbooks/execution.md` §fills) before applying rolling helpers:

```python
uni15 = uni.resample(Timeframe.minutes(15))   # -> a coarser Universe
```

## Why-capture

Record why this feature exists (the economic/microstructure reason), the formula
with its causal horizon, and the alternatives you rejected — `feature()` in the
study DSL already insists on these.
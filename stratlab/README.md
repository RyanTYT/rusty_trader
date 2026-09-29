# stratlab

A **pure-Python, contract-first** toolkit for developing and backtesting
intraday strategies against `raw_bars.parquet`, with the guarantee that a
strategy can be ported to the Rust trading-app and validated to **numerically
identical** backtest results.

`stratlab` is a Python *re-implementation* of the Rust backtester's numerics
(not a wrapper over it). Its two load-bearing rules are:

1. **Causality** — never peek at the future.
2. **Parity** — never disagree with the Rust backtester.

It is designed to be driven by an LLM from bash, with a thin `SKILL.md` for
directions and per-phase playbooks loaded on demand.

```bash
cd rusty_trader
pip install -e stratlab           # once; gives the `stratlab` command
stratlab verify                   # 20 causality + parity checks
```

---

## Layout

```
stratlab/
  pyproject.toml          # console script `stratlab`
  SKILL.md                # thin root: invariants + routing (NOT a menu)
  playbooks/              # features / signal / execution / validation / bias / parity
  README.md
  stratlab/
    io.py                 # Bar, Universe, Timeframe, load_raw_bars, resample
    primitives/           # reference (streaming) + _vec (vectorized) + verify
    cost/                 # IBKR commission + fill (Rust fill_model.rs ports)
    metrics/rust.py       # results.rs parity port
    metrics/full.py       # trade stats, MFE/MAE, positive-PnL-by-N
    metrics/dynamic.py    # forward-bps / EoD / conditional-on-score
    engine/               # contract, fast (screening), thorough (parity)
    study/                # DSL + scaffold + validate + render
    parity.py             # Rust-golden harness
    cli.py                # bash entrypoints
  tests/
```

---

## Modules

### `io.py` — data, `Bar`, `Universe`, `Timeframe`

Loads `raw_bars.parquet` (`ts_epoch_ms | asset | OHLCV`, 5-min, 78/day) into a
long frame, attaches ET-local columns, and pivots to cached **wide grids**
indexed `(et_date, minutes_since_open)` × asset.

- `Bar` — frozen dataclass, the atomic unit (mirrors a raw_bars row and the Rust
  `AggBar`).
- `Universe` — `.grid_close/.open/.high/.low/.volume/.dollar_vol/.logret`; the
  whole-indicator stack vectorizes over one `(n_days×78, n_assets)` panel.
- `Timeframe` (`minutes(n)/hourly/daily/weekly/monthly/yearly`) — **calendar-
  boundary** resampling, not bar-count (same semantics in Rust `rolling_fn.rs`).
- `Universe.resample(tf)` — returns a coarser `Universe`, so features are
  timeframe-agnostic.

### `primitives/` — rolling functions

The Rust `rolling_fn.rs` helpers, in two same-shape surfaces:

- `reference` — streaming classes (`RollingMean/Std/ZScore/Max/Min/Sum/Roc/EwmMean/
  RankPct/DayVwap/Beta/FracDiff` + `RollingAggregator`), mirroring Rust's
  `push`/`replace_last`/`value`, `None` until full, Rust math in docstrings.
- `_vec` — vectorized twins (`rolling_mean`, `rolling_z`, `ema`, `session_vwap`,
  `fracdiff`, `aggregate_bars`, …).
- `verify` — proves the two agree and are causal (`20/20`).

Parity traps baked in: std = sample `ddof=1`; EWMA `alpha=2/(span+1)`
`adjust=False`; rank-pct = count(values ≤ current)/window; VWAP = close-weighted.

**Anti-steering**: `reference` is a *verification spec, not a menu* — invent
features freely; the valve is `verify.check_causal`, not the import list.

### `cost/` — IBKR commission + fill

Verbatim ports of `fill_model.rs`: `CommissionModel.Fixed/Tiered` +
`commission(qty, price, model)` (1% value cap), `apply_slippage`, `decide_fill`
(market = close±slippage signed; limit = fill-on-cross at limit), and the
`FillModel` family (market/mid/limit/stop-market/stop-limit).

### `metrics/` — three layers

- **`rust.py`** — the *parity* port of `results.rs`: **population**-variance,
  **arithmetic**-return Sharpe/Sortino; max-DD `(peak−eq)/peak·100`; per-trade
  PnL via running avg-cost (fees on sells only). Validated vs real Rust JSON to
  ~1e-14.
- **`full.py`** — the *research* layer: FIFO round-trip matching, win
  rate/PnL/skew/kurt/streaks/hold/MFE/MAE, `positive_pnl_by_n` (bars + days),
  `portfolio_risk` (Calmar/VaR/CVaR/tail).
- **`dynamic.py`** — *interpretable output goals*: `forward_bps` (N bars / N
  days / **EoD**), `mean_bps_to_eod`, `conditional_by_score` (score buckets →
  realized bps). Answers "what does a prediction buy, in execution terms".

### `engine/` — the backtester

- `contract.py` — `Order`, `Outcome` (`no_action`/`set_targets`/`emit_orders`),
  `Clock`, `Context`, `EngineConfig`, `Strategy` (`on_row(bars, ctx) -> Outcome`).
- `fast.py` — vectorized screening (`pos[t]=signal[t−1]`); NOT the parity target.
- `thorough.py` — event/replay matching `reconcile.rs`/`fill_model.rs`:
  resting-order resolution, market@close±slippage, limit/stop/stop-limit,
  EOD close, `commission()`, FX, per-bar equity snapshots, `compute_results`.

Fast = screening; thorough = the number you believe. `starting_equity` everywhere.

### `study/` — traceable research

Features → Signal → Execution → Validation, enforced by the DSL. `feature`/
`signal_rule`/`execution_model`/`validation_check` each carry `**Why:**` +
**Alternatives** + **Assumptions**; `scaffold()` writes a correctly-labelled
`run.py` (jupytext-percent); `validate()` gates the labels; `render` produces a
`.ipynb`.

### `parity.py` + `cli.py`

`parity.py` — `RustGolden` loader, `metric_parity_problems` (reproduce metrics
from the golden's equity curve to machine precision), `full_parity_problems`
(exact trade count, equity ≤1bp, Sharpe ≤0.01).

`cli.py` — `verify`, `backtest`, `resample`, `study new|validate|render`,
`parity`; exit `0`/`1`; versioned JSON (`schema_version: 1.0`).

---

## Cross-cutting decisions (and why)

- **Pure Python, no Rust bridge.** You asked for it; a bridge is a possible
  *later* accelerator (see below), but the contract is "no build".
- **Two same-shape surfaces** (streaming `reference` vs vectorized `_vec`).
  Streaming is the readable spec; vectorized is the fast path; `verify` proves
  equality so the fast path is trustworthy.
- **`rust.py` differs deliberately from the old `stratkit`** (which used ddof=1
  log-return annualization). The parity metric uses population variance +
  arithmetic returns because that's what `results.rs` does.
- **Calendar-boundary `Timeframe`**, not bar count (your correction) — the same
  in Python and Rust.
- **Thorough = parity, fast = screening** (your two-mode requirement).
- **Study DSL enforces the "why" capture** (your traceability requirement).

---

## How to use it (the workflow)

1. `stratlab verify` — the harness is green before you trust anything.
2. `stratlab study new <slug>` → fill `run.py` (each phase with a *why*);
   `stratlab study validate …` gates the labels; `stratlab study render …`
   produces the notebook.
3. Build features with `primitives._vec`; `verify.check_causal` for anything new;
   resample with `uni.resample(Timeframe.minutes(15))` for other horizons.
4. `stratlab backtest --strategy s.py:Class --universe raw_bars.parquet --full-stats`
   → trace the trades, `positive_pnl_by_n`, `conditional_by_score`.
5. `stratlab parity` → pin the metric math to a Rust golden; for full parity,
   port the `on_row` logic to `src/strategy/*.rs` and diff via
   `full_parity_problems` + the exec-parity scripts.

---

## Gaps / limitations

- Full VP-touch trade-level parity is not yet run (needs the Rust backtest
  swapped to the pruned-12 *honest* model + a transaction dump).
- No Rust bridge (by design); porting a strategy to Rust is manual but
  machine-checked.
- Fast mode is an API (`backtest_fast(signal, uni)`), not a CLI flag.
- `conditional_by_score` needs per-entry scores from your strategy (API, not CLI).
- `MFE/MAE` use intrabar high/low; `forward_bps` uses close-to-close (two
  documented conventions).
- Thorough mode is O(bars) in Python; the Rust backtester is the fast path.

---

## Rust bridge (planned, not built)

Possible as an **optional PyO3/maturin accelerator** over the pure numerics
(`rolling_fn.rs`, `commission`/`decide_fill`, `stats_math.rs`) — extracted into
a small, dependency-free Rust crate and exposed as `stratlab._native`, with a
pure-Python fallback. Benefits: bit-exact primitives by construction + speed.
Constraints: must extract f64 leaf functions (some current helpers use
`rust_decimal`/`HistoricalDataFullKeys`), a compiled wheel per platform, and it
does *not* replace the "port the strategy to Rust" step — the strategy loop
stays Python for exploration. See the design discussion in the repo history.
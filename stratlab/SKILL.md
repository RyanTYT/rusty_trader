# stratlab — contract-first research + backtesting toolkit

`stratlab` develops and backtests strategies against `raw_bars.parquet`,
parity-ported from the Rust trading-app backtester
(`trading/trading-app/src/backtester` + `strategy/helpers/rolling_fn.rs`).

Run from the repo root (Pure Python, no build):
```bash
cd rusty_trader
PYTHONPATH=stratlab .venv/bin/python -m stratlab.cli verify
# or: stratlab verify   (after `pip install -e stratlab`)
```

## Data

`raw_bars.parquet` (in `data/backtest-results/stats/`) is the single input:
`ts_epoch_ms (i64, UTC ms) | asset | open | high | low | close | volume (f64)`,
5-min bars, 78/day, 9:30–16:00 ET.

## Non-negotiable invariants (apply to EVERY phase)

1. **Causality** — every rolling/aggregate/feature is causal (output at `t` from
   inputs `<= t`). Rolling std/z must trail; a static std inflates Sharpe 5–10×.
2. **No same-bar look-ahead** — `signal[t]` acts on bar `t+1`; any confirmation
   uses a signal observable strictly before entry; event selection uses the
   FIRST event, never "the best bar in the group".
3. **Anti-steering** — `primitives.reference` is a *verification spec*, not a
   menu. Invent features freely; the valve is `verify`, not the import list.
4. **Parity** — numerics must match the Rust backtester (`stratlab.metrics.rust`
   + `stratlab.cost`); acceptance gate = exact trade count, equity ≤ ~1bp,
   Sharpe ≤ ~0.01 vs a committed golden.

Research follows **Features → Signal → Execution → Validation**, each captured
with a *reason* in a `study/` (see `playbooks/validation.md`).

## Routing — load the playbook for the phase you're in

| phase | playbook |
|---|---|
| starting / growing a study | `playbooks/study-first.md` |
| building/verifying features | `playbooks/features.md` |
| combining features → signal | `playbooks/signal.md` |
| orders, fills, cost, timing | `playbooks/execution.md` |
| H1/H2, walk-forward, robustness | `playbooks/validation.md` |
| hunting look-ahead / overfit | `playbooks/bias.md` |
| matching the Rust backtester | `playbooks/parity.md` |

## Where does new code go?

| You're adding... | it goes in... | how |
|---|---|---|
| a research question / experiment / sweep / diagnostic | `studies/<slug>/run.py` (a cell) | `stratlab study append <slug> --question "..."` |
| a one-off diagnostic / analysis | `studies/<slug>/run.py` (a cell) | `stratlab study append` |

See `playbooks/study-first.md` for the full workflow. `run.py` is the source;
`run.ipynb` is rendered output.

## CLI

```bash
stratlab verify                         # causality + parity (20 checks)
stratlab backtest --strategy s.py:Class --universe raw_bars.parquet \
                  --start-equity 100000 --slippage-bps 2 --commission tiered --out r.json
stratlab resample --universe raw_bars.parquet --timeframe 15m --out bars15.parquet
stratlab study new <slug> --title "..."
stratlab study validate <slug>/run.py
stratlab parity [--golden backtest_results.json]
```

The `study` DSL (`stratlab.study`): `feature`/`signal_rule`/`execution_model`/
`validation_check` each carry `**Why:**` + **Alternatives** + **Assumptions**;
`validate()` gates the labels.
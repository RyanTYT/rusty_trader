# Execution modeling

How a decision becomes a fill, and how much that costs. Parity source:
`trading/trading-app/src/backtester/execution/fill_model.rs` + `reconcile.rs`.

## Order types

`stratlab.engine.Order(symbol, is_buy, qty, order_type=..., limit_price, stop, ...)`.
Order types: `MKT` (market), `LMT` (limit), `STP` (market stop), `STP`+limit
(stop-limit), and midprice. Bracket linking uses `references_parent_order`
(entry → arm exits, cancel siblings).

## Fills

- **market**: close ± `slippage_bps/1e4`, quantity signed (+buy/−sell).
- **limit**: fills iff the bar's `[low, high]` crosses the limit, **at** the
  limit (no slippage) — rests until crossed.
- **stop / stop-limit**: breach + (market) `stop ± pct_of_bar·(high−low)`, or
  (stop-limit) trigger then rest one more bar then fill on a later cross.
- **EOD** (`minutes_since_open >= 385`): force-close open positions at close
  (or mid).

## Cost

`stratlab.cost` — `CommissionModel.Fixed/Tiered` + `commission(qty, price, model)`
(1% trade-value cap) + `apply_slippage`. Tiers: Fixed $0.005/sh, min $1; Tiered
$0.0035/sh, min $0.35, + regulatory/clearing/pass-through.

## Timing / FX

Each fill settles against `CASH:SGD` via `fx_to_sgd(currency)` (USD→SGD 1.2713
default). The thorough engine produces per-bar equity snapshots
(`cash + mark-to-market positions`).

## The thorough engine is the parity path

```python
from stratlab.engine import run_thorough, EngineConfig, Outcome
res = run_thorough(uni, strategy, EngineConfig(starting_equity=..., slippage_bps=..., commission_model=...))
```

`strategy.on_row(bars, ctx) -> Outcome`. Fast/screening mode is a separate API
(`backtest_fast(signal, uni)`), not the parity target.

## Record

`execution_model()` in the study DSL: order type, timing, fill + cost model, and
why — plus alternatives (market vs limit, midpoint vs close) and assumptions
(queue, fill-on-cross, no partial fills).
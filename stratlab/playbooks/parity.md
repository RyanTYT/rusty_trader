# Rust parity + portability

Validate a Python strategy against the Rust backtester to identical results.

## The parity contract (numerics that must match exactly)

| thing | Rust source | Python rule |
|---|---|---|
| Sharpe/Sortino | `results.rs::sharpe_sortino` | **population** variance + **arith** `(cur-prev)/prev` |
| max drawdown | `results.rs` | `(peak - eq)/peak * 100` (positive %) |
| per-trade PnL | `realized_per_trade_pnl` | running avg-cost; fees on sells only |
| market fill | `decide_fill` | `close ± slippage_bps/1e4`, signed qty |
| limit fill | `decide_fill` | fill iff range crosses limit, at limit |
| commission | `commission()` | `CommissionModel.Fixed/Tiered`, 1% value cap |
| rolling std | `RollingStd` | sample (ddof=1) |
| EWMA | `EwmMean` | alpha=2/(span+1), adjust=False |
| VWAP | `RollingDayVwap` | close-weighted (not typical price) |

## Acceptance gate

**Exact trade count, end-equity within ~1bp, Sharpe within ~0.01** vs a committed
Rust JSON golden. Looser than that = a fill-semantics bug, not "close enough".

## The harness

`stratlab.parity`:
- `metric_parity_problems(golden)` — reproduce max DD / Sharpe / Sortino / scalar
  from the golden's equity curve, to machine precision (validated ~1e-14).
- `full_parity_problems(golden, py_transactions, py_equity_curve)` — the trade
  count + equity gate (needs the Rust transaction dump; the results JSON has
  scalar counts only).

```bash
stratlab parity [--golden backtest_results_vp_touch.json]
```

## Portability loop

1. Write the strategy once in Python (`on_row`) and backtest (`run_thorough`).
2. Port it to `trading/trading-app/src/strategy/*.rs` (`on_bar_update` →
   `BarUpdateOutcome`); the component fixtures (`vp_touch_*_parity.rs` +
   `research/cand_vp_out/parity_fixture.json`) already diff `compute_features` /
   `VolumeProfile::from_bars` against Python.
3. Diff equity/trades via `full_parity_problems`.

The `RollingAggregator` exists in both languages (`Timeframe`/`BucketKey` in
`rolling_fn.rs`; `stratlab.io.Timeframe`) with identical calendar-boundary
semantics.
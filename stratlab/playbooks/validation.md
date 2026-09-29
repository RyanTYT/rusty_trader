# Validation process

Prove the edge is real, not mined. (For the *specific ways it goes wrong*, see
`playbooks/bias.md`.)

## The discipline

1. **H1/H2 chronological split** — fit/select on the first half, report the
   second half. `Universe.split_dates(frac=0.5)`.
2. **Walk-forward** — train/validate/test in rolling windows; report OOS only.
3. **Cost reality** — report NET of the IBKR model (`stratlab.cost`). An edge
   that needs ≥ ~10bp/trade net to clear cost usually doesn't.
4. **Robustness** — vary slippage/commission up, test capacity (does the edge
   concentrate in low-volume regimes?), param sensitivity (does the result hold
   across nearby params?).

## Metrics — two layers

- `stratlab.metrics.rust` — the parity port (population Sharpe/Sortino, max DD,
  per-trade PnL). The number you believe against Rust.
- `stratlab.metrics.full` — research stats: win rate, PnL distribution,
  skew/kurt, streaks, hold time, MFE/MAE, positive-PnL-by-N, portfolio risk.
- `stratlab.metrics.dynamic` — **interpretable output goals**: forward bps move
  by horizon & to EoD, and conditioned on the model score. Read the result the
  way you'd act on it ("enter → mean +Xbp by EoD, P(win)=Y%").

## Gate

Run `stratlab verify` before trusting anything, and `stratlab parity` to pin the
metric math to the Rust golden.

## Study lifecycle

`hypothesis → in-progress → validated → rejected → promoted-to-production`.
`promoted-to-production` links the study to the Rust strategy + its parity golden.
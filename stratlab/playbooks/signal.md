# Signal construction

Turn features into a decision: a target position, or an order.

## The timing contract

- `signal[t]` is computed from info `<= t` and **acts on bar `t+1`**. In the
  vectorized fast engine this is `pos[t] = signal[t-1]`.
- In the thorough engine the strategy sees bar `t`'s OHLCV and returns
  `Outcome.set_targets(...)` / `Outcome.emit_orders(...)`; market fills are at
  bar `t`'s **close** ± slippage (the Rust same-bar close convention). If you
  must not peek same-bar, base the signal on prior bars and treat `t`'s close
  only as the fill price.

## Cross-sectional signals

`xsec_rank`, `xsec_z` act along `axis=1` (contemporaneous across assets) — no
time leak. A cross-sectional strategy can return targets for multiple symbols in
one `on_row(bars, ctx)`.

## Thresholding / model outputs

Prefer **dynamic, horizon-indexed targets** over a single static label. Evaluate
a directional prediction against the forward bps move at multiple horizons and
to End-of-Day (mean bps to EoD, P(win), median) — see `stratlab.metrics.dynamic`
— and condition that curve on the model's score/confidence to show how a decision
maps to realized move. This is what makes an output *understandable* in execution
terms.

## Avoid

- same-bar confirmation: entry@N open ⇒ confirm via `close[N-1]`, never `close[N]`.
- selecting the "best" bar/event from a group using future info — use the FIRST.
- a static/full-sample std for any z-threshold.

## Record

`signal_rule()` in the study DSL: name, rule, thresholds, and the **Why** +
alternatives + assumptions.
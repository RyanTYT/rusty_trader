# Bias / look-ahead hunting

The audit checklist. Each entry is a way a "great" result turns out to be an
artifact. This repo has burned on most of these.

## Look-ahead (the sharpest knife)

- **Static/full-sample std** for a z-threshold → Sharpe inflated 5–10×. Use a
  causal rolling std.
- **Same-bar confirmation**: filter at bar `t+1` using `close[t+1]` while
  entering at `open[t+1]` → you selected trades already in your favor. Enter
  AFTER the confirmation is observable (entry@t+2, confirm via `close[t+1]/close[t]`).
- **Episode/group representative selection**: picking the "best" touch/event in a
  group using info from ALL bars in the group (incl. future) → look-ahead. Use
  the FIRST event (real-time cooldown).
- **Centered windows / future smoothing** anywhere.
- **Support / existence leak** — a derived feature left `NaN` (or imputed to one
  constant) wherever a *forward* label is absent, e.g. a stage-1 output computed
  on `d[isfinite(forward_label)]` and then handed to stage 2 as a *feature*. The
  model splits on the missing block; "this level gets no volume for the next N
  sessions" becomes a usable input. **The embargo does not stop it** — an embargo
  protects the *training label*, not the feature's missingness on the *scored*
  rows. Detection: rank each feature's NaN-pattern against the target (if it
  ranks, it leaks), then ablate — impute with a real causal value; if the edge
  dies, the edge was the block. Full procedure: `feature-panel-audit` §7A.
- **A metric that improves when you add a bug.** If agreement with a reference
  *rises* after a change, check whether the change imported the reference's leak
  rather than fixed yours. Agreement is not validity.

## Selection / overfitting

- Fit on the full sample then test on it → in-sample Sharpe is meaningless.
- Selecting params/assets/direction on the same window you report → H1/H2 or
  walk-forward OOS only.
- Data snooping (tried 300 variants, kept the winner) → deflate: use a holdout,
  walk-forward, or a PBO/deflated-Sharpe estimate.
- Survivorship / universe cherry-picking (excluding names *because* they lost).

## Cost/timing honesty

- Count entry AND exit as two sides (no double-count, but don't under-count).
- Fill at a realistic price (close±slippage market; crossing limit) — not open
  mid-bar when the signal used that bar's close.
- FX/currency settlement consistent (see `playbooks/execution.md`).

## Cross-check

- `stratlab verify` (causality + streaming/vec).
- `stratlab parity` (metric math vs Rust golden).
- Re-run the stratlab path and the RS strategy in Rust and diff (the portability
  loop in `playbooks/parity.md`).

If a result feels too good, suspect one of the above before you suspect the edge.
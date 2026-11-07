# Promotion — study → runnable Python ↔ Rust twin

A study result (`studies/<slug>/`) is research, not a strategy. **Fully
promoted** means the strategy runs standalone through the serial,
completely causal loop in Python, its Rust twin is committed, and both are
verified for parity by committed tests. Promotion is a *gate*, not a grad
school — an unpromoted book, however rich, must still be run from its
study folder.

## Fully promoted = directly rust-portable (mandatory criterion)

**Ruling 2026-11-06: for a strategy to be fully promoted, it must conform
to the rust engine trait logics and not rely on frozen maps — everything
live-composed — so it is directly portable to the rust engine.**

Concretely, beyond the gates below, a fully promoted strategy must:

- **Conform to the rust engine trait shape** — the strategy path is
  expressible as the engine's native Strategy/Gate mechanics (features
  from arriving bars, GateLogic-style decisions, sizing from booked
  equity), not a python-only harness that the engine cannot seat.
- **No frozen maps at runtime** — the live path must not read back
  precomputed/battery artifacts (weight grids, offline score dumps,
  frozen fill books). Eagle-oracle artifacts may exist as TEST
  EVIDENCE only (`#[cfg(test)]` / unit fixtures); nothing on the live
  path loads them.
- **Everything live-composed** — all weights, gates, severity/fold
  states compose on-line from the strategy's own traded data as bars
  arrive (streaming fit, warm-up seeding, causal running fills). If a
  component cannot compose live, it is not promotable until it can.

A strategy whose results depended on frozen-map replay is, at full
promotion, a research pin — not a live book — until re-specified to
compose everything on-line (precedent: blended5's suite — severity
quintiles, connors sigc_w, tc1 tag-on gates — all compose live in-process).

## PROMOTION.md is the single status doc (no separate variant files)

The per-strategy `PROMOTION.md` carries THREE things and nothing spawns a
sibling markdown:

1. the original promotion evidence (gates, parity deltas, golden JSON),
2. the **design/architecture outline** of the strategy as it now stands
   (sleeves/chains, engine variant, seams, universe, fill semantics),
3. the **continuously most-updated pinned promoted results** — when a
   new engine variant, ladder, or ruling lands, UPDATE `PROMOTION.md`
   in place (new section, clearly dated, superseding prior pins).

Do NOT create separate variant/status markdowns (`*_VARIANT.md`,
`*_STATUS.md`, `*_PLAN.md`) inside the promoted folder — they rot, get
superseded silently, and fragment the record. If one exists, fold its
still-true content into `PROMOTION.md` and delete it.


## The promoted folder

`promoted/<slug>/` — one directory per promoted strategy, self-contained:

| file | must contain |
|---|---|
| `strategy.py` | the **full runnable strategy**: causal features, a `Strategy` (`on_row`) driving the serial causal loop, the `EngineConfig` used (starting equity per `reporting.md` — 100K base unless the user specified otherwise — plus slippage/commission), a `backtest(uni)` entry point, and the `STUDY` DSL rationale. Imports only `stratlab.engine` / `primitives` / `cost` / `io` — **never** `strategies/<one-off>.py`; the promoted copy must run without the rest of `strategies/`. |
| `test_<slug>.py` | engine tests that drive `run_thorough` end-to-end on a small committed fixture (the serial completely causal loop): enter/exit/fill/bracket semantics asserted bar-by-bar, plus a causality check (every feature at `t` reproducible from inputs `<= t`) and the NaN/support audit from `feature-panel-audit` §7A where the strategy uses forward labels. |
| `strategy.rs` | the **full Rust twin** for `trading/trading-app/src/strategy/<slug>.rs` — same features, same signals, same fill semantics. |
| `PROMOTION.md` | the evidence record: gates run and their numbers, engine config, the golden JSON name, parity deltas (trade count / end-equity / Sharpe), source study slug + date. |

## Promotion gates

1. **Research clean** — `stratlab study validate <source-study>` passes;
   the source study's Validation section answers H1/H2 + cost questions.
2. **Serial causal loop** — `run_thorough(uni, strategy.on_row, config)`
   replays the full universe bar-by-bar to completion with no vectorized
   shortcut, no `.shift(-k)` anywhere in the strategy path.
3. **Causality + parity suite** — `stratlab verify` passes (20 checks);
   engineered features audit as strictly causal.
4. **Rust parity** — the twin is committed in `trading/trading-app`
   (add `pub mod <slug>;` + the `.rs` file in `strategy/`), and
   `stratlab parity --golden <json>` clears the acceptance gate:
   **exact trade count, end-equity ≤ ~1bp, Sharpe ≤ ~0.01** vs the
   committed Rust golden. `strategy.rs` in the promoted folder is
   byte-identical to the file under `trading-app/src/strategy/` — where
   it is not, `PROMOTION.md` records which commit each matches.
5. **Tests pass solo** — `pytest promoted/<slug>` runs green from a clean
   checkout (fixture committed, no dependency on external data paths).

## Conventions

- `slug` matches the Rust module name (`snake_case`, no version suffix).
- `PROMOTION.md` quotes the book with the full reporting column set
  (`playbooks/reporting.md`): CAGR | Sharpe | Sortino | MaxDD | win | PF |
  net bps | base capital — Tiered AND Fixed for cost-sensitive books.
- Demotion is a normal event: if parity breaks or a test fails, the
  folder gets a `DEMOTE` note in `PROMOTION.md` until re-promotion, and
  the routing table/README marks it.
- Folders in the **index** (`promoted/README.md`) as they are promoted.

## What does NOT get promoted

- A vectorized-only signal with no `on_row` form — the serial causal loop
  is the promotion requirement.
- A book assembled post-hoc (caps, overlays, sizing hacks) with no single
  strategy spec — promote the spec, not the aftertaste.
- A strategy still under H1-only fit or failing cost reality
  (`validation.md`) — fix the spec, then promote the result.

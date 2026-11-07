# Reporting — how numbers are quoted

Numbers shown to a decision-maker must be complete and consistent-capital.
This playbook enforces the *format* of every reported result; `bias.md`
enforces what makes the numbers legitimate in the first place.

## The quote contract (enforced in every table, bullet, and summary)

1. **Sharpe is never alone.** Every table row, README bullet, and
   user-facing summary pairs Sharpe with **Sortino — both annualised** —
   even when the table gets wider. `metrics.met()` / `pretty()` already
   carry both; do not trim them.
2. **The full column set** for any backtest book:
   `CAGR | Sharpe | Sortino | MaxDD | win % | PF | net bps/trade`
   where CAGR is 252d and annualised on the run's **own session count**.
   PnL in currency or equity-curve screenshots are supplements, not
   substitutes for the columns.
3. **Net is the default; gross is labelled.** Default cost model is
   Tiered + stated slippage bps. A number that omits cost treatment reads
   as net; calling a gross number without an explicit `gross` tag is an
   error. Cost-sensitive books report **Tiered AND Fixed** side by side
   (minimums can flip the sign).
4. **Sub-period metrics are not additive.** Sharpe/Sortino are computed on
   per-bar returns and window-dependent; H1/H2 sub-runs are also
   cold-start (fresh SMA warmup). Quote them per period, never sum or
   average them into the full-period figure — pad the window if the gate
   needs a fair comparison.

## Capital base — 100,000 unless the user says otherwise

- **`starting_equity` defaults to 100,000 — always.** `EngineConfig` and
  `backtest --start-equity` stay at the 100K base unless the user
  explicitly specified a different figure.
- If the user specifies another base (e.g. a 25k notional sleeve), quote
  it **with the book**: currency figures (PnL, equity, DD in $, notional)
  are base-dependent; ratio figures (Sharpe/Sortino, CAGR, DD %, win,
  PF, bps) are base-invariant and still quoted normally.
- Never switch the base between two runs to flatter a drawdown or a PnL.
  Two books compared side-by-side share one base, stated once.

## Style rules

5. **Deployment-matched comparison (user rule, 30 Oct 2026).** Never quote a
   low-deployment book side-by-side with a max-leverage book of the same
   strategy family (e.g. py MEW u173 m0.8, u_eod 0.58, next to connw u173mkt
   drift whose u_eod rides ~1.8). Quote MEW at the m that maximises leverage
   (matches the target book's deployment) or state plainly that the two books
   differ in deployment and cannot be compared.

- Rule out silent unit flips: SL/TP in bps vs points, risk per trade as
  % of equity vs $ fixed — say which one was used.
- A table caption or first bullet states the engine config once
  (`run_thorough-fast`, slippage, commission, universe, period) — not
  repeated per column.
- Quote splits (e.g. 2021–23 vs 2024+) alongside the full period; a
  full-period Sharpe with a failing H2 is reported as-is, not hidden.

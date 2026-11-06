"""Shared contracts for the backtest engine.

A strategy is a stateful object with one method::

    def on_row(self, bars: dict[str, Bar], ctx: Context) -> Outcome

called once per timestamp with the full cross-sectional row, returning a
`BarUpdateOutcome`-like `Outcome` (`NoAction`, `SetTargets`, or `EmitOrders`).
`EmitOrders` carries a flat list of `Order`s — a bracket parent (entry) plus
OCA children (stop / take-profit / close), exactly as the Rust `OrderIBKR`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from stratlab.cost import CommissionModel
from stratlab.io import Bar, ts_ms_to_et_date, ts_ms_to_mso

# Order type strings (match Rust ibapi `order_type`).
MKT = "MKT"               # market — fills immediately at close +/- slippage
LMT = "LMT"               # limit — rests until crossed (entry, or take-profit)
STP = "STP"               # stop (with limit_price => stop-limit, else market-stop)
MIDPRICE = "MIDPRICE"     # time-gated close — fills at bar CLOSE (Rust quirk, see below)
MIDCLOSE = "MIDCLOSE"     # time-gated close — fills at MID (Rust quirk, see bracket.rs)

# Fixed FX rates to SGD — verbatim port of Rust `default_fx_map` in
# `backtester/oracle/price_supplier.rs` (exchange-rates.org ~2026-08-26).
FIXED_FX_TO_SGD: dict[str, float] = {
    "USD": 1.2713, "EUR": 1.4819, "GBP": 1.7287, "JPY": 0.007983, "CNY": 0.1891,
    "HKD": 0.1622, "AUD": 0.9132, "NZD": 0.7564, "CAD": 0.9163, "CHF": 1.5794,
    "SEK": 0.1335, "NOK": 0.1360, "DKK": 0.1982, "PLN": 0.3437, "CZK": 0.06142,
    "HUF": 0.004092, "RON": 0.2819, "BGN": 0.7577, "RUB": 0.01506, "TRY": 0.02641,
    "ILS": 0.4267, "AED": 0.3462, "SAR": 0.3383, "QAR": 0.3488, "KWD": 4.1182,
    "BHD": 3.3724, "OMR": 3.3066, "JOD": 1.7932,
    "EGP": 0.02532, "ZAR": 0.07975, "NGN": 0.0009374, "KES": 0.009823,
    "GHS": 0.1136, "MAD": 0.1375, "DZD": 0.009554, "TND": 0.4386,
    "INR": 0.01332, "IDR": 0.00007154, "MYR": 0.3158, "THB": 0.03877,
    "PHP": 0.02063, "VND": 0.00004870, "KRW": 0.0009185, "TWD": 0.03995,
    "PKR": 0.004579, "BDT": 0.01035, "LKR": 0.003870, "NPR": 0.008333,
    "KHR": 0.0003143, "MMK": 0.0006055, "BND": 1.0011, "MOP": 0.1575, "SGD": 1.0,
    "MXN": 0.07502, "BRL": 0.2469, "ARS": 0.0008396, "CLP": 0.001382,
    "COP": 0.0004109, "PEN": 0.3793, "UYU": 0.03163,
}


def fx_to_sgd(currency: str) -> float:
    """units of SGD per 1 unit of `currency` (1.0 for unknown/SGD)."""
    return FIXED_FX_TO_SGD.get(currency, 1.0)


def good_after_time_to_mso(s: str) -> int | None:
    """Parse Rust `good_after_time` ("YYYYMMDD-HH:MM:SS") → minutes_since_open."""
    if not s:
        return None
    parts = s.replace(" ", "-").split("-")
    if len(parts) < 2:
        return None
    tp = parts[1].split(":")
    if len(tp) < 2:
        return None
    h, m = int(tp[0]), int(tp[1])
    return (h * 60 + m) - 570


@dataclass(frozen=True)
class Order:
    """One emitted order (mirrors Rust `OrderIBKR`). A bracket parent has
    `references_parent_order == -1`; its children reference the parent's index
    in the `EmitOrders` list."""

    symbol: str
    is_buy: bool
    qty: float
    order_type: str = MKT
    limit_price: float | None = None   # LMT price, or STP limit (=> stop-limit)
    stop: float | None = None          # aux_price — STP trigger
    close_mso: int | None = None       # time-gated close trigger (minutes since open)
    close_is_mid: bool = False         # True = fill at mid, False = fill at close
    references_parent_order: int = -1  # -1 = entry (bracket parent)
    oca_group: str = ""
    # Earliest bar timestamp (epoch ms) at which this order may act. Before it,
    # the order is inert: it is neither evaluated nor filled, and it survives to
    # the next bar still resting. Mirrors IBKR's ``activeStartTime``.
    #
    # This is the general form of a time-based exit. A bracket whose child is a
    # MIDCLOSE with ``active_after = entry_ts + hold`` closes itself, so the
    # strategy no longer has to count bars per lot -- and cannot drift out of
    # step with the feed while doing it. Unlike ``close_mso``, which is an
    # ABSOLUTE minutes-since-open gate, this is measured from a chosen instant,
    # so it survives across sessions if the caller wants it to.
    #
    # Applies to ANY order type, not just closes: a stop can be armed after a
    # delay, a limit can be placed later. Once armed it behaves exactly as its
    # ``order_type`` says.
    active_after: float | None = None
    # ── per-order attribution + mixed-fill routing (Blended5Fund order
    # adapter, 2026-10-29) ────────────────────────────────────────────────
    # Emitter tag (sleeve name). The engine copies it onto the RestingOrder
    # and onto every fill it produces, so a strategy/fund adapter can
    # attribute fills exactly per tag and diff desired vs (position + its
    # own open orders). None = unattributed (legacy emitters unchanged).
    order_ref: str | None = None
    # Prints-remaining before an unfilled resting ENTRY limit falls back to
    # market at that print's close + adverse slippage (mirrors the
    # set_targets "limit_then_market" semantics, but carried ON the order --
    # per-order, leaving engine-global EngineConfig.entry_fill_mode
    # untouched). Decrement consumes one symbol-PRINT (non-printing bars do
    # not age the order; survives overnight gaps). None = no TTL: the limit
    # rests until crossed, EOD force-close, or cancel.
    ttl: int | None = None
    # Cancel request: the engine removes the RESTING entry order matching
    # (order_ref, symbol) instead of placing anything. Setting cancel on an
    # order whose type/price/qty are placeholders (qty 0) is the convention.
    cancel: bool = False


class Outcome:
    """What a strategy returns from `on_row`."""

    __slots__ = ("kind", "targets", "orders")

    def __init__(self, kind: str, targets: dict[str, float] | None = None,
                 orders: list[Order] | None = None):
        self.kind = kind
        self.targets = targets
        self.orders = orders

    @classmethod
    def no_action(cls) -> "Outcome":
        return cls("no_action")

    @classmethod
    def set_targets(cls, targets: dict[str, float]) -> "Outcome":
        return cls("set_targets", targets=targets)

    @classmethod
    def emit_orders(cls, orders: list[Order]) -> "Outcome":
        return cls("emit_orders", orders=list(orders))


@dataclass
class Clock:
    time_ms: int = 0
    mso: int = 0
    et_date: object = None

    def set(self, ms: int) -> None:
        self.time_ms = ms
        self.mso = ts_ms_to_mso(ms)
        self.et_date = ts_ms_to_et_date(ms)


@dataclass
class Context:
    clock: Clock
    bars: dict[str, Bar]
    #: Live account state, or None if the caller did not provide it. Set by
    #: ``run_thorough`` before every ``on_row``. Strategies that size off
    #: equity need this -- the engine does not gate orders on cash, so a
    #: strategy that issues a fixed share count can lever itself bust.
    account: "object | None" = None


class Strategy(Protocol):
    def on_row(self, bars: dict[str, Bar], ctx: Context) -> Outcome: ...


@dataclass
class EngineConfig:
    starting_equity: float = 100_000.0
    slippage_bps: float = 0.0
    commission_model: CommissionModel = CommissionModel.Tiered
    bar_minutes: int = 5
    currency: str = "USD"                     # fills/commissions are denominated here
    fx: Callable[[str], float] | None = None  # currency -> SGD (default: fixed map)
    #: Force-close still-open brackets at the session close (mso >= 385) and
    #: drop them.  `True` reproduces the Rust reference engine, where no
    #: inventory is carried overnight, and is the default because every other
    #: strategy in this repo was written against that guarantee.
    #:
    #: Set `False` for a strategy whose time exit legitimately spans sessions.
    #: Layer 4's `fill` walks `k_hold + 1` bars straight through session
    #: boundaries and overnight gaps -- it has no session concept at all -- so a
    #: session-bounded executor cannot reproduce it.  With this left `True`,
    #: every bracket is capped at `78 - entry_bar_within_session` bars: measured
    #: 0.0% of 24,428 notebook brackets reached the 120-bar hold, against
    #: 24.0% under the study's own fill.  The entries gate
    #: (`if not eod`) is deliberately NOT governed by this flag -- it concerns
    #: resting limit entries, not market ones.
    force_close_at_eod: bool = True

    # ── entry fill semantics (Blended5 limit-order study) ────────────────
    #: How ENTRY increments (flat->positioned or same-sign adds) fill.
    #: "market" (default) = the engine baseline, delta fills at this bar's
    #: close + adverse slippage. "limit" = entries rest as LIMIT orders
    #: offset k bp from the decision close; TTL counts SYMBOL-PRINTS and a
    #: never-crossed limit is CANCELLED (the leg never opens). BUY: limit =
    #: anchor*(1 - k*1e-4), fills iff bar.low <= limit at px = the LIMIT
    #: price (see `optimistic_limit_fill` for the gap-through open-fill
    #: variant); SELL mirrored on high. Exits/reductions are
    #: UNAFFECTED (market) so EOD cap enforcement keeps its teeth.
    #: "limit_then_market" = same resting limit, but on TTL exhaustion the
    #: remainder crosses to market at THAT print's close + slippage (no miss,
    #: mild adverse selection).
    #: "midprice" = the market path with every fill executed as an IBKR
    #: MIDPRICE peg: fills at the bar's CLOSE with NO slippage (the engine's
    #: documented MIDPRICE semantic, thorough.py `_process_resting`). Same
    #: targets/sizing as market mode — only the execution price differs.
    #: Default-off: behaviour is byte-identical to "market" except prices.
    entry_fill_mode: str = "market"
    entry_limit_k_bp: float = 1.0
    entry_limit_ttl: int = 3

    # ── resting-limit fill semantics on gap-throughs ──────────────────────
    #: False (DEFAULT, rust `LimitFillModel` parity) = a crossed resting limit
    #: fills AT THE LIMIT price even when the bar's open gapped through it
    #: (the conservative cap: never assume price improvement beyond the
    #: limit you placed). True = OPTIMISTIC/tape-truth form (the engine's
    #: historical behavior, kept for byte-parity with §14 hybrid artifacts):
    #: when the open gaps through the limit the fill executes at the OPEN
    #: (auction-clearing / price-time interpretation — fill at your limit in
    #: that case would price you above the actual print for buys, below it
    #: for sells). Causal either way; the difference is conservatism, not
    #: look-ahead.
    #: PIN POINTER (canonical notice = study/blended5_fund/FILE_AUDIT.md §17):
    #: ALL stored python hybrid results — the §14 ladder, the Oct-23
    #: `_fill_order_study_report` goldens, every `_fund_report_fund_hybrid_*`
    #: — were produced under `optimistic_limit_fill=True` (open-on-gap).
    #: Reproduce that lineage via the harness seam `LIMIT_FILL=optimistic`;
    #: the engine default is the at-limit (classic) form. Any comparison
    #: against a stored hybrid number MUST state which fill form it used.
    optimistic_limit_fill: bool = False

    # ── entry-only queue (E-fork; entry-delay-only experiments) ─────────
    #: 0 (default) = legacy same-bar reconcile of the whole basket. >0 = with
    #: fill_lag_bars=0, split targets: legs that INCREASE the position toward
    #: the target ride the pending queue for `lag_entries_bars` rows and
    #: settle at the flush row's close (anchor per entry_limit_anchor);
    #: reductions and flattens reconcile same-row as lag0. Only supported
    #: with fill_lag_bars=0 (guard in run_thorough).
    lag_entries_bars: int = 0

    # ── entry-limit anchor (lag1 look-ahead repair, R3 control) ──────────
    #: Which close anchors a resting ENTRY limit's price.
    #: "decision" (DEFAULT as of 2026-10-26, honest) = the close of the row
    #: whose decision produced the queued target (close(r)) — a price that
    #: EXISTS at placement time. A limit set from decision row N is first
    #: executable on row N+1's prints (the row any lag>=1 queue settles on);
    #: fills are never conditioned on the future.
    #: "settle" (LEGACY, kept only for byte-parity with stored goldens) = the
    #: settle row's own close — at fill_lag_bars=1 that is close(r+1), a
    #: price that does not exist when the row's open happens, so resting
    #: limits were priced off a bar's own close and filled against that
    #: bar's prints (intrabar look-ahead; gap-through share of lag1 buy $:
    #: study/vbo_intx results_gap_audit.json 25.5%, px med −43bp vs fbc).
    #: This default flip did NOT change golden lag0-family artifacts (at
    #: fill_lag_bars=0 both anchors coincide with the decision bar); lag1
    #: runs that must reproduce stored goldens pass "settle" explicitly.
    #: Only meaningful with lim entry modes; fill rows are unaffected.
    entry_limit_anchor: str = "decision"

    # ── target-fill lag (Rust replay parity) ─────────────────────────────
    #: How many bars AFTER a strategy emits `set_targets` the engine
    #: settles them. 1 (default) = Rust-replay parity: the Rust in-memory
    #: replayer's flush-on-advance semantics are fundamental there (the
    #: blended5_fund Rust twin flushes a row's targets when the NEXT row's
    #: first printed bar arrives, so bar-r targets fill at bar r+1's
    #: prices). The engine parks each `set_targets` basket in a queue and
    #: reconciles it at the start of the row `fill_lag_bars` rows later,
    #: BEFORE mark/ctx so the strategy still sizes against the settled
    #: book (fund ghost bookkeeping assumes this convergence).
    #: 0 = legacy same-bar fills — byte-exact with the pre-lag battery
    #: artifacts (pass explicitly for battery-parity reruns).
    fill_lag_bars: int = 1

    def fx_rate(self, currency: str) -> float:
        return fx_to_sgd(currency) if self.fx is None else self.fx(currency)


__all__ = [
    "MKT", "LMT", "STP", "MIDPRICE", "MIDCLOSE",
    "FIXED_FX_TO_SGD", "fx_to_sgd", "good_after_time_to_mso",
    "Order", "Outcome", "Clock", "Context", "Strategy", "EngineConfig",
]
"""Thorough (event/replay) backtest mode — the parity target.

Ports the Rust ``InMemoryReplay`` + ``reconcile.rs`` + ``bracket.rs`` semantics:

* bars are replayed in ascending time (one cross-sectional row per 5-min bar);
* `SetTargets` → delta → market fill at close ± slippage;
* `EmitOrders` → OCA brackets: a parent entry + stop / take-profit / close
  children. The entry fills (market immediate, or rests if limit); exits stay
  inactive until the entry fills; when one exit fills its OCA siblings cancel;
  EOD (`mso > 385`) force-closes active exits and drops the rest;
* fills use `decide_fill` / the `FillModel` family; commissions use
  `commission()`; cash + position marks settle with the fixed FX map
  (USD → SGD = 1.2713 etc.);
* equity is snapshotted per bar and results computed with
  `stratlab.metrics.rust.compute_results` (population Sharpe/Sortino, avg-cost
  per-trade PnL).
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from types import SimpleNamespace

from stratlab.cost import (
    BarExtremeFillModel,
    BarOhlc,
    LimitFillModel,
    MarketFillModel,
    StopLimitFillModel,
    StopMarketFillModel,
    apply_slippage,
    commission,
)
from stratlab.io import Bar
from stratlab.metrics import BacktestResults, compute_results

from .contract import (
    LMT,
    MIDCLOSE,
    MIDPRICE,
    STP,
    Clock,
    Context,
    EngineConfig,
    Order,
)

RESTING = "resting"
FILLED = "filled"
CANCELLED = "cancelled"


@dataclass
class RestingOrder:
    symbol: str
    is_buy: bool
    qty: float
    bracket_id: int
    role: str                # "entry" | "exit"
    active: bool
    fill_model: object | None  # None only for time-gated close
    close_mso: int | None
    close_is_mid: bool
    status: str = RESTING
    #: Earliest bar timestamp at which this order may act (``Order.active_after``).
    #: None = always active, which is every order's behaviour before this field
    #: existed -- so an existing strategy is unaffected.
    active_after: float | None = None
    #: Bar timestamp at which this bracket's entry filled. Recorded so a
    #: time-based child can be reasoned about (and so metrics can report a true
    #: hold time) without the strategy re-deriving it from its own bar counter.
    entry_filled_ms: int | None = None
    #: Emitter tag (``Order.order_ref``) — copied from the bracket parent (or
    #: child) so fills and open orders are attributable per strategy/sleeve.
    order_ref: str | None = None
    #: Prints-remaining before an unfilled resting ENTRY limit falls back to
    #: market at that print's close + adverse slippage (``Order.ttl``). Aged
    #: one print per symbol-print (a symbol that does not print does not
    #: consume TTL; limits survive overnight gaps). None = no TTL.
    ttl: int | None = None


class MarketState:
    """In-memory cash / positions / transactions / resting-order book."""

    def __init__(self, config: EngineConfig):
        self.config = config
        self.cash = config.starting_equity
        self.positions: dict[str, float] = {}
        #: Gross inventory per SIDE. `positions` is the net; these are not.
        #: See `_fill` -- exits are armed against these, because a hedged book
        #: has a net position of zero while both of its lots are genuinely open.
        self.long_qty: dict[str, float] = {}
        self.short_qty: dict[str, float] = {}
        self.transactions: list[tuple] = []
        self.fills: list[dict] = []          # rich fill log for full stats
        self.resting: list[RestingOrder] = []
        self.equity_curve: list[dict] = []
        self._bracket_seq = 0
        #: Last price seen for every symbol that has ever printed.
        #:
        #: REQUIRED, not an optimisation. A bar's ``prices`` map contains only
        #: the symbols that printed on THAT timestamp, so a position in a symbol
        #: absent from the current bar had no price to look up. The old code
        #: `continue`d, which valued the position at ZERO and dropped it out of
        #: equity entirely -- silently, with no marker. On a 40-asset run with a
        #: single 14,859-share position that removed ~$83k of value from the
        #: curve and made a profitable book (closed round-trips +$10,731) report
        #: as -$23,197: `final_equity` came out exactly equal to `cash`, which
        #: is arithmetically impossible while any position is open.
        #:
        #: Carrying the last known price is also the correct accounting: a
        #: position is still worth what it was worth when it last traded. This
        #: is what makes a symbol that stops printing -- delisting, a data gap,
        #: a halt -- read as "held at its last mark" instead of "worthless".
        self.last_price: dict[str, float] = {}
        #: Resting ENTRY limits (Blended5 limit-order study; unused unless
        #: EngineConfig.entry_fill_mode != "market"). Keyed by symbol: the
        #: engine converts target increments into passive orders keyed
        #: `symbol -> (is_buy, qty, limit_px, ttl_prints_left)`. Exits never
        #: rest, so cap-equalise trims stay immediate.
        self.pending_limits: dict[str, dict] = {}
        #: Fill-model counters (Blended5 limit-order study). Idle in the
        #: default "market" mode; every entry-limit lifecycle event bumps
        #: exactly one bucket so a run's whole fill model is read post-run:
        #:   limit_placed      entry increments converted to resting limits
        #:   limit_cross       filled passively (low/high crossed the limit)
        #:   limit_fell_back   "limit_then_market" TTL exhaustion -> market
        #:   limit_cancelled   "limit" TTL expiry, flat-intent cancel, or a
        #:                     reduce/flatten of the same symbol
        self.fill_counters: dict[str, int] = {
            "limit_placed": 0, "limit_cross": 0,
            "limit_fell_back": 0, "limit_cancelled": 0,
        }

    def next_bracket_id(self) -> int:
        bid = self._bracket_seq
        self._bracket_seq += 1
        return bid

    def position_qty(self, symbol: str) -> float:
        return self.positions.get(symbol, 0.0)

    def mark(self, prices: dict[str, float]) -> None:
        """Record the prices seen this bar, for later valuation."""
        for sym, px in prices.items():
            if math.isfinite(px) and px > 0:
                self.last_price[sym] = px

    def position_value(self, prices: dict[str, float]) -> tuple[float, list[str]]:
        """Mark-to-market value of open positions, and any symbols still unpriced.

        Prefers this bar's price, falls back to the last price that symbol
        printed at. A symbol in neither is genuinely unvaluable -- it is
        reported rather than dropped, so "cannot be marked" is visible instead
        of reading as "worth zero".
        """
        fx = self.config.fx_rate(self.config.currency)
        pv = 0.0
        unpriced: list[str] = []
        for sym, qty in self.positions.items():
            if abs(qty) < 1e-12:
                continue
            px = prices.get(sym)
            if px is None or not math.isfinite(px) or px <= 0:
                px = self.last_price.get(sym)
            if px is None:
                unpriced.append(sym)
                continue
            pv += qty * px * fx
        return pv, unpriced

    def snapshot(self, clock: Clock, prices: dict[str, float]) -> None:
        self.mark(prices)
        pv, unpriced = self.position_value(prices)
        row = {
            "time": clock.time_ms,
            "cash": self.cash,
            "positions_value": pv,
            "equity": self.cash + pv,
        }
        if unpriced:
            # Non-zero `n_unpriced` is a data warning, not a normal state. It is
            # recorded so a run that cannot value part of its book cannot be
            # mistaken for one that has no positions left.
            row["unpriced"] = unpriced
        self.equity_curve.append(row)


def _is_immediate(fill_model: object) -> bool:
    fn = getattr(fill_model, "is_immediate", None)
    return bool(fn()) if fn is not None else False


def _fill(state: MarketState, config: EngineConfig, symbol: str,
          is_buy: bool, qty: float, price: float, ms: int,
          opening: bool = False, order_ref: str | None = None) -> None:
    """Book one fill.

    ``opening`` distinguishes an ENTRY (which opens gross inventory on its own
    side) from an EXIT (which closes inventory on the opposite side).

    This distinction is load-bearing, and getting it wrong is silent. The VP
    strategy holds concurrent opposing lots -- the study has both sides open on
    63% of its multi-lot asset-dates, up to 25 lots on one instrument -- so
    inventory must be tracked GROSS PER SIDE, not netted.

    Netting it on every fill means a short entry consumes the long inventory:
    buy 10 then sell 10 leaves long=0, short=10, and the long's stop is then
    cancelled as "position gone" while the long is still very much open. That
    is precisely the case this exists to support.
    """
    signed = qty if is_buy else -qty
    fees = commission(signed, price, config.commission_model)   # in contract currency
    fx = config.fx_rate(config.currency)                        # contract -> SGD
    state.cash += -(signed * price + fees) * fx
    state.positions[symbol] = state.positions.get(symbol, 0.0) + signed

    if opening:
        # An entry adds to its own side's gross inventory.
        side = state.long_qty if is_buy else state.short_qty
        side[symbol] = side.get(symbol, 0.0) + qty
    else:
        # An exit retires inventory on the side it was protecting: a sell-stop
        # covers a long, a buy-stop covers a short.
        side = state.long_qty if not is_buy else state.short_qty
        side[symbol] = max(0.0, side.get(symbol, 0.0) - qty)
    # transactions are (stock, exchange, currency, quantity, price, fees) —
    # the exact shape `realized_per_trade_pnl` expects (chronological).
    state.transactions.append((symbol, "SMART", config.currency, signed, price, fees))
    # rich fill log for full stats (time, side, price, fees).
    state.fills.append({
        "time_ms": ms, "symbol": symbol, "qty": signed,
        "price": price, "fees": fees, "is_buy": is_buy,
        "order_ref": order_ref,
    })


# ────────────────────────────────────────────────────────────────────────────
# Target-position arm (the `PendingDbQuery` path)
# ────────────────────────────────────────────────────────────────────────────
def _process_pending_limits(state, config, bars, clock):
    """Fill / age resting ENTRY limits (entry_fill_mode != "market").

    Order semantics per symbol-PRINT of the limit's symbol (a symbol that
    does not print does not consume TTL):
      crossed  -> fill the whole remainder (px = the LIMIT price — the
                  conservative rust `LimitFillModel` cap. With
                  `config.optimistic_limit_fill=True` the legacy tape-truth
                  form is restored: px = the OPEN when the open gapped
                  through the limit, else the limit price). No slippage
                  either way -- the fund is the passive side;
      expired  -> "limit" mode: cancel (leg never opens).
                   "limit_then_market": cross to market at THIS print's
                   close + adverse slippage (fallback, no miss).
    Limits survive overnight gaps (a mid-cross at the next open fills
    there). Exits never rest; a strategy reduce/flatten of a symbol with a
    pending limit cancels the pending part (see `_reconcile_targets`).

    PIN POINTER (canonical notice = study/blended5_fund/FILE_AUDIT.md §17):
    every stored python hybrid result (§14 ladder, Oct-23
    `_fill_order_study_report` goldens, all `_fund_report_fund_hybrid_*`)
    was produced with `optimistic_limit_fill=True`. The engine default is
    the at-limit form; reproduce the stored lineage with the harness seam
    `LIMIT_FILL=optimistic`.
    """
    mode = config.entry_fill_mode
    for sym, pl in list(state.pending_limits.items()):
        bar = bars.get(sym)
        if bar is None or not math.isfinite(bar.low) \
                or not math.isfinite(bar.high):
            continue
        is_buy, qty, limit = pl["is_buy"], pl["qty"], pl["limit_px"]
        crossed = (bar.low <= limit) if is_buy else (bar.high >= limit)
        if crossed:
            # Default = rust LimitFillModel parity: fill AT the limit (cap
            # semantics). Optimistic (opt-in) = tape-truth: the open when it
            # gapped through the limit.
            px = limit
            if config.optimistic_limit_fill and (
                (bar.open <= limit) if is_buy else (bar.open >= limit)
            ):
                px = bar.open
            _fill(state, config, sym, is_buy, qty, px, clock.time_ms,
                  opening=True)
            state.fill_counters["limit_cross"] += 1
            del state.pending_limits[sym]
            continue
        pl["ttl"] -= 1
        if pl["ttl"] <= 0:
            if mode == "limit_then_market":
                price = apply_slippage(bar.close, is_buy, config.slippage_bps)
                _fill(state, config, sym, is_buy, qty, price, clock.time_ms,
                      opening=True)
                state.fill_counters["limit_fell_back"] += 1
            else:
                state.fill_counters["limit_cancelled"] += 1
            del state.pending_limits[sym]


def _reconcile_targets(state, config, targets, bars, ms, anchor_px=None):
    mp_mode = config.entry_fill_mode == "midprice"   # IBKR midprice peg: fill at close, NO slippage
    lim_mode = config.entry_fill_mode not in ("market", "midprice")
    for sym, target_qty in targets.items():
        if sym.startswith("CASH:"):
            continue
        cur = state.position_qty(sym)
        delta = target_qty - cur
        if abs(delta) < 1e-9:
            # Absolute-target semantics: the latest intent is "hold exactly
            # here" -> any resting ENTRY limit for the symbol is cancelled
            # (only possible in lim-mode; nothing ever rests in market mode).
            if lim_mode and sym in state.pending_limits:
                state.pending_limits.pop(sym, None)
                state.fill_counters["limit_cancelled"] += 1
            continue
        bar = bars.get(sym)
        if bar is None or not math.isfinite(bar.close):
            continue
        if lim_mode:
            # ENTRY vs EXIT decomposition (general): an increment that opens
            # a fresh position or extends it in its own direction is an
            # entry (may rest as a limit); one that reduces or flips
            # toward zero is a market exit and cancels any resting entry
            # for that symbol. A full FLIP (delta beyond the position,
            # opposite direction) fills entirely at market -- conservative,
            # and never exercised by this repo's long-only books.
            opening = (cur == 0) or ((delta > 0) == (cur > 0))
            if opening:
                pl = state.pending_limits.get(sym)
                d_abs, d_buy = float(abs(delta)), delta > 0
                if pl is not None and pl["is_buy"] == d_buy \
                        and pl["qty"] == d_abs:
                    # Same intent already rests (re-emitting wrappers like the
                    # FundBook resell the full basket every bar): keep the
                    # ORIGINAL anchor + ttl -- re-anchoring every bar would
                    # pin the limit to the tape and TTL would never age.
                    continue
                anchor = bar.close
                if anchor_px is not None:
                    # R3/decision-anchor: lag1 settle rests entry limits on
                    # the DECISION row's close (close(r)), the golden-lag0
                    # anchor, instead of the settle row's own close — which
                    # does not exist at placement time (intrabar look-ahead).
                    dec = anchor_px.get(sym)
                    if dec is not None and math.isfinite(dec) and dec > 0:
                        anchor = dec
                k = config.entry_limit_k_bp * 1e-4
                state.pending_limits[sym] = {
                    "is_buy": d_buy, "qty": d_abs,
                    "limit_px": anchor * (1.0 - k if d_buy else 1.0 + k),
                    "ttl": int(config.entry_limit_ttl),
                }
                state.fill_counters["limit_placed"] += 1
                continue
            if sym in state.pending_limits:
                state.pending_limits.pop(sym, None)
                state.fill_counters["limit_cancelled"] += 1
            # Reduce/flip fills market toward zero. For a reduce of a SHORT
            # the fill is a buy-cover (delta > 0), for a reduce of a LONG a
            # sell (delta < 0) — derive the side from delta, never hardcode.
            is_buy = delta > 0
            price = apply_slippage(bar.close, is_buy, config.slippage_bps)
            _fill(state, config, sym, is_buy, abs(delta), price, ms,
                  opening=True)
            continue
        is_buy = delta > 0
        if mp_mode:
            price = bar.close                            # MIDPRICE peg: close, no slippage
        else:
            price = apply_slippage(bar.close, is_buy, config.slippage_bps)
        _fill(state, config, sym, is_buy, abs(delta), price, ms, opening=True)


# ────────────────────────────────────────────────────────────────────────────
# Order / bracket construction (port of `bracket.rs`)
# ────────────────────────────────────────────────────────────────────────────

def _parse_brackets(orders: list[Order]) -> list[tuple[int, list[int]]]:
    """Group orders into (parent_idx, [child_idx]) by references_parent_order."""
    brackets: list[tuple[int, list[int]]] = []
    parent: int | None = None
    children: list[int] = []
    for i, o in enumerate(orders):
        if o.references_parent_order == -1:
            if parent is not None:
                brackets.append((parent, children))
                children = []
            parent = i
        else:
            children.append(i)
    if parent is not None:
        brackets.append((parent, children))
    return brackets


def _build_resting_orders(state, orders: list[Order], slippage_bps: float,
                          stop_pct_of_bar: float,
                          stop_fill_model: type | None = None) -> list[RestingOrder]:
    """Build resting orders for one bracket batch.

    ``stop_fill_model`` selects the fill convention for ``STP`` exits. It
    defaults to :class:`StopMarketFillModel`, preserving existing behaviour; pass
    :class:`BarExtremeFillModel` for the gap-through bound. That model fills a
    breached stop at the triggering bar's adverse extreme, which is the worst
    price that actually printed -- a real bound, where ``StopMarketFillModel``
    extrapolates past the bar's own range to a price that never traded.
    """
    StopModel = stop_fill_model or StopMarketFillModel
    out: list[RestingOrder] = []
    for parent_idx, child_indices in _parse_brackets(orders):
        bracket_id = state.next_bracket_id()
        parent = orders[parent_idx]
        entry_is_buy = parent.is_buy
        qty = parent.qty

        # entry: limit (resting) if LMT with a positive limit, else market
        if parent.order_type == LMT and parent.limit_price and parent.limit_price > 0.0:
            entry_model: object = LimitFillModel(parent.limit_price, entry_is_buy)
        else:
            entry_model = MarketFillModel(slippage_bps, entry_is_buy)
        out.append(RestingOrder(parent.symbol, entry_is_buy, qty, bracket_id, "entry",
                                True, entry_model, None, False, RESTING,
                                active_after=parent.active_after,
                                order_ref=parent.order_ref, ttl=parent.ttl))

        for ci in child_indices:
            child = orders[ci]
            child_is_buy = child.is_buy
            if child.order_type == STP:
                stop = child.stop or 0.0
                if child.limit_price and child.limit_price > 0.0:
                    model: object = StopLimitFillModel(stop, child.limit_price, child_is_buy)
                else:
                    model = StopModel(stop, stop_pct_of_bar, child_is_buy) \
                        if StopModel is StopMarketFillModel \
                        else StopModel(stop, child_is_buy)
                out.append(RestingOrder(child.symbol, child_is_buy, child.qty, bracket_id,
                                        "exit", False, model, None, False, RESTING,
                                        active_after=child.active_after,
                                        order_ref=child.order_ref
                                        if child.order_ref is not None
                                        else parent.order_ref))
            elif child.order_type == LMT:
                tp = child.limit_price or 0.0
                if tp > 0.0:
                    out.append(RestingOrder(child.symbol, child_is_buy, child.qty, bracket_id,
                                            "exit", False, LimitFillModel(tp, child_is_buy),
                                            None, False, RESTING,
                                            active_after=child.active_after,
                                            order_ref=child.order_ref
                                            if child.order_ref is not None
                                            else parent.order_ref))
            elif child.order_type in (MIDPRICE, MIDCLOSE):
                # Rust quirk: close_is_mid is True for "MIDCLOSE" (fill at mid),
                # False for "MIDPRICE" (fill at close).
                close_is_mid = child.order_type == MIDCLOSE
                out.append(RestingOrder(child.symbol, child_is_buy, child.qty, bracket_id,
                                        "exit", False, None, child.close_mso, close_is_mid,
                                        RESTING, active_after=child.active_after,
                                        order_ref=child.order_ref
                                        if child.order_ref is not None
                                        else parent.order_ref))
    return out


# ────────────────────────────────────────────────────────────────────────────
# Resting-order processing (port of `reconcile.rs::process_resting_orders`)
# ────────────────────────────────────────────────────────────────────────────
def _process_resting(state, config, bars, ms, mso):
    """Port of Rust `reconcile.rs::process_resting_orders` bracket guarantees
    (the stop-limit-path semantics, applied to every bracket):

    * at most ONE exit fill per bracket per bar (book order) — the legacy
      per-order loop could fill BOTH the stop and the TP on a bar spanning the
      whole bracket, leaving a phantom reversed position;
    * at EOD (mso >= 385) the exit sequence still runs on the final bar FIRST
      (stop / TP / time-gated close), and only a still-open position
      force-closes ONCE at the final bar's close; the rest of the bracket is
      dropped (resting entries never fill at EOD);
    * OCA: once an exit fills, still-resting exit siblings are cancelled.
    """
    groups: dict[int, list[RestingOrder]] = {}
    bracket_order: list[int] = []
    for ro in state.resting:
        if ro.status != RESTING:
            continue
        if ro.bracket_id not in groups:
            bracket_order.append(ro.bracket_id)
        groups.setdefault(ro.bracket_id, []).append(ro)

    remaining: list[RestingOrder] = []
    for bid in bracket_order:
        group = groups[bid]
        bar = bars.get(group[0].symbol)
        if bar is None or not math.isfinite(bar.close):
            remaining.extend(group)
            continue
        ohlc = BarOhlc(bar.open, bar.high, bar.low, bar.close)
        eod = mso >= 385
        exit_filled = False

        # resting (limit) entries may fill on this bar — but never at EOD
        entry_filled_this_bar = False
        entry_filled_ms: int | None = None
        if not eod:
            for ro in group:
                if ro.role != "entry" or not ro.active or ro.fill_model is None:
                    continue
                if ro.active_after is not None and ms < ro.active_after:
                    continue          # not armed yet: inert, survives to next bar
                fe = ro.fill_model.evaluate(ohlc)
                if fe.kind == "fill":
                    _fill(state, config, ro.symbol, ro.is_buy, ro.qty,
                          fe.price, ms, opening=True, order_ref=ro.order_ref)
                    ro.status = FILLED
                    entry_filled_ms = ms
                    entry_filled_this_bar = True
                elif ro.ttl is not None:
                    # Per-order TTL aging (Blended5Fund order adapter): an
                    # unfilled resting ENTRY limit ages one print per
                    # symbol-PRINT — the group-level `continue` above already
                    # guarantees this bar printed for this symbol, so
                    # non-printing bars never consume TTL. On exhaustion the
                    # order falls back to market at THIS print's close +
                    # adverse slippage ("limit_then_market" semantics carried
                    # on the order; counts as an entry fill so exit-arming
                    # still happens).
                    ro.ttl -= 1
                    if ro.ttl <= 0:
                        price = apply_slippage(bar.close, ro.is_buy,
                                               config.slippage_bps)
                        _fill(state, config, ro.symbol, ro.is_buy, ro.qty,
                              price, ms, opening=True, order_ref=ro.order_ref)
                        state.fill_counters["limit_fell_back"] += 1
                        ro.status = FILLED
                        entry_filled_ms = ms
                        entry_filled_this_bar = True
            if entry_filled_this_bar:
                for ro in group:
                    if ro.role == "exit":
                        ro.active = True
                        ro.entry_filled_ms = entry_filled_ms

        # INVARIANT: an exit may only fill against a position that still exists.
        #
        # A strategy can flatten a position without going through this bracket --
        # e.g. a market time-stop emitted on a later bar, or a manual exit. The
        # bracket's resting stop does NOT know that, so it stays armed and later
        # fires against a flat (or even inverted) book, manufacturing a short.
        #
        # Measured on KRT+ANAB: 1,079 transactions left a short position, worst
        # -178 shares, purely from this. OCA only cancels siblings *within* a
        # bracket, so nothing caught an externally-flattened position.
        #
        # Real venues kill the resting order when the position it protects is
        # closed. Enforce that: skip and cancel any exit whose position is gone.
        #
        # Checked against the SIDE's inventory, not the net. A long exit sells,
        # so it is protected by long inventory; a short exit buys, protected by
        # short inventory. Checking the net instead cancels every stop in a
        # hedged book -- +100/-100 nets to 0, which reads as "flat" and strands
        # both lots with no exit at all. This is the only place the engine
        # departs from the net-position model; with no opposing lots the two
        # agree exactly.
        for ro in group:
            if ro.role != "exit" or ro.status != RESTING or not ro.active:
                continue
            # NOTE: inactive exits are SKIPPED, not cancelled. An exit is
            # inactive until its entry fills, so it has no position yet by
            # design -- and with no position the guard below would cancel it
            # on the first bar. That stranded every resting-entry bracket
            # (LMT/STP parents): the entry filled later but its exits were
            # already gone, leaving the position open with no exit at all.
            # The stale-stop protection is preserved: once active (entry
            # filled), an exit whose position is gone is still cancelled.
            if ro.qty <= 0:
                continue
            # A sell closes long inventory; a buy covers short inventory.
            protected = (state.long_qty if not ro.is_buy
                         else state.short_qty).get(ro.symbol, 0.0)
            if protected < ro.qty - 0.5 * max(ro.qty, 1.0):
                ro.status = CANCELLED

        # exits in book order, ONE fill max (stop-first if emitted first)
        for ro in group:
            if exit_filled or ro.role != "exit" or not ro.active or ro.status != RESTING:
                continue
            if ro.active_after is not None and ms < ro.active_after:
                continue              # the time-based exit has not come due yet
            price = None
            if ro.fill_model is not None:
                # STP / LMT: the model decides, price-wise and trigger-wise
                fe = ro.fill_model.evaluate(ohlc)
                if fe.kind == "fill":
                    price = fe.price
            elif ro.close_mso is not None:
                # MIDCLOSE/MIDPRICE gated on an absolute clock time
                if mso >= ro.close_mso:
                    price = (bar.high + bar.low) / 2.0 if ro.close_is_mid else bar.close
            elif ro.active_after is not None:
                # MIDCLOSE with NO close_mso: the gate IS the trigger. Both
                # other paths are None for such an order, so without this branch
                # a time exit reached its deadline and simply never filled --
                # it sat resting until the session's EOD force-close, which is
                # 48 bars late on a 78-bar day.
                price = (bar.high + bar.low) / 2.0 if ro.close_is_mid else bar.close
            if price is not None:
                _fill(state, config, ro.symbol, ro.is_buy, ro.qty, price, ms,
                      order_ref=ro.order_ref)
                ro.status = FILLED
                exit_filled = True

        # EOD: ONE force-close for a still-open position, then drop the bracket
        if eod and not exit_filled and config.force_close_at_eod:
            closed = False
            for ro in group:
                if closed or ro.role != "exit" or not ro.active or ro.status != RESTING:
                    continue
                price = (bar.high + bar.low) / 2.0 if ro.close_is_mid else bar.close
                _fill(state, config, ro.symbol, ro.is_buy, ro.qty, price, ms,
                      order_ref=ro.order_ref)
                ro.status = FILLED
                closed = True
            exit_filled = closed

        # OCA sibling cancellation
        if any(ro.role == "exit" and ro.status == FILLED for ro in group):
            for ro in group:
                if ro.role == "exit" and ro.status == RESTING:
                    ro.status = CANCELLED
        if eod and config.force_close_at_eod:
            continue  # drop the entire bracket (incl. unfilled entries)
        remaining.extend(ro for ro in group if ro.status == RESTING)

    state.resting = remaining


def _reconcile_orders(state, config, orders, bars, ms, stop_pct_of_bar=0.5,
                     stop_fill_model=None):
    # Cancel pre-pass (per-order lifecycle, Blended5Fund order adapter): an
    # order with cancel=True is a placeholder — remove every RESTING ENTRY
    # order matching (order_ref, symbol) instead of placing anything.
    if any(getattr(o, "cancel", False) for o in orders):
        for o in orders:
            if not getattr(o, "cancel", False):
                continue
            for ro in state.resting:
                if ro.status == RESTING and ro.role == "entry" \
                        and ro.symbol == o.symbol \
                        and ro.order_ref == o.order_ref:
                    ro.status = CANCELLED
                    state.fill_counters["limit_cancelled"] += 1

    # Cancel placeholders are consumed by the pre-pass above; they must NOT
    # enter the bracket build (a qty-0 MKT placeholder would otherwise build
    # an immediate market entry and print a 0-qty fill under its order_ref).
    orders = [o for o in orders if not getattr(o, "cancel", False)]

    resting = _build_resting_orders(state, orders, config.slippage_bps,
                                    stop_pct_of_bar, stop_fill_model)

    # Immediate (market) entries fill now against their own bar.
    # active_after gates them the same way the resting path gates LMT
    # entries: before its active time an order is inert (the Order contract
    # says "applies to ANY order type"), so it stays RESTING and fills at
    # the first bar on/after its active timestamp via _process_resting.
    filled_entry_brackets: set[int] = set()
    for ro in resting:
        if ro.role != "entry" or not _is_immediate(ro.fill_model):
            continue
        if ro.active_after is not None and ms < ro.active_after:
            continue          # not armed yet: keep resting, fills later
        bar = bars.get(ro.symbol)
        if bar is None or not math.isfinite(bar.close):
            continue
        fe = ro.fill_model.evaluate(BarOhlc(bar.open, bar.high, bar.low, bar.close))
        if fe.kind == "fill":
            _fill(state, config, ro.symbol, ro.is_buy, ro.qty, fe.price, ms,
                  opening=True, order_ref=ro.order_ref)
            ro.status = FILLED
            filled_entry_brackets.add(ro.bracket_id)

    # Arm exits whose entry filled this bar.
    for ro in resting:
        if ro.role == "exit" and ro.bracket_id in filled_entry_brackets:
            ro.active = True

    state.resting.extend(ro for ro in resting if ro.status == RESTING)


# ────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class ThoroughResult:
    results: BacktestResults
    state: MarketState


def run_thorough(uni, strategy, config: EngineConfig | None = None,
                 stop_fill_model=None) -> ThoroughResult:
    """Replay `uni` bar-by-bar, driving `strategy.on_row`, and return results.

    ``stop_fill_model`` selects the fill convention for stop exits. Defaults to
    :class:`StopMarketFillModel` (unchanged); pass
    :class:`BarExtremeFillModel` for the gap-through bound.
    """
    config = config or EngineConfig()
    if config.lag_entries_bars and config.fill_lag_bars:
        raise ValueError("lag_entries_bars is only supported with fill_lag_bars=0")
    state = MarketState(config)
    clock = Clock()

    long = uni.long.sort_values("ts_epoch_ms")
    pending: deque = deque()   # (row_idx, targets, decision-anchor px) queue
    row_idx = 0
    for ms, g in long.groupby("ts_epoch_ms", sort=True):
        bars: dict[str, Bar] = {}
        for r in g.itertuples(index=False):
            if all(math.isfinite(x) for x in (r.open, r.high, r.low, r.close)):
                bars[r.asset] = Bar(
                    ts_epoch_ms=int(ms),
                    open=float(r.open), high=float(r.high), low=float(r.low),
                    close=float(r.close), volume=float(r.volume),
                )
        if not bars:
            continue
        row_idx += 1
        clock.set(int(ms))
        # Target-fill lag (Rust replay parity, EngineConfig.fill_lag_bars):
        # settle baskets queued `fill_lag_bars` rows ago BEFORE mark/ctx, so
        # the strategy still sizes against the settled book.
        while pending and pending[0][0] + config.fill_lag_bars <= row_idx:
            _, due_targets, due_anchor = pending.popleft()
            _reconcile_targets(state, config, due_targets, bars, clock.time_ms,
                               anchor_px=due_anchor)
        # Mark positions to the current bar's closes so a strategy sizing off
        # equity sees an up-to-date figure, not last bar's.
        px = {s: b.close for s, b in bars.items()}
        state.mark(px)
        # Same last-known-price fallback as `snapshot`, and it matters MORE
        # here: this is the equity the strategy sizes against, so understating
        # it shrinks position size, and once it went negative it triggered
        # `blocked_equity` on 540 touches in a 40-asset run -- an artefact of
        # missing marks, not of risk.
        pv, unpriced = state.position_value(px)
        # Resting ENTRY limits act on this bar's prints BEFORE the strategy
        # sees positions, so the just-filled state is what strategies size on.
        # (order: mark -> pending limits -> ctx -> on_row -> stops -> reconcile)
        if config.entry_fill_mode != "market":
            _process_pending_limits(state, config, bars, clock)
        ctx = Context(clock=clock, bars=bars,
                      account=SimpleNamespace(equity=state.cash + pv,
                                              cash=state.cash,
                                              positions=dict(state.positions),
                                              # Gross per side. A strategy holding
                                              # concurrent opposing lots cannot
                                              # reconcile against the net.
                                              long_qty=dict(state.long_qty),
                                              short_qty=dict(state.short_qty),
                                              n_fills=len(state.fills),
                                              fills=state.fills,
                                              unpriced=list(unpriced),
                                              # Resting (unfilled) order book,
                                              # per-order attributed — the
                                              # Blended5Fund order adapter diffs
                                              # desired vs position + its own
                                              # open orders (IBKR-style).
                                              open_orders=[
                                                  {"symbol": ro.symbol,
                                                   "is_buy": ro.is_buy,
                                                   "qty": ro.qty,
                                                   "order_ref": ro.order_ref,
                                                   "role": ro.role}
                                                  for ro in state.resting
                                                  if ro.status == RESTING]))

        # order matches Rust replay.rs: on_bar_update -> process_resting -> reconcile
        outcome = strategy.on_row(bars, ctx)
        _process_resting(state, config, bars, clock.time_ms, clock.mso)
        if outcome.kind == "set_targets":
            if config.fill_lag_bars > 0:
                # Rust-replay parity: park the basket; it settles
                # `fill_lag_bars` rows later (flush at top of the loop).
                # Decision-row anchors ride with the basket so lag1 entry
                # limits anchor on close(r) — entry_limit_anchor="decision"
                # is the default (honest); "settle" is the legacy opt-in.
                d_anchor = ({s: b.close for s, b in bars.items()
                             if s in outcome.targets}
                            if config.entry_limit_anchor == "decision" else None)
                pending.append((row_idx, dict(outcome.targets), d_anchor))
            else:
                if config.lag_entries_bars:
                    # E-fork (entry-only delay at lag0): increase-to-target
                    # legs ride the pending queue for lag_entries_bars rows;
                    # reductions/flattens reconcile same-row as legacy lag0.
                    pos = state.positions
                    ent = {s: q for s, q in outcome.targets.items()
                           if q > pos.get(s, 0.0)}
                    rest = {s: q for s, q in outcome.targets.items()
                            if q <= pos.get(s, 0.0) and q != pos.get(s, 0.0)}
                    if rest:
                        _reconcile_targets(state, config, rest, bars,
                                           clock.time_ms)
                    if ent:
                        d_anchor = ({s: b.close for s, b in bars.items()
                                     if s in ent}
                                    if config.entry_limit_anchor == "decision"
                                    else None)
                        pending.append((row_idx + config.lag_entries_bars,
                                        dict(ent), d_anchor))
                else:
                    _reconcile_targets(state, config, outcome.targets, bars,
                                       clock.time_ms)
        elif outcome.kind == "emit_orders":
            _reconcile_orders(state, config, outcome.orders, bars, clock.time_ms,
                              stop_fill_model=stop_fill_model)

        state.snapshot(clock, px)

    results = compute_results(
        equity_curve=state.equity_curve,
        transactions=state.transactions,
        starting_capital=config.starting_equity,
        bar_interval_minutes=config.bar_minutes,
    )
    return ThoroughResult(results=results, state=state)


__all__ = ["MarketState", "RestingOrder", "ThoroughResult", "run_thorough"]
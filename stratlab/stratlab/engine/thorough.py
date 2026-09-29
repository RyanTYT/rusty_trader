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
from dataclasses import dataclass

from stratlab.cost import (
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


class MarketState:
    """In-memory cash / positions / transactions / resting-order book."""

    def __init__(self, config: EngineConfig):
        self.config = config
        self.cash = config.starting_equity
        self.positions: dict[str, float] = {}
        self.transactions: list[tuple] = []
        self.fills: list[dict] = []          # rich fill log for full stats
        self.resting: list[RestingOrder] = []
        self.equity_curve: list[dict] = []
        self._bracket_seq = 0

    def next_bracket_id(self) -> int:
        bid = self._bracket_seq
        self._bracket_seq += 1
        return bid

    def position_qty(self, symbol: str) -> float:
        return self.positions.get(symbol, 0.0)

    def snapshot(self, clock: Clock, prices: dict[str, float]) -> None:
        pv = 0.0
        fx = self.config.fx_rate(self.config.currency)
        for sym, qty in self.positions.items():
            px = prices.get(sym)
            if px is None or not math.isfinite(px):
                continue
            pv += qty * px * fx
        self.equity_curve.append({
            "time": clock.time_ms,
            "cash": self.cash,
            "positions_value": pv,
            "equity": self.cash + pv,
        })


def _is_immediate(fill_model: object) -> bool:
    fn = getattr(fill_model, "is_immediate", None)
    return bool(fn()) if fn is not None else False


def _fill(state: MarketState, config: EngineConfig, symbol: str,
          is_buy: bool, qty: float, price: float, ms: int) -> None:
    signed = qty if is_buy else -qty
    fees = commission(signed, price, config.commission_model)   # in contract currency
    fx = config.fx_rate(config.currency)                        # contract -> SGD
    state.cash += -(signed * price + fees) * fx
    state.positions[symbol] = state.positions.get(symbol, 0.0) + signed
    # transactions are (stock, exchange, currency, quantity, price, fees) —
    # the exact shape `realized_per_trade_pnl` expects (chronological).
    state.transactions.append((symbol, "SMART", config.currency, signed, price, fees))
    # rich fill log for full stats (time, side, price, fees).
    state.fills.append({
        "time_ms": ms, "symbol": symbol, "qty": signed,
        "price": price, "fees": fees, "is_buy": is_buy,
    })


# ────────────────────────────────────────────────────────────────────────────
# Target-position arm (the `PendingDbQuery` path)
# ────────────────────────────────────────────────────────────────────────────
def _reconcile_targets(state, config, targets, bars, ms):
    for sym, target_qty in targets.items():
        if sym.startswith("CASH:"):
            continue
        cur = state.position_qty(sym)
        delta = target_qty - cur
        if abs(delta) < 1e-9:
            continue
        bar = bars.get(sym)
        if bar is None or not math.isfinite(bar.close):
            continue
        is_buy = delta > 0
        price = apply_slippage(bar.close, is_buy, config.slippage_bps)
        _fill(state, config, sym, is_buy, abs(delta), price, ms)


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
                          stop_pct_of_bar: float) -> list[RestingOrder]:
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
                                True, entry_model, None, False, RESTING))

        for ci in child_indices:
            child = orders[ci]
            child_is_buy = child.is_buy
            if child.order_type == STP:
                stop = child.stop or 0.0
                if child.limit_price and child.limit_price > 0.0:
                    model: object = StopLimitFillModel(stop, child.limit_price, child_is_buy)
                else:
                    model = StopMarketFillModel(stop, stop_pct_of_bar, child_is_buy)
                out.append(RestingOrder(child.symbol, child_is_buy, child.qty, bracket_id,
                                        "exit", False, model, None, False, RESTING))
            elif child.order_type == LMT:
                tp = child.limit_price or 0.0
                if tp > 0.0:
                    out.append(RestingOrder(child.symbol, child_is_buy, child.qty, bracket_id,
                                            "exit", False, LimitFillModel(tp, child_is_buy),
                                            None, False, RESTING))
            elif child.order_type in (MIDPRICE, MIDCLOSE):
                # Rust quirk: close_is_mid is True for "MIDCLOSE" (fill at mid),
                # False for "MIDPRICE" (fill at close).
                close_is_mid = child.order_type == MIDCLOSE
                out.append(RestingOrder(child.symbol, child_is_buy, child.qty, bracket_id,
                                        "exit", False, None, child.close_mso, close_is_mid, RESTING))
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
        if not eod:
            for ro in group:
                if ro.role != "entry" or not ro.active or ro.fill_model is None:
                    continue
                fe = ro.fill_model.evaluate(ohlc)
                if fe.kind == "fill":
                    _fill(state, config, ro.symbol, ro.is_buy, ro.qty, fe.price, ms)
                    ro.status = FILLED
                    entry_filled_this_bar = True
            if entry_filled_this_bar:
                for ro in group:
                    if ro.role == "exit":
                        ro.active = True

        # exits in book order, ONE fill max (stop-first if emitted first)
        for ro in group:
            if exit_filled or ro.role != "exit" or not ro.active or ro.status != RESTING:
                continue
            price = None
            if ro.close_mso is not None:
                if mso >= ro.close_mso:
                    price = (bar.high + bar.low) / 2.0 if ro.close_is_mid else bar.close
            elif ro.fill_model is not None:
                fe = ro.fill_model.evaluate(ohlc)
                if fe.kind == "fill":
                    price = fe.price
            if price is not None:
                _fill(state, config, ro.symbol, ro.is_buy, ro.qty, price, ms)
                ro.status = FILLED
                exit_filled = True

        # EOD: ONE force-close for a still-open position, then drop the bracket
        if eod and not exit_filled:
            closed = False
            for ro in group:
                if closed or ro.role != "exit" or not ro.active or ro.status != RESTING:
                    continue
                price = (bar.high + bar.low) / 2.0 if ro.close_is_mid else bar.close
                _fill(state, config, ro.symbol, ro.is_buy, ro.qty, price, ms)
                ro.status = FILLED
                closed = True
            exit_filled = closed

        # OCA sibling cancellation
        if any(ro.role == "exit" and ro.status == FILLED for ro in group):
            for ro in group:
                if ro.role == "exit" and ro.status == RESTING:
                    ro.status = CANCELLED
        if eod:
            continue  # drop the entire bracket (incl. unfilled entries)
        remaining.extend(ro for ro in group if ro.status == RESTING)

    state.resting = remaining


def _reconcile_orders(state, config, orders, bars, ms, stop_pct_of_bar=0.5):
    resting = _build_resting_orders(state, orders, config.slippage_bps, stop_pct_of_bar)

    # Immediate (market) entries fill now against their own bar.
    filled_entry_brackets: set[int] = set()
    for ro in resting:
        if ro.role != "entry" or not _is_immediate(ro.fill_model):
            continue
        bar = bars.get(ro.symbol)
        if bar is None or not math.isfinite(bar.close):
            continue
        fe = ro.fill_model.evaluate(BarOhlc(bar.open, bar.high, bar.low, bar.close))
        if fe.kind == "fill":
            _fill(state, config, ro.symbol, ro.is_buy, ro.qty, fe.price, ms)
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


def run_thorough(uni, strategy, config: EngineConfig | None = None) -> ThoroughResult:
    """Replay `uni` bar-by-bar, driving `strategy.on_row`, and return results."""
    config = config or EngineConfig()
    state = MarketState(config)
    clock = Clock()

    long = uni.long.sort_values("ts_epoch_ms")
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
        clock.set(int(ms))
        ctx = Context(clock=clock, bars=bars)

        # order matches Rust replay.rs: on_bar_update -> process_resting -> reconcile
        outcome = strategy.on_row(bars, ctx)
        _process_resting(state, config, bars, clock.time_ms, clock.mso)
        if outcome.kind == "set_targets":
            _reconcile_targets(state, config, outcome.targets, bars, clock.time_ms)
        elif outcome.kind == "emit_orders":
            _reconcile_orders(state, config, outcome.orders, bars, clock.time_ms)

        state.snapshot(clock, {s: b.close for s, b in bars.items()})

    results = compute_results(
        equity_curve=state.equity_curve,
        transactions=state.transactions,
        starting_capital=config.starting_equity,
        bar_interval_minutes=config.bar_minutes,
    )
    return ThoroughResult(results=results, state=state)


__all__ = ["MarketState", "RestingOrder", "ThoroughResult", "run_thorough"]
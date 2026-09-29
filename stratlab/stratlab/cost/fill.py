"""Fill-decision logic — verbatim port of
``trading/trading-app/src/backtester/execution/fill_model.rs``.

* Market orders: fill at bar close ± slippage (bps), quantity signed by
  direction (+buy / -sell).
* Limit orders: fill iff the bar's [low, high] crosses the limit, at the limit
  price (no slippage).
* The resting ``FillModel`` family (Market/Mid/Limit/StopMarket/StopLimit) is
  used by the thorough event engine to rest orders across bars until they fill.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillOutcome:
    filled: bool
    fill_price: float
    fill_qty: float  # signed: +buy / -sell


@dataclass(frozen=True)
class BarOhlc:
    open: float
    high: float
    low: float
    close: float


def apply_slippage(close: float, is_buy: bool, slippage_bps: float) -> float:
    """The slippage calculator: fill price = close +/- slippage (bps)."""
    slip = close * (slippage_bps / 1e4)
    return close + slip if is_buy else close - slip


def decide_fill(*, is_buy: bool, qty: float, limit_price: float | None,
                low: float, high: float, close: float,
                slippage_bps: float) -> FillOutcome:
    """Decide one fill against a single bar (Rust `decide_fill`).

    A limit order is signalled by a positive `limit_price` (None/0 = market).
    """
    signed_qty = qty if is_buy else -qty
    if limit_price is not None and limit_price > 0.0:
        crosses = (low <= limit_price) if is_buy else (high >= limit_price)
        if crosses:
            return FillOutcome(filled=True, fill_price=limit_price, fill_qty=signed_qty)
        return FillOutcome(filled=False, fill_price=0.0, fill_qty=0.0)
    return FillOutcome(filled=True,
                       fill_price=apply_slippage(close, is_buy, slippage_bps),
                       fill_qty=signed_qty)


# ────────────────────────────────────────────────────────────────────────────
# Resting-order fill models (evaluated against successive bars until filled)
# ────────────────────────────────────────────────────────────────────────────
class FillEval:
    Rest = "rest"
    Fill = "fill"

    def __init__(self, kind: str, price: float = 0.0):
        self.kind = kind
        self.price = price

    @classmethod
    def rest(cls) -> "FillEval":
        return cls("rest")

    @classmethod
    def fill(cls, price: float) -> "FillEval":
        return cls("fill", price)


class MarketFillModel:
    """Fills immediately at the bar close ± slippage_bps (crossing the spread)."""

    def __init__(self, slippage_bps: float, is_buy: bool):
        self.slippage_bps = slippage_bps
        self.is_buy = is_buy

    def evaluate(self, bar: BarOhlc) -> FillEval:
        return FillEval.fill(apply_slippage(bar.close, self.is_buy, self.slippage_bps))

    def is_immediate(self) -> bool:
        return True


class MidPriceFillModel:
    """Fills immediately at (high + low) / 2."""

    def evaluate(self, bar: BarOhlc) -> FillEval:
        return FillEval.fill((bar.high + bar.low) / 2.0)

    def is_immediate(self) -> bool:
        return True


class LimitFillModel:
    """Rests until the bar's range crosses the limit; fills at the limit (no
    slippage)."""

    def __init__(self, limit: float, is_buy: bool):
        self.limit = limit
        self.is_buy = is_buy

    def evaluate(self, bar: BarOhlc) -> FillEval:
        crossed = (bar.low <= self.limit) if self.is_buy else (bar.high >= self.limit)
        return FillEval.fill(self.limit) if crossed else FillEval.rest()


class StopMarketFillModel:
    """Market stop: fills on breach at stop ± pct_of_bar*(high-low) (adverse)."""

    def __init__(self, stop: float, pct_of_bar: float, is_buy: bool):
        self.stop = stop
        self.pct_of_bar = pct_of_bar
        self.is_buy = is_buy

    def evaluate(self, bar: BarOhlc) -> FillEval:
        breached = (bar.high >= self.stop) if self.is_buy else (bar.low <= self.stop)
        if not breached:
            return FillEval.rest()
        slip = self.pct_of_bar * (bar.high - bar.low)
        price = self.stop + slip if self.is_buy else self.stop - slip
        return FillEval.fill(price)


class StopLimitFillModel:
    """Stop-limit: rests until stop breached; then rests one MORE bar, then
    fills only when a later bar crosses the limit (conservative gap-through)."""

    def __init__(self, stop: float, limit: float, is_buy: bool):
        self.stop = stop
        self.limit = limit
        self.is_buy = is_buy
        self.triggered = False
        self.rest_bars = 0

    def evaluate(self, bar: BarOhlc) -> FillEval:
        if not self.triggered:
            breached = (bar.high > self.stop) if self.is_buy else (bar.low < self.stop)
            if breached:
                self.triggered = True
                self.rest_bars = 1
            return FillEval.rest()
        if self.rest_bars > 0:
            self.rest_bars -= 1
            return FillEval.rest()
        crossed = (bar.low <= self.limit) if self.is_buy else (bar.high >= self.limit)
        return FillEval.fill(self.limit) if crossed else FillEval.rest()


__all__ = [
    "FillOutcome", "BarOhlc", "FillEval",
    "apply_slippage", "decide_fill",
    "MarketFillModel", "MidPriceFillModel", "LimitFillModel",
    "StopMarketFillModel", "StopLimitFillModel",
]
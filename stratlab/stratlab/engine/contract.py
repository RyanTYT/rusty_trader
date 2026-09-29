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

    def fx_rate(self, currency: str) -> float:
        return fx_to_sgd(currency) if self.fx is None else self.fx(currency)


__all__ = [
    "MKT", "LMT", "STP", "MIDPRICE", "MIDCLOSE",
    "FIXED_FX_TO_SGD", "fx_to_sgd", "good_after_time_to_mso",
    "Order", "Outcome", "Clock", "Context", "Strategy", "EngineConfig",
]
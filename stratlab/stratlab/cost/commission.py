"""IBKR Pro commission — verbatim port of
``trading/trading-app/src/backtester/execution/fill_model.rs``.

Both plans (Fixed / Tiered) plus a 1%-of-trade-value cap. `qty` is signed
(+ buy, - sell), `price` is the fill price. Returns total commission in the
contract currency (USD for US stocks).
"""

from __future__ import annotations

from enum import Enum

# Regulatory fees (both Fixed + Tiered).
SEC_FEE_RATE = 0.0000206          # $/USD of aggregate sales (sells only).
FINRA_TAF_PER_SHARE = 0.000195    # sells only.
FINRA_TAF_CAP = 9.79              # max per trade.
FINRA_CAT_PER_SHARE = 0.000003    # both buys + sells.

# Tiered-only: clearing + pass-through.
NSCC_DTC_PER_SHARE = 0.00020      # clearing.
NYSE_PASS_THROUGH = 0.000175      # x commission.
FINRA_PASS_THROUGH = 0.00056      # x commission.

TRADE_VALUE_CAP = 0.01            # 1% of trade value (sub-dollar stocks).


class CommissionModel(str, Enum):
    Fixed = "fixed"
    Tiered = "tiered"

    @property
    def per_share(self) -> float:
        return 0.005 if self is CommissionModel.Fixed else 0.0035

    @property
    def min_per_order(self) -> float:
        return 1.00 if self is CommissionModel.Fixed else 0.35

    @classmethod
    def from_str(cls, s: str) -> "CommissionModel":
        try:
            return cls(s.lower())
        except ValueError:
            raise ValueError(f"unknown commission model {s!r} (expected 'fixed' or 'tiered')")


def commission(qty: float, price: float, model: CommissionModel = CommissionModel.Tiered) -> float:
    """IBKR Pro commission for a single fill. `qty` signed (+buy, -sell).

    Defaults to Tiered (the Rust `BacktestConfig` default)."""
    trade_value = abs(qty) * price
    base = min(max(abs(qty) * model.per_share, model.min_per_order), TRADE_VALUE_CAP * trade_value)

    total = base + FINRA_CAT_PER_SHARE * abs(qty)  # CAT — both buys + sells

    if qty < 0.0:  # sells only: SEC fee + FINRA TAF
        total += SEC_FEE_RATE * trade_value
        total += min(FINRA_TAF_PER_SHARE * abs(qty), FINRA_TAF_CAP)

    if model is CommissionModel.Tiered:
        total += NSCC_DTC_PER_SHARE * abs(qty)
        total += NYSE_PASS_THROUGH * base
        total += FINRA_PASS_THROUGH * base

    return total


__all__ = ["CommissionModel", "commission"]
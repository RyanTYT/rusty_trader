"""Regression goldens for the cost + metrics parity ports (Rust fill_model.rs /
results.rs). Run directly: `.venv/bin/python stratlab/tests/test_core.py`."""

from __future__ import annotations

from stratlab.cost import CommissionModel, commission, decide_fill
from stratlab.metrics import (
    bars_per_year, max_drawdown_pct, realized_per_trade_pnl, compute_results,
)


def test_commission_fixed():
    # buy 100 @ 50, Fixed: base=max(0.5, 1.0)=1.0 + CAT 0.0003
    assert abs(commission(100.0, 50.0, CommissionModel.Fixed) - 1.0003) < 1e-9


def test_commission_tiered():
    # base 0.35 + CAT 0.0003 + NSCC 0.02 + NYSE 0.00006125 + FINRA 0.000196
    assert abs(commission(100.0, 50.0, CommissionModel.Tiered) - 0.37055725) < 1e-9


def test_commission_sell_fixed():
    # + SEC 0.103 + TAF min(0.0195, 9.79)=0.0195
    assert abs(commission(-100.0, 50.0, CommissionModel.Fixed) - 1.1228) < 1e-9


def test_decide_fill_market():
    f = decide_fill(is_buy=True, qty=10, limit_price=None, low=99, high=101, close=100, slippage_bps=2)
    assert f.filled and f.fill_qty == 10 and abs(f.fill_price - 100.02) < 1e-12


def test_decide_fill_limit_cross():
    f = decide_fill(is_buy=True, qty=10, limit_price=99.5, low=99, high=101, close=100, slippage_bps=2)
    assert f.filled and abs(f.fill_price - 99.5) < 1e-12
    f2 = decide_fill(is_buy=True, qty=10, limit_price=99.5, low=99.6, high=101, close=100, slippage_bps=2)
    assert not f2.filled


def test_metrics():
    tx = [("A", "NASDAQ", "USD", 10.0, 100.0, 1.0), ("A", "NASDAQ", "USD", -10.0, 103.0, 1.0)]
    assert abs(realized_per_trade_pnl(tx)[0] - 29.0) < 1e-9
    assert bars_per_year(5) == 19656.0
    assert bars_per_year(1440) == 252.0
    eq = [{"equity": 100.0}, {"equity": 90.0}, {"equity": 95.0}]
    assert abs(max_drawdown_pct([s["equity"] for s in eq]) - 10.0) < 1e-9
    r = compute_results(equity_curve=eq, transactions=tx, starting_capital=100.0, bar_interval_minutes=5)
    assert r.num_trades == 2 and r.num_closed_trades == 1 and r.win_rate_pct == 100.0


if __name__ == "__main__":
    import sys
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"{len(fns)} passed")
    sys.exit(0)
"""Regression goldens for the cost + metrics parity ports (Rust fill_model.rs /
results.rs). Run directly: `.venv/bin/python stratlab/tests/test_core.py`."""

from __future__ import annotations

from stratlab.cost import CommissionModel, commission, decide_fill
from stratlab.cost.fill import BarExtremeFillModel, BarOhlc, StopMarketFillModel
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


def _bar(o, h, l, c):
    return BarOhlc(open=o, high=h, low=l, close=c)


def test_bar_extreme_gap_through_fills_at_printed_extreme():
    # Sell stop at 99.5 breached by low 99.0 -> fills at the bar LOW.
    r = BarExtremeFillModel(stop=99.5, is_buy=False).evaluate(_bar(100.0, 101.0, 99.0, 100.5))
    assert r.kind == "fill" and abs(r.price - 99.0) < 1e-12
    # Buy stop at 101.0 breached by high 102.0 -> fills at the bar HIGH.
    r2 = BarExtremeFillModel(stop=101.0, is_buy=True).evaluate(_bar(100.0, 102.0, 99.5, 101.0))
    assert r2.kind == "fill" and abs(r2.price - 102.0) < 1e-12


def test_bar_extreme_no_breach_rests():
    b = _bar(100.0, 100.5, 99.6, 100.0)
    assert BarExtremeFillModel(99.0, False).evaluate(b).kind == "rest"
    assert BarExtremeFillModel(101.0, True).evaluate(b).kind == "rest"


def test_bar_extreme_graze_fills_at_stop():
    # Low exactly at the stop: the extreme IS the stop, so the two conventions
    # differ ONLY on gapping bars.
    r = BarExtremeFillModel(stop=99.0, is_buy=False).evaluate(_bar(100.0, 100.5, 99.0, 99.8))
    assert abs(r.price - 99.0) < 1e-12


def test_bar_extreme_never_fills_better_than_its_stop():
    # Pathological bar where the 'extreme' is on the favourable side of the
    # stop; the clamp must hold.
    r = BarExtremeFillModel(stop=99.0, is_buy=False).evaluate(_bar(100.0, 100.5, 99.9, 100.0))
    assert r.price <= 99.0
    # On the buy side the clamp is unreachable by construction (the high must
    # reach the stop to trigger), so assert the invariant directly.
    r2 = BarExtremeFillModel(stop=101.0, is_buy=True).evaluate(_bar(100.0, 101.4, 99.5, 101.2))
    assert r2.kind == "fill" and r2.price >= 101.0


def test_bar_extreme_strictly_better_than_stopmarket_on_gap():
    b = _bar(100.0, 101.0, 99.0, 100.5)
    ext = BarExtremeFillModel(99.5, False).evaluate(b).price
    # StopMarketFillModel prices off stop - pct*(high-low): 99.5 - 0.5*2.0 = 98.5,
    # BELOW the bar's low, i.e. a price that never traded.
    smm = StopMarketFillModel(stop=99.5, pct_of_bar=0.5, is_buy=False).evaluate(b)
    assert ext >= smm.price
    assert smm.price < b.low
    # Ladder on this bar (lower = worse for a seller): StopMarket 98.5 <
    # bar_extreme 99.0 < exact 99.5 <= close 100.5.
    assert 98.5 <= ext <= 99.5 <= 100.5


if __name__ == "__main__":
    import sys
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"{len(fns)} passed")
    sys.exit(0)
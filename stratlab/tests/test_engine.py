"""Engine tests: fast mode + thorough mode (targets, FX, OCA brackets)."""

from __future__ import annotations

import math

import pandas as pd
from stratlab.cost import CommissionModel, commission
from stratlab.engine import (
    LMT,
    MKT,
    STP,
    EngineConfig,
    Order,
    Outcome,
    backtest_fast,
    fx_to_sgd,
    run_thorough,
)
from stratlab.io import Universe, _attach_et
from stratlab.metrics import realized_per_trade_pnl


def _make_universe(rows) -> Universe:
    """rows = list of (hh, mm, open, high, low, close)."""
    out = []
    for hh, mm, o, h, l, c in rows:
        et = pd.Timestamp(f"2024-01-02 {hh:02d}:{mm:02d}", tz="America/New_York")
        ts = int(et.tz_convert("UTC").value // 1_000_000)
        out.append((ts, "T", o, h, l, c, 1000.0))
    df = pd.DataFrame(out, columns=["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"])
    return Universe.from_long(_attach_et(df))


def _single_symbol_universe() -> Universe:
    return _make_universe([
        (9, 30, 100.0, 101.0, 99.0, 100.0),
        (9, 35, 101.0, 102.0, 100.0, 101.0),
        (9, 40, 102.0, 103.0, 101.0, 102.0),
    ])


class BuyThenSell:
    def on_row(self, bars, ctx):
        if ctx.clock.mso == 0:
            return Outcome.set_targets({"T": 10.0})
        if ctx.clock.mso >= 10:
            return Outcome.set_targets({"T": 0.0})
        return Outcome.no_action()


def _test_fast_flat():
    uni = _single_symbol_universe()
    signal = pd.DataFrame({"T": [0.0] * len(uni.grid_close)}, index=uni.grid_close.index)
    r = backtest_fast(signal, uni, starting_equity=1000.0, bps_per_side=5.0)
    assert r.metrics["n_trades"] == 0
    assert abs(r.equity.iloc[-1] - 1000.0) < 1e-9
    assert math.isnan(r.metrics["sharpe_ann"])


def _test_default_fx():
    # USD -> SGD = 1.2713 (the Rust default_fx_map value)
    assert abs(fx_to_sgd("USD") - 1.2713) < 1e-12
    assert fx_to_sgd("SGD") == 1.0
    assert abs(EngineConfig().fx_rate("USD") - 1.2713) < 1e-12


def _test_thorough_buy_sell():
    uni = _single_symbol_universe()
    # fx = identity keeps this a pure fill/commission/PnL golden (in USD).
    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(uni, BuyThenSell(), cfg)

    r = res.results
    assert r.num_trades == 2
    assert r.num_closed_trades == 1
    assert abs(res.state.position_qty("T")) < 1e-9

    buy_fees = commission(10.0, 100.0, CommissionModel.Tiered)
    sell_fees = commission(-10.0, 102.0, CommissionModel.Tiered)
    pnl = realized_per_trade_pnl(res.state.transactions)
    assert len(pnl) == 1
    assert abs(pnl[0] - ((102.0 - 100.0) * 10.0 - sell_fees)) < 1e-9

    expected = 1000.0 + (102.0 - 100.0) * 10.0 - buy_fees - sell_fees
    assert abs(r.final_equity - expected) < 1e-6


class BracketStrategy:
    """Market entry 10 @ bar0; stop 98 (sell) + take-profit 101 (sell). TP
    crosses bar1 -> the stop sibling should be cancelled, not filled on bar2."""

    def __init__(self):
        self.fired = False

    def on_row(self, bars, ctx):
        if not self.fired and ctx.clock.mso == 0:
            self.fired = True
            return Outcome.emit_orders([
                Order("T", True, 10.0, order_type=MKT, references_parent_order=-1),
                Order("T", False, 10.0, order_type=STP, stop=98.0, references_parent_order=0),
                Order("T", False, 10.0, order_type=LMT, limit_price=101.0, references_parent_order=0),
            ])
        return Outcome.no_action()


def _test_thorough_bracket():
    # bar2 has low 88 < 98, which WOULD trigger the stop unless it was cancelled
    # by the TP fill on bar1.
    uni = _make_universe([
        (9, 30, 100.0, 100.5, 99.5, 100.0),
        (9, 35, 101.0, 102.0, 100.0, 101.0),   # TP 101 crosses here
        (9, 40, 90.0, 91.0, 88.0, 90.0),       # would hit stop 98 if not cancelled
    ])
    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(uni, BracketStrategy(), cfg)

    assert res.results.num_trades == 2               # entry + TP only (stop cancelled)
    assert abs(res.state.position_qty("T")) < 1e-9   # flat
    pnl = realized_per_trade_pnl(res.state.transactions)
    assert len(pnl) == 1
    sell_fees = commission(-10.0, 101.0, CommissionModel.Tiered)
    assert abs(pnl[0] - ((101.0 - 100.0) * 10.0 - sell_fees)) < 1e-9


class LimitEntryBracketStrategy:
    """LIMIT entry 10 @ 99.5 (rests, no slippage) + TP 103 + stop 95. Exercises
    the entry-resting -> fill -> arm-exits path in `_process_resting`."""

    def __init__(self):
        self.fired = False

    def on_row(self, bars, ctx):
        if not self.fired and ctx.clock.mso == 0:
            self.fired = True
            return Outcome.emit_orders([
                Order("T", True, 10.0, order_type=LMT, limit_price=99.5, references_parent_order=-1),
                Order("T", False, 10.0, order_type=STP, stop=95.0, references_parent_order=0),
                Order("T", False, 10.0, order_type=LMT, limit_price=103.0, references_parent_order=0),
            ])
        return Outcome.no_action()


def _test_thorough_limit_entry_bracket():
    uni = _make_universe([
        (9, 30, 100.0, 100.5, 100.0, 100.0),  # limit 99.5 not crossed yet
        (9, 35, 99.0, 100.0, 98.0, 99.0),     # limit 99.5 crossed -> buy @ 99.5
        (9, 40, 103.0, 104.0, 102.0, 103.0),  # TP 103 crossed -> sell @ 103
    ])
    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(uni, LimitEntryBracketStrategy(), cfg)

    assert res.results.num_trades == 2
    assert abs(res.state.position_qty("T")) < 1e-9
    pnl = realized_per_trade_pnl(res.state.transactions)
    assert len(pnl) == 1
    # entry was a LIMIT -> fills at 99.5 exactly (no slippage)
    entry_price = res.state.transactions[0][4]
    assert abs(entry_price - 99.5) < 1e-12
    sell_fees = commission(-10.0, 103.0, CommissionModel.Tiered)
    assert abs(pnl[0] - ((103.0 - 99.5) * 10.0 - sell_fees)) < 1e-9


if __name__ == "__main__":
    _test_fast_flat(); print("PASS  test_fast_flat")
    _test_default_fx(); print("PASS  test_default_fx")
    _test_thorough_buy_sell(); print("PASS  test_thorough_buy_sell")
    _test_thorough_bracket(); print("PASS  test_thorough_bracket")
    _test_thorough_limit_entry_bracket(); print("PASS  test_thorough_limit_entry_bracket")
    print("5 passed")

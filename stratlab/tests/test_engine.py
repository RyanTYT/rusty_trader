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
from stratlab.engine.thorough import MarketState, RESTING
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
    # fill_lag_bars=0: golden written pre-lag feature; lag0 = its intended
    # regime (the working tree's default lag1 would settle the buy a row
    # late and the final sell past the tape — stale expectation otherwise).
    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0,
                       fill_lag_bars=0)
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


class _HoldForever:
    """Buys once and never sells, so a position is still open at the end."""

    def __init__(self):
        self.fired = False

    def on_row(self, bars, ctx):
        if not self.fired:
            self.fired = True
            return Outcome("emit_orders", None, [
                Order(symbol="T", is_buy=True, qty=10)])
        return Outcome.no_action()


def _test_open_position_is_still_valued_when_its_symbol_stops_printing():
    """A position must not evaporate because its symbol is absent from a bar.

    Regression. A bar's price map holds only the symbols that printed on THAT
    timestamp, and both the equity snapshot and the account handed to the
    strategy used to skip any position they did not find there -- valuing it at
    ZERO, silently. A 40-asset VP run closed round-trips for +$10,731 and
    reported final equity of -$23,197: exactly the cash balance, which is
    impossible with positions open. One 14,859-share position that stopped
    printing was worth ~$84k and had vanished from the curve.

    Here "T" buys on bar 0 and prints again only on bar 2, so bar 1 must still
    value it at the last mark rather than at zero.
    """
    rows = []
    for hh, mm, c in ((9, 30, 100.0), (9, 40, 102.0)):
        et = pd.Timestamp(f"2024-01-02 {hh:02d}:{mm:02d}", tz="America/New_York")
        ts = int(et.tz_convert("UTC").value // 1_000_000)
        rows.append((ts, "T", c, c + 1, c - 1, c, 1000.0))
    # A bar where only "OTHER" prints. "T" is absent from that timestamp's
    # price map while a position in it is open -- exactly the condition that
    # used to value it at zero.
    mid_et = pd.Timestamp("2024-01-02 09:35", tz="America/New_York")
    rows.append((int(mid_et.tz_convert("UTC").value // 1_000_000),
                 "OTHER", 50.0, 51.0, 49.0, 50.0, 1000.0))
    df = pd.DataFrame(rows, columns=["ts_epoch_ms", "asset", "open", "high",
                                     "low", "close", "volume"])
    uni = Universe.from_long(_attach_et(df))

    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(uni, _HoldForever(), cfg)

    assert res.state.position_qty("T") == 10.0, "the position was not held"
    curve = res.state.equity_curve
    assert len(curve) == 3

    buy_fees = commission(10.0, 100.0, CommissionModel.Tiered)
    # The middle bar is the one where "T" is absent from the price map. It must
    # still be carried at 100.0 -- its last print -- so positions_value is
    # 1,000.00 and NOT 0.
    mid = curve[1]
    assert abs(mid["positions_value"] - 1000.0) < 1e-9, (
        f"the open position was dropped from the mark: "
        f"positions_value={mid['positions_value']} (expected 1000.0)"
    )
    assert "unpriced" not in mid, "a last price was available but not used"
    assert abs(mid["equity"] - (1000.0 - 1000.0 - buy_fees + 1000.0)) < 1e-6

    # Equity must be cash + the value of EVERYTHING still open. Before the fix
    # this identity broke the moment a symbol went quiet.
    for row in curve:
        assert abs(row["equity"] - (row["cash"] + row["positions_value"])) < 1e-9

    assert abs(res.results.final_equity - curve[-1]["equity"]) < 1e-9
    assert "unpriced" not in curve[-1]


class _HedgeBothSides:
    """Opens a long AND a short bracket on the same instrument, then holds.

    The long's stop sits below the entry and the short's above, so the bar's
    range touches the short's stop and leaves the long's alone.
    """

    def __init__(self):
        self.fired = False

    def on_row(self, bars, ctx):
        if not self.fired:
            self.fired = True
            return Outcome("emit_orders", None, [
                # long bracket: buy, protected by a SELL stop below
                Order(symbol="T", is_buy=True, qty=10),
                Order(symbol="T", is_buy=False, qty=10, order_type=STP,
                      stop=98.0, references_parent_order=0),
                # short bracket: sell, protected by a BUY stop above
                Order(symbol="T", is_buy=False, qty=10),
                Order(symbol="T", is_buy=True, qty=10, order_type=STP,
                      stop=102.0, references_parent_order=0),
            ])
        return Outcome.no_action()


def _test_hedged_lots_keep_both_exits_armed():
    """A long and a short in one instrument must BOTH keep their stops.

    Regression. `positions` is net by design, and the exit-arming check used to
    read the net: +10 long with -10 short nets to 0, which looks flat, so BOTH
    stops were cancelled as "position gone" and the lots were stranded with no
    exit at all. Exits are now armed against gross per-side inventory.

    The study needs this -- 63% of its multi-lot asset-dates hold both sides.
    """
    uni = _make_universe([
        (9, 30, 100.0, 101.0, 99.0, 100.0),
        (9, 35, 100.0, 104.0, 99.5, 103.0),   # touches the short's stop at 102
        (9, 40, 103.0, 104.0, 102.0, 103.0),
    ])
    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(uni, _HedgeBothSides(), cfg)

    st = res.state
    assert st.long_qty.get("T", 0.0) == 10.0, (
        f"the long lot is gone: {st.long_qty}"
    )
    assert st.short_qty.get("T", 0.0) == 0.0, (
        f"the short's stop never fired; short inventory is {st.short_qty}"
    )
    # The NET is the long, because the short was covered.
    assert st.positions.get("T", 0.0) == 10.0, (
        f"net position is {st.positions.get('T')}, expected the surviving long"
    )
    # And the short's stop produced a real closing transaction.
    sells = [t for t in st.transactions if t[3] < 0]
    assert sells, "nothing ever sold -- the short was never closed"


def _test_snapshot_reports_symbols_it_cannot_value():
    """Unpriceable positions are named, not assumed worthless."""
    st = MarketState(EngineConfig(starting_equity=1000.0, fx=lambda c: 1.0))
    st.positions["GONE"] = 10.0        # never given a price
    st.positions["T"] = 5.0
    st.mark({"T": 100.0})

    class _C:
        time_ms = 0

    st.snapshot(_C(), {"T": 101.0})
    row = st.equity_curve[-1]
    assert row.get("unpriced") == ["GONE"], (
        f"an unpriceable position was not reported: {row}"
    )
    assert abs(row["positions_value"] - 505.0) < 1e-9


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


def _test_emit_mkt_active_after_is_honored():
    """A market ENTRY with `active_after` must NOT fill on the emit bar.

    Regression. The immediate-fill loop in `_reconcile_orders` never checked
    `active_after`, so an MKT bracket emitted at bar 0 armed for bar 2 filled
    at bar 0's close anyway -- invisibly converting a planned late entry into
    an early one. The resting LMT path (and every exit path) had the gate;
    only this loop missed it. Post-fix the MKT stays RESTING (and therefore
    visible in ctx.account.open_orders, inert) until its first active bar.
    """
    day = "2024-01-02"
    ts_ = [int(pd.Timestamp(f"{day} {hh:02d}:{mm:02d}",
                            tz="America/New_York").tz_convert("UTC").value // 1_000_000)
           for hh, mm in ((9, 30), (9, 35), (9, 40), (9, 45))]

    def _rows():
        return _make_universe([
            (9, 30, 100.0, 100.5, 99.5, 100.0),
            (9, 35, 101.0, 101.3, 100.4, 101.0),
            (9, 40, 102.0, 102.5, 101.8, 102.0),   # gate opens here
            (9, 45, 103.0, 103.5, 102.8, 103.0),
        ])

    seen = []

    class _GatedMkt:
        def on_row(self, bars, ctx):
            seen.append(list(ctx.account.open_orders))
            if ctx.clock.mso == 0:
                return Outcome.emit_orders([
                    Order("T", True, 5.0, order_type=MKT,
                          active_after=ts_[2], order_ref="gated")
                ])
            return Outcome.no_action()

    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(_rows(), _GatedMkt(), cfg)
    st = res.state

    # the fill lands at the ACTIVE bar's close (102.0), not the emit bar's
    # (100.0); exactly one fill
    assert [(f["qty"], f["price"], f.get("order_ref")) for f in st.fills] \
        == [(5.0, 102.0, "gated")], st.fills
    assert st.position_qty("T") == 5.0
    # before the gate: inert but VISIBLE in the open-order book
    assert seen[0] == []                                  # ctx pre-emit
    assert seen[1] == [{"symbol": "T", "is_buy": True, "qty": 5.0,
                        "order_ref": "gated", "role": "entry"}], seen[1]
    assert seen[2][0]["qty"] == 5.0                       # still resting pre-fill
    # after the gate: armed exit siblings (inactive until fill) still show
    # resting in the book; the entry itself is gone post-fill
    assert seen[3] == [] or all(o["role"] != "entry" for o in seen[3])


class _MixedFillProbe:
    """Per-order lifecycle probe: an LMT entry with ttl=2 rests, ages two
    prints without crossing, then falls back to market at close+slip; a
    second resting LMT is cancelled mid-run. Verifies order_ref attribution
    (fills) and the strategy-visible open-orders book."""

    def __init__(self):
        self.seen_open = []

    def on_row(self, bars, ctx):
        self.seen_open.append(list(ctx.account.open_orders))
        if ctx.clock.mso == 0:
            return Outcome.emit_orders([
                Order("T", True, 10.0, order_type=LMT, limit_price=99.5,
                      ttl=2, order_ref="macd"),
                Order("T", True, 5.0, order_type=LMT, limit_price=95.0,
                      ttl=99, order_ref="connors"),
            ])
        if ctx.clock.mso == 15:
            return Outcome.emit_orders([
                Order("T", True, 0.0, cancel=True, order_ref="connors"),
            ])
        return Outcome.no_action()


def _test_emit_order_ref_ttl_and_open_orders():
    # All 4 bars stay at/above 100: neither limit ever crosses.
    # macd (ttl=2) ages on bars 1+2 then falls back to market at bar2 close;
    # connors (ttl=99) is cancelled by the placeholder order on bar 4.
    uni = _make_universe([
        (9, 30, 100.0, 100.5, 100.0, 100.0),
        (9, 35, 101.0, 101.5, 100.5, 101.0),
        (9, 40, 102.0, 102.5, 101.5, 102.0),
        (9, 45, 103.0, 103.5, 102.5, 103.0),
    ])
    probe = _MixedFillProbe()
    cfg = EngineConfig(starting_equity=1000.0, slippage_bps=0.0,
                       commission_model=CommissionModel.Tiered, fx=lambda c: 1.0)
    res = run_thorough(uni, probe, cfg)
    st = res.state

    # order_ref travels into the fills log
    ref_fills = [f for f in st.fills if f.get("order_ref") == "macd"]
    assert len(ref_fills) == 1
    f = ref_fills[0]
    assert f["qty"] == 10.0 and f["is_buy"]
    assert abs(f["price"] - 102.0) < 1e-9       # market fallback at close
    assert not [f for f in st.fills if f.get("order_ref") == "connors"], (
        "the cancelled order must never fill")
    assert st.position_qty("T") == 10.0

    # fill counters: 1 fell back, 1 cancelled, 0 crossed (limit_placed is a
    # set_targets-path counter only — the emit path has never counted it)
    fc = st.fill_counters
    assert fc["limit_fell_back"] == 1 and fc["limit_cancelled"] == 1
    assert fc["limit_cross"] == 0

    # open-orders book visible to the strategy, per-order attributed:
    # bar1 sees both rests, bar4 (mso 15) sees only the surviving connors one
    refs1 = {o["order_ref"] for o in probe.seen_open[1]}
    assert refs1 == {"macd", "connors"}
    qtys = {(o["order_ref"], o["qty"]) for o in probe.seen_open[1]}
    assert qtys == {("macd", 10.0), ("connors", 5.0)}
    refs3 = {o["order_ref"] for o in probe.seen_open[3]}
    assert refs3 == {"connors"}
    # nothing left resting after the cancel
    assert not [ro for ro in st.resting if ro.status == RESTING]


if __name__ == "__main__":
    _test_fast_flat(); print("PASS  test_fast_flat")
    _test_default_fx(); print("PASS  test_default_fx")
    _test_thorough_buy_sell(); print("PASS  test_thorough_buy_sell")
    _test_thorough_bracket(); print("PASS  test_thorough_bracket")
    _test_thorough_limit_entry_bracket(); print("PASS  test_thorough_limit_entry_bracket")
    _test_open_position_is_still_valued_when_its_symbol_stops_printing()
    print("PASS  test_open_position_is_still_valued_when_its_symbol_stops_printing")
    _test_snapshot_reports_symbols_it_cannot_value()
    print("PASS  test_snapshot_reports_symbols_it_cannot_value")
    _test_hedged_lots_keep_both_exits_armed()
    print("PASS  test_hedged_lots_keep_both_exits_armed")
    _test_emit_order_ref_ttl_and_open_orders()
    print("PASS  test_emit_order_ref_ttl_and_open_orders")
    _test_emit_mkt_active_after_is_honored()
    print("PASS  test_emit_mkt_active_after_is_honored")
    print("9 passed")

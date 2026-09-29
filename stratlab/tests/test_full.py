"""Full-stats tests: trade matching + positive-PnL-by-N + full_report."""

from __future__ import annotations

import pandas as pd

from stratlab.io import Universe, _attach_et
from stratlab.engine import EngineConfig, Outcome, run_thorough
from stratlab.metrics.full import close_trades, positive_pnl_by_n, full_report


def _uni(days):
    """days = list of (date_str, [close0, close1, close2]) -> single 'T' universe."""
    rows = []
    for date_str, closes in days:
        for k, close in enumerate(closes):
            hh, mm = 9, 30 + 5 * k
            et = pd.Timestamp(f"{date_str} {hh:02d}:{mm:02d}", tz="America/New_York")
            ts = int(et.tz_convert("UTC").value // 1_000_000)
            rows.append((ts, "T", close, close + 1.0, close - 1.0, close, 1000.0))
    df = pd.DataFrame(rows, columns=["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"])
    return Universe.from_long(_attach_et(df))


def test_close_trades():
    uni = _uni([("2024-01-02", [100.0, 101.0, 102.0])])
    tss = sorted(uni.long["ts_epoch_ms"].unique())
    fills = [
        {"time_ms": int(tss[0]), "symbol": "T", "qty": 5.0, "price": 100.0, "fees": 1.0, "is_buy": True},
        {"time_ms": int(tss[2]), "symbol": "T", "qty": -5.0, "price": 103.0, "fees": 1.0, "is_buy": False},
    ]
    trades = close_trades(fills)
    assert len(trades) == 1
    t = trades[0]
    assert t.side == 1 and t.qty == 5.0
    assert t.entry_price == 100.0 and t.exit_price == 103.0
    assert abs(t.gross_pnl - 15.0) < 1e-12
    assert abs(t.net_pnl - (15.0 - 2.0)) < 1e-12  # gross - entry fees - exit fees


def test_positive_pnl_by_n():
    uni = _uni([("2024-01-02", [100.0, 101.0, 102.0]), ("2024-01-03", [103.0, 104.0, 105.0])])
    entry_ts = int(uni.long["ts_epoch_ms"].min())
    r = positive_pnl_by_n([(entry_ts, "T", 1)], uni, by_bars=(1, 3, 5), by_days=(1,))
    assert r["n_entries"] == 1
    assert abs(r["by_bars"][1]["mean"] - 0.01) < 1e-12      # 101/100 - 1
    assert abs(r["by_bars"][5]["mean"] - 0.05) < 1e-12      # 105/100 - 1
    assert r["by_bars"][1]["p_positive"] == 1.0
    assert abs(r["by_days"][1]["mean"] - 0.05) < 1e-12      # day 2 close 105/100 - 1


class BuySell:
    def __init__(self):
        self.start_date = None
        self.done = False

    def on_row(self, bars, ctx):
        if self.start_date is None:
            self.start_date = ctx.clock.et_date
        if ctx.clock.et_date == self.start_date and ctx.clock.mso == 0 and not self.done:
            return Outcome.set_targets({"T": 1.0})
        if ctx.clock.et_date != self.start_date and ctx.clock.mso >= 10:
            self.done = True
            return Outcome.set_targets({"T": 0.0})
        return Outcome.no_action()


def test_full_report_engine():
    uni = _uni([("2024-01-02", [100.0, 101.0, 102.0]), ("2024-01-03", [103.0, 104.0, 105.0])])
    res = run_thorough(uni, BuySell(), EngineConfig(starting_equity=1000.0, slippage_bps=0.0))
    report = full_report(uni, res.state.fills, equity_curve=res.results.equity_curve)
    assert report["trade_summary"]["n_closed"] == 1
    assert report["positive_pnl_by_n"]["n_entries"] == 1
    assert "calmar" in report["portfolio_risk"]
    assert report["trade_summary"]["win_rate"] == 1.0  # bought 100 -> sold 105


if __name__ == "__main__":
    test_close_trades()
    print("PASS  test_close_trades")
    test_positive_pnl_by_n()
    print("PASS  test_positive_pnl_by_n")
    test_full_report_engine()
    print("PASS  test_full_report_engine")
    print("3 passed")
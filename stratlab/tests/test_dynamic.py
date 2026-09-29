"""Dynamic output-goal metrics tests: forward bps (bars/days/EoD) + conditional."""

from __future__ import annotations

import pandas as pd

from stratlab.io import Universe, _attach_et
from stratlab.metrics.dynamic import forward_bps, mean_bps_to_eod, conditional_by_score


def _uni(days):
    rows = []
    for date_str, closes in days:
        for k, close in enumerate(closes):
            et = pd.Timestamp(f"{date_str} 09:{30 + 5 * k:02d}", tz="America/New_York")
            ts = int(et.tz_convert("UTC").value // 1_000_000)
            rows.append((ts, "T", close, close + 1.0, close - 1.0, close, 1000.0))
    df = pd.DataFrame(rows, columns=["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"])
    return Universe.from_long(_attach_et(df))


def test_forward_bps():
    uni = _uni([("2024-01-02", [100.0, 101.0, 102.0]), ("2024-01-03", [103.0, 104.0, 105.0])])
    entry_ts = int(uni.long["ts_epoch_ms"].min())
    entries = [(entry_ts, "T", 1)]  # long

    r = forward_bps(entries, uni, by_bars=(1, 5), by_days=(1,))
    assert r["n_entries"] == 1

    # EoD = close of day 1 (102): (102/100 - 1) * 1e4 = 200 bps
    assert abs(r["eod"]["mean_bps"] - 200.0) < 1e-9
    assert r["eod"]["p_positive"] == 1.0

    # 1 bar later = 101: +100 bps
    assert abs(r["by_bars"][1]["mean_bps"] - 100.0) < 1e-9
    # 5 bars later = 105: +500 bps
    assert abs(r["by_bars"][5]["mean_bps"] - 500.0) < 1e-9
    # 1 trading day later = day 2 close 105: +500 bps
    assert abs(r["by_days"][1]["mean_bps"] - 500.0) < 1e-9

    assert abs(mean_bps_to_eod(entries, uni)["mean_bps"] - 200.0) < 1e-9


def test_conditional_by_score():
    uni = _uni([("2024-01-02", [100.0, 101.0, 102.0])])
    entry_ts = int(uni.long["ts_epoch_ms"].min())
    buckets = conditional_by_score([(entry_ts, "T", 1)], [0.9], uni, horizon="eod", n_buckets=2)
    assert len(buckets) == 1
    assert buckets[0]["count"] == 1
    assert abs(buckets[0]["mean_bps"] - 200.0) < 1e-9


if __name__ == "__main__":
    test_forward_bps()
    print("PASS  test_forward_bps")
    test_conditional_by_score()
    print("PASS  test_conditional_by_score")
    print("2 passed")
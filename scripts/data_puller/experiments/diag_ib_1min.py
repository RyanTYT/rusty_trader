"""Diagnostic: what does IBKR actually return for a known-truncated day?

Tests several endDateTime anchors / durations against one asset-day so we can
tell whether the step-size theory holds, or whether the truncation has a
different cause. Run inside the ib-gateway-ibc container.
"""
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, "/app")
from ib_async import IB, Stock
from pull_5min_bars import bars_to_rows

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
TARGET = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
DAY = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2021, 3, 23)


def show(label, rows):
    if not rows:
        print(f"  {label:34s} -> 0 bars")
        return
    t = [r["ts_epoch_ms"] for r in rows]
    d0 = datetime.fromtimestamp(min(t) / 1000, tz=UTC).astimezone(ET)
    d1 = datetime.fromtimestamp(max(t) / 1000, tz=UTC).astimezone(ET)
    print(f"  {label:34s} -> {len(rows):4d} bars  "
          f"{d0:%Y-%m-%d %H:%M} .. {d1:%H:%M}")


ib = IB()
ib.connect("127.0.0.1", 4002, clientId=21, timeout=20)
print(f"connected (serverVersion={ib.client.serverVersion()})\n")

c = Stock(TARGET, "SMART", "USD", primaryExchange="NASDAQ")
ib.qualifyContracts(c)
print(f"contract {TARGET}: conId={c.conId} exch={c.exchange} "
      f"primary={c.primaryExchange} currency={c.currency}\n")

print(f"probing {TARGET} on {DAY} (ET)\n")

cases = [
    ("end=16:00  duration=1 D", datetime(DAY.year, DAY.month, DAY.day, 16, 0, tzinfo=ET), "1 D"),
    ("end=16:00  duration=2 D", datetime(DAY.year, DAY.month, DAY.day, 16, 0, tzinfo=ET), "2 D"),
    ("end=23:59  duration=1 D", datetime(DAY.year, DAY.month, DAY.day, 23, 59, 59, tzinfo=ET), "1 D"),
    ("end=23:59  duration=2 D", datetime(DAY.year, DAY.month, DAY.day, 23, 59, 59, tzinfo=ET), "2 D"),
    ("end=16:00  duration=5 D", datetime(DAY.year, DAY.month, DAY.day, 16, 0, tzinfo=ET), "5 D"),
    ("end=16:00  duration=10 D", datetime(DAY.year, DAY.month, DAY.day, 16, 0, tzinfo=ET), "10 D"),
    ("end=16:00  duration=1 M", datetime(DAY.year, DAY.month, DAY.day, 16, 0, tzinfo=ET), "1 M"),
]
for label, end, dur in cases:
    try:
        bars = ib.reqHistoricalData(
            c, endDateTime=end.astimezone(ET).strftime("%Y%m%d %H:%M:%S US/Eastern"),
            durationStr=dur, barSizeSetting="1 min", whatToShow="TRADES",
            useRTH=True, formatDate=2, timeout=60)
        show(label, bars_to_rows(list(bars), TARGET))
    except Exception as e:
        print(f"  {label:34s} -> ERROR {type(e).__name__}: {str(e)[:70]}")
    import time as _t
    _t.sleep(1)

ib.disconnect()

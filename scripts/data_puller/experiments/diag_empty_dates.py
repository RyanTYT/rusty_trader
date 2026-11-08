"""Are the 'empty' repair dates genuinely absent from IBKR's archive, or is
our 1-day request the problem?

For a suspect date, fetch a multi-day window that CONTAINS it. If the
neighbouring days return but the suspect date does not, IBKR truly has no data
for it. If it comes back inside the wider window, our 1-day request is at fault.
"""
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, "/app")
from ib_async import IB, Stock

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
SUSPECTS = [("AAPL", date(2025, 1, 30)), ("AAPL", date(2024, 12, 24)),
            ("ADBE", date(2021, 6, 3)), ("ADBE", date(2021, 5, 5))]

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=31, timeout=20)
print(f"connected (serverVersion={ib.client.serverVersion()})\n")


def fetch(sym, end_dt, dur):
    c = Stock(sym, "SMART", "USD",
              primaryExchange={"AAPL": "NASDAQ", "ADBE": "NASDAQ"}[sym])
    ib.qualifyContracts(c)
    bars = ib.reqHistoricalData(
        c, endDateTime=end_dt.astimezone(ET).strftime("%Y%m%d %H:%M:%S US/Eastern"),
        durationStr=dur, barSizeSetting="1 min", whatToShow="TRADES",
        useRTH=True, formatDate=2, timeout=90)
    out = {}
    for b in bars:
        t = int(b.date) if not hasattr(b.date, "timestamp") else int(b.date.timestamp())
        d = datetime.fromtimestamp(t, tz=UTC).astimezone(ET)
        out[d.date()] = out.get(d.date(), 0) + 1
    return out


for sym, d in SUSPECTS:
    print(f"--- {sym} {d} ({d.strftime('%A')}) ---")
    # 1-day request centred on the suspect date
    one = fetch(sym, datetime(d.year, d.month, d.day, 16, 0, tzinfo=ET), "1 D")
    print(f"  1-day request  : {one}")
    # 9-day window containing it -- if the date exists in the archive it
    # should appear here even if the 1-day request failed
    win = fetch(sym, datetime(d.year, d.month, d.day, 16, 0, tzinfo=ET), "9 D")
    lo = (d - timedelta(days=8)).isoformat()
    print(f"  9-day window   : {len(win)} distinct dates "
          f"from {min(win) if win else '-'} to {max(win) if win else '-'}")
    print(f"    suspect date present in window? {d in win}  "
          f"-> {win.get(d, 'ABSENT')}")
    for day in sorted(win):
        mark = "  <== suspect" if day == d else ""
        print(f"      {day} {day.strftime('%a')}: {win[day]:3d}{mark}")
    print()

ib.disconnect()

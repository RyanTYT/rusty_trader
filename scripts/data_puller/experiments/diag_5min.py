"""Probe how large a 5-min request IBKR will serve, and at what concurrency.

Two questions that determine the whole run:
  1. what durationStr actually returns a COMPLETE span (no silent truncation)?
  2. what request rate does the gateway tolerate before it drops us?

Truncation here is the silent kind -- it returns partial bars with no error --
so each case is validated by checking every session inside the window has the
expected bar count.
"""
import asyncio
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, "/app")
from ib_async import IB, Stock
from pull_5min_bars import bars_to_rows

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
END = datetime(2026, 9, 30, 16, 0, tzinfo=ET)
SYMS = [("AAPL", "NASDAQ"), ("AMD", "NASDAQ"), ("TSLA", "NASDAQ"),
        ("XOM", "NYSE"), ("COIN", "NASDAQ"), ("AMD", "NASDAQ")]


def counts(rows):
    per = Counter()
    for r in rows:
        d = datetime.fromtimestamp(r["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET)
        per[d.date()] += 1
    return per


ib = IB()
ib.connect("127.0.0.1", 4002, clientId=41, timeout=20)
print(f"connected (serverVersion={ib.client.serverVersion()})\n")

# ── 1. max span ─────────────────────────────────────────────────────────────
print("=" * 74)
print("1. HOW LARGE A 5-MIN REQUEST COMES BACK COMPLETE?")
print("=" * 74)
contracts = {}
for sym, ex in SYMS:
    c = Stock(sym, "SMART", "USD", primaryExchange=ex)
    ib.qualifyContracts(c)
    contracts[sym] = c

for days in (30, 60, 90, 120, 180):
    rows = []
    t0 = time.time()
    for sym, _ in SYMS[:3]:
        bars = ib.reqHistoricalData(
            contracts[sym],
            endDateTime=END.strftime("%Y%m%d %H:%M:%S US/Eastern"),
            durationStr=f"{days} D", barSizeSetting="5 mins",
            whatToShow="TRADES", useRTH=True, formatDate=2, timeout=180)
        rows += bars_to_rows(list(bars), sym)
    el = time.time() - t0
    per = counts(rows)
    # a session is complete if it has >=70 bars (5-min: 78 full, 42 half-day)
    full = sum(1 for v in per.values() if v >= 70)
    partial = sum(1 for v in per.values() if v < 70 and v > 0)
    print(f"  {days:>3}D -> {len(rows):>6,} bars | sessions={len(per):>3} "
          f"full={full:>3} partial={partial:>2} | {el:>5.1f}s | "
          f"{len(rows)/max(el,1e-9):>6.0f} bars/s")
    time.sleep(1)

# ── 2. rate tolerance ───────────────────────────────────────────────────────
print()
print("=" * 74)
print("2. SUSTAINED RATE: does the gateway drop us? (5-min, 30D spans)")
print("=" * 74)
tasks = []
for i in range(60):
    sym = ["AAPL", "AMD", "XOM", "SPY"][i % 4]
    day = 40 + (i % 8) * 30          # spread across ~8 different windows
    e = datetime(2026, 1, 1, 16, 0, tzinfo=ET) + timedelta(days=day)
    if e > END:
        e -= timedelta(days=200)
    tasks.append((sym, e))


async def rate_test(conc, delay):
    sem = asyncio.Semaphore(conc)
    ok = {"n": 0, "bars": 0, "empty": 0, "err": 0}

    async def one(sym, e):
        async with sem:
            if delay:
                await asyncio.sleep(delay)
            try:
                bars = await ib.reqHistoricalDataAsync(
                    contracts[sym], e.strftime("%Y%m%d %H:%M:%S US/Eastern"),
                    "30 D", "5 mins", "TRADES", True, 2, False, [], 120)
                if len(bars):
                    ok["n"] += 1
                    ok["bars"] += len(bars)
                else:
                    ok["empty"] += 1
            except Exception:
                ok["err"] += 1

    t0 = time.time()
    await asyncio.gather(*(one(s, e) for s, e in tasks))
    el = time.time() - t0
    return el, ok


for conc, delay in ((8, 0.0), (8, 0.15), (4, 0.35)):
    el, ok = asyncio.get_event_loop().run_until_complete(rate_test(conc, delay))
    tot = len(tasks)
    rate = tot / max(el, 1e-9)
    print(f"  conc={conc:<3} delay={delay:<5} {tot} req in {el:>5.1f}s "
          f"({rate:>4.2f}/s) -> ok={ok['n']:<3} empty={ok['empty']:<3} "
          f"err={ok['err']:<3} empty-rate={100*ok['empty']/tot:>5.1f}%")

ib.disconnect()

"""Minimal window-size test: is one big request faster than many small ones?

FIRST ATTEMPT AT THIS WAS MIS-SIZED
===================================
The previous version walked 12 symbols x 36 windows = 432 SERIAL requests to
compare strategies. At the ~120s/request latency measured on this account that
is ~14 hours to answer a question that needs about six requests. Sizing a
probe by "what would the real run do" is the wrong instinct when the real run
is 26h -- the probe must be smaller than the thing it measures.

THE QUESTION, PRECISELY
=======================
IB's serving rate, not our request count, is suspected to be the ceiling. If
bars/second is flat regardless of window size, then collapsing 35 requests
into 1 saves nothing and we keep the working script.

So: hold the symbol and the span FIXED, vary only the window size, and compare
bars/second. Then confirm the big window returns the SAME bars, because a
truncating large request would otherwise look like a speedup -- that is exactly
the failure that produced 4,154 short asset-days in the 1-min file.

Six requests total. Minutes, not hours.
"""
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app/scripts")
from ib_async import IB, Stock  # noqa: E402
import nest_asyncio  # noqa: E402

UTC = timezone.utc
ET = timezone(timedelta(hours=-4))
START = datetime(2021, 1, 1, tzinfo=UTC)
END = datetime(2026, 9, 25, tzinfo=UTC)
SYM = "QQQ"          # liquid, and the one we actually need pulled
SMALL = "5 mins"


def complete_sessions(bars):
    byday = {}
    for b in bars or []:
        d = b.date.strftime("%Y-%m-%d")
        byday[d] = byday.get(d, 0) + 1
    return len(byday), sum(1 for n in byday.values() if n >= 70)


def sweep(ib, chunk_days):
    """Fetch the whole span in `chunk_days` windows. -> (bars, seconds)"""
    t0 = time.time()
    c = Stock(SYM, "SMART", "USD")
    ib.qualifyContracts(c)
    bars, errs, reqs = [], 0, 0
    cur = END
    while cur > START:
        lo = max(cur - timedelta(days=chunk_days), START)
        endstr = cur.astimezone(ET).strftime("%Y%m%d %H:%M:%S US/Eastern")
        try:
            got = ib.reqHistoricalData(
                c, endstr, f"{chunk_days} D", SMALL, "TRADES", True, 2,
                False, [], 300)
            bars.extend(got or [])
        except Exception as e:  # noqa: BLE001
            errs += 1
            print(f"    err {type(e).__name__} at {endstr}", flush=True)
        reqs += 1
        cur = lo - timedelta(seconds=1)
        ib.sleep(0.3)
    return bars, time.time() - t0, reqs, errs


def main():
    nest_asyncio.apply()
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=45, timeout=60)
    print(f"connected (serverVersion={ib.client.serverVersion()})")
    print(f"{SYM}  {START:%Y-%m-%d}..{END:%Y-%m-%d}  5-min\n", flush=True)

    print(f"{'window':>8} {'reqs':>5} {'secs':>8} {'bars':>9} {'bars/s':>9} "
          f"{'sess':>5} {'full':>5} {'err':>4}")
    results = {}
    for cd in (60, 365, 1200):
        bars, dt, reqs, errs = sweep(ib, cd)
        ns, nf = complete_sessions(bars)
        print(f"{cd:>7}d {reqs:>5} {dt:>8.1f} {len(bars):>9,} "
              f"{len(bars)/dt:>9,.0f} {ns:>5} {nf:>5} {errs:>4}", flush=True)
        results[cd] = (len(bars), dt, ns, nf)
        ib.sleep(1.0)

    print("\n" + "=" * 62)
    print("VERDICT")
    print("=" * 62)
    ref = results[max(results)]
    for cd, (n, dt, ns, nf) in sorted(results.items()):
        ratio = n / ref[0] if ref[0] else 0
        print(f"  {cd:>5}d  {dt:7.1f}s  {n:>9,} bars  "
              f"{'ALL' if ratio > 0.995 else f'{ratio:.1%} OF FULL'}  "
              f"complete {nf}/{ns}")
    small, big = results[60], results[1200]
    if big[0]:
        print(f"\n  bars/s at 60d : {small[0]/small[1]:,.0f}")
        print(f"  bars/s at1200d: {big[0]/big[1]:,.0f}")
        print(f"  time saved    : {small[1]-big[1]:.1f}s "
              f"({small[1]/max(big[1],.1):.1f}x)")
    if big[3] < small[3] * 0.98:
        print("\n  -> LARGE WINDOW TRUNCATES. Do not use it.")
    elif (small[0] / small[1]) / max(big[0] / big[1], 1) < 1.3:
        print("\n  -> bars/s is flat. Fewer requests buys ~nothing; "
              "IB's serving rate is the ceiling. Keep the working script.")
    else:
        print("\n  -> LARGE WINDOWS WIN on throughput.")
    ib.disconnect()


if __name__ == "__main__":
    main()
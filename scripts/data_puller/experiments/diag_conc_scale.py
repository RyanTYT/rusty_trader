"""Does 5-min throughput scale with concurrency, or is IB server-side capped?

The full run measured ~120s median latency per 60-day request at concurrency 8,
i.e. ~0.07 req/s. Extrapolated over the remaining ~22.6M bars that is ~22h,
which is not worth starting blindly. Two very different worlds hide behind that
number:

  * IB paces us -> adding concurrency buys little, and we must simply budget 22h
  * our client is the bottleneck -> concurrency 20+ cuts it to a few hours

So measure bars/s at two concurrency levels on an identical workload, and let
the ratio pick the setting. Same symbols, same window, same bar size, one
variable changed.

Deliberately measures BARS PER SECOND, not requests per second: a request that
returns 14,040 bars and one that returns 0 both cost the same round trip, so
req/s would reward the wrong thing.
"""
import asyncio
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app/scripts")
from ib_async import IB, Stock  # noqa: E402
import nest_asyncio  # noqa: E402

UTC = timezone.utc
ET = timezone(timedelta(hours=-4))  # EDT; windows below are all in DST
BAR_SIZE = "5 mins"
CHUNK_DAYS = 60
WINDOW_END = "20260630 23:59:59 US/Eastern"
N_SYMBOLS = 24

# A spread of liquid + mid caps, so the answer isn't skewed by one name.
SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM",
           "JNJ", "V", "PG", "UNH", "HD", "COST", "BAC", "WMT", "CRM", "ORCL",
           "AMD", "INTC", "CSCO", "PFE", "T", "VZ", "MRK"]


def make(sym: str) -> Stock:
    return Stock(sym, "SMART", "USD")


def one_round(ib: IB, conc: int, delay: float, label: str):
    """One pass over SYMBOLS at the given concurrency. Returns bars/s."""
    sem = asyncio.Semaphore(conc)
    lock = asyncio.Lock()
    last = [0.0]
    tally = {"ok": 0, "empty": 0, "err": 0, "bars": 0}

    async def one(sym):
        if delay:
            async with lock:
                loop = asyncio.get_event_loop()
                w = last[0] + delay - loop.time()
                if w > 0:
                    await asyncio.sleep(w)
                last[0] = loop.time()
        async with sem:
            c = make(sym)
            try:
                ib.qualifyContracts(c)
                bars = await ib.reqHistoricalDataAsync(
                    c, WINDOW_END, f"{CHUNK_DAYS} D", BAR_SIZE,
                    "TRADES", True, 2, False, [], 300)
                n = len(bars or [])
                tally["bars"] += n
                tally["ok" if n else "empty"] += 1
            except Exception:  # noqa: BLE001
                tally["err"] += 1

    async def go():
        await asyncio.gather(*(one(s) for s in SYMBOLS))

    t0 = time.time()
    asyncio.get_event_loop().run_until_complete(go())
    dt = time.time() - t0
    print(f"  conc={conc:<3} delay={delay:<5} {dt:6.1f}s  "
          f"ok={tally['ok']:>3} empty={tally['empty']:>3} err={tally['err']:>3}  "
          f"{tally['bars']:>8,} bars  {tally['bars']/dt:>8,.0f} bars/s", flush=True)
    return tally["bars"] / dt


def main():
    nest_asyncio.apply()
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=41, timeout=60)
    print(f"connected (serverVersion={ib.client.serverVersion()})\n", flush=True)

    print("=" * 74)
    print(f"WARM-UP: one serial pass ({N_SYMBOLS} symbols x {CHUNK_DAYS}D x 5-min)")
    print("=" * 74, flush=True)
    one_round(ib, 1, 0.0, "warmup")   # not scored: fills IB's cache

    rates = {}
    for conc, delay in ((8, 0.35), (24, 0.0)):
        print(f"\n{'=' * 74}\nconc={conc} delay={delay}\n{'=' * 74}",
              flush=True)
        rates[conc] = one_round(ib, conc, delay, f"conc{conc}")

    b8, b24 = rates[8], rates[24]
    ratio = b24 / b8 if b8 else float("inf")
    print(f"\n{'=' * 74}\nVERDICT\n{'=' * 74}")
    print(f"  conc 8  -> {b8:,.0f} bars/s")
    print(f"  conc 24 -> {b24:,.0f} bars/s")
    print(f"  speedup -> {ratio:.2f}x for 3x the concurrency")
    rem = 22.6e6
    for conc, r in rates.items():
        print(f"  at conc {conc:<3} remaining ~22.6M bars = {rem/r/3600:.1f} h")
    if ratio > 1.8:
        print("\n  -> SCALES. Raise concurrency; do not buy concurrency with "
              "delays here.")
    elif ratio > 1.25:
        print("\n  -> PARTIAL. Worth a mid setting (14-16) plus a small delay.")
    else:
        print("\n  -> SERVER-CAPPED. Concurrency buys nothing; the ~20h is "
              "real and must be budgeted or scoped down.")
    ib.disconnect()


if __name__ == "__main__":
    main()
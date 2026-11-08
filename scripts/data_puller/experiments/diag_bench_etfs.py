"""Which benchmark ETFs can we actually pull, and do they cover the size range?

You lean on SPY and QQQ but only SPY is in the data, and nothing covers small
cap. This checks three things per candidate ETF:

  1. RESOLVES   -- qualifyContracts returns a conId (the gate that silently
                   zeroed 12 tickers earlier)
  2. TRADES     -- a real bar count over a recent window, not an empty reply
  3. HISTORY    -- how far back bars go, which decides whether it is usable as
                   a benchmark for a 2021-start backtest

It also pulls a DAILY bar over the full 2021-2026 window for the survivors,
because benchmark coverage is about long-run comparison and intraday history on
an ETF is usually shorter than on its constituents.

Cost note: this is ~14 symbols x 1 request, so minutes, not hours.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ib_async import IB, Stock  # noqa: E402
import nest_asyncio  # noqa: E402

CANDIDATES = [
    # symbol, exchange, what it is for
    ("SPY", "ARCA", "S&P 500 -- you have this"),
    ("QQQ", "NASDAQ", "Nasdaq-100 -- your stated preference, NOT in data"),
    ("RSP", "ARCA", "S&P 500 equal-weight -- removes mega-cap weight bias"),
    ("VTI", "ARCA", "total US market -- broadest single reference"),
    ("IWM", "ARCA", "Russell 2000 -- THE small-cap benchmark"),
    ("IWB", "ARCA", "Russell 1000 -- large/mid cap, Russell family"),
    ("IWD", "ARCA", "Russell 1000 Value"),
    ("IWF", "ARCA", "Russell 1000 Growth"),
    ("IWO", "ARCA", "Russell 2000 Growth"),
    ("IWS", "ARCA", "Russell 2000 Value"),
    ("MDY", "ARCA", "S&P MidCap 400 -- mid-cap benchmark"),
    ("VTWO", "ARCA", "S&P SmallCap 600 -- small-cap, S&P methodology"),
]

FULL_END = "20260925 23:59:59 US/Eastern"
# IB caps the historical window per request, so ask for the whole span at once
# and let it return what it has -- if it truncates, re-chunk before trusting
# the "first bar" date as the fund's true inception.
CHUNK_DAYS = 3700


def probe(ib: IB):
    print("=" * 78)
    print("1. RESOLUTION + RECENT TRADING  (5-min bars, last 10 days)")
    print("=" * 78)
    ok = {}
    for sym, ex, note in CANDIDATES:
        c = Stock(sym, "SMART", "USD", primaryExchange=ex)
        try:
            ib.qualifyContracts(c)
        except Exception as e:  # noqa: BLE001
            print(f"  {sym:<5} EXC {e}", flush=True)
            continue
        conid = getattr(c, "conId", 0)
        if not conid:
            print(f"  {sym:<5} NO CONTRACT ({ex})", flush=True)
            continue
        try:
            bars = ib.reqHistoricalData(
                c, "20260915 23:59:59 US/Eastern", "10 D", "5 mins",
                "TRADES", True, 2, False, [], 60)
        except Exception as e:  # noqa: BLE001
            print(f"  {sym:<5} conId={conid:<10} FETCH ERR {e}", flush=True)
            continue
        n = len(bars or [])
        ok[sym] = c
        print(f"  {sym:<5} conId={conid:<10} bars={n:>6}  {note}", flush=True)
        ib.sleep(0.4)

    print()
    print("=" * 78)
    print("2. FULL-WINDOW HISTORY  (daily bars, 2021-01-01 -> 2026-09-25)")
    print("=" * 78)
    for sym, c in ok.items():
        try:
            bars = ib.reqHistoricalData(
                c, FULL_END, f"{CHUNK_DAYS} D", "1 day",
                "TRADES", True, 2, False, [], 90)
        except Exception as e:  # noqa: BLE001
            print(f"  {sym:<5} ERR {e}", flush=True)
            continue
        n = len(bars or [])
        if n:
            first = min(b.date for b in bars)
            last = max(b.date for b in bars)
            print(f"  {sym:<5} {n:>5} daily bars  {first} -> {last}", flush=True)
        else:
            print(f"  {sym:<5} no daily bars", flush=True)
        ib.sleep(0.4)
    return ok


def main():
    nest_asyncio.apply()
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=44, timeout=60)
    print(f"connected (serverVersion={ib.client.serverVersion()})\n", flush=True)
    probe(ib)
    ib.disconnect()


if __name__ == "__main__":
    main()
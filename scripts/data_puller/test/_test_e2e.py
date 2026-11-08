"""End-to-end run of pull_bars.py main() with a mocked IB Gateway.

Exercises the parts unit tests can't reach: the backwards pagination cursor,
coverage tracking, chunk-file lifecycle, periodic merge, and the final report.
"""
import importlib.util, os, shutil, sys, tempfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

S = "/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/scripts/data_puller"
sys.path.insert(0, S)          # pull_bars imports ib_throttle lazily
spec = importlib.util.spec_from_file_location("PB", f"{S}/pull_bars.py")
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


CALLS = []          # (symbols, end_dt, chunk_days) per request_chunk call
DATES_SEEN = []     # every trading day we hand back


class FakeBar:
    def __init__(self, ts, o, h, l, c, v):
        self.date, self.open, self.high = ts, o, h
        self.low, self.close, self.volume = l, c, v


class FakeIB:
    def disconnect(self):
        pass


def fake_request_chunk(ib, symbols, end_dt, duration_str, throttle, retries,
                       bar_size=None, exchanges=None, errors=None):
    """Mimic IB: return RTH-only 5-min bars for [end-chunk_days, end].

    Two IB behaviours the cursor's termination depends on and the fake must
    reproduce: bars never extend past `end_dt` (a real reply's newest bar is
    <= end), and this mock only has 2024-01-01..2024-03-31 data (older
    windows return nothing, so the earliest-cursor breaks at the start bound
    instead of stalling on a hard data floor).
    """
    chunk_days = int(duration_str.split()[0])
    CALLS.append((list(symbols), end_dt, chunk_days))
    start = end_dt - timedelta(days=chunk_days)
    end_s = int(end_dt.timestamp())
    rows = []
    day = start.date()
    while day <= end_dt.date():
        in_range = datetime(2024, 1, 1).date() <= day <= datetime(2024, 3, 31).date()
        if in_range and day.weekday() < 5:
            DATES_SEEN.append(day)
            base = datetime(day.year, day.month, day.day, 9, 30, tzinfo=ET)
            for i in range(78):  # 09:30 -> 15:55 ET
                t = (base + timedelta(minutes=5 * i)).astimezone(UTC)
                if int(t.timestamp()) > end_s:
                    break  # real IB would not return bars past endDt
                rows.append(FakeBar(int(t.timestamp()), 1., 2., .5, 1.5, 100.))
        day += timedelta(days=1)
    out = {}
    for s in symbols:
        out[s] = P.bars_to_rows(rows, s) if rows else []
    return out


P.request_chunk = fake_request_chunk
P.connect_or_die = lambda *a, **k: FakeIB()

tmpd = tempfile.mkdtemp(prefix="e2e_")
base = os.path.join(tmpd, "raw_bars.parquet")
out = os.path.join(tmpd, "extended.parquet")

# Seed a small base so seeding-from-raw_bars.parquet is exercised. Seed rows
# must themselves be valid RTH bars (09:30-15:55 ET) or the RTH assertions
# below would fail on the seed rather than on the puller's output.
seed_rows = []
_seed_base = datetime(2024, 1, 2, 9, 30, tzinfo=ET)  # a Tuesday
for a in ["AAA", "BBB"]:
    for i in range(10):
        t = (_seed_base + timedelta(minutes=5 * i)).astimezone(UTC)
        seed_rows.append({"ts_epoch_ms": int(t.timestamp() * 1000), "asset": a,
                          "open": 1., "high": 2., "low": .5, "close": 1.5,
                          "volume": 10.})
pd.DataFrame(seed_rows, columns=P.COLS).to_parquet(base, index=False)

sys.argv = ["pull_bars.py", "--tf", "5 mins", "--mode", "full",
            "--start", "2024-01-01", "--end", "2024-03-31",
            "--chunk-days", "30", "--throttle", "0", "--merge-every", "2",
            "--assets", "AAA,BBB,CCC", "--out", out]

print("=" * 70); print("E2E: full main() with mocked gateway"); print("=" * 70)
rc = P.main()
check("main() returned 0", rc == 0, f"rc={rc}")

print(); print("=" * 70); print("pagination behaviour"); print("=" * 70)
check("made multiple chunk requests", len(CALLS) >= 3, f"{len(CALLS)} calls")
ends = [c[1] for c in CALLS]
check("cursor walks BACKWARDS in time", all(ends[i] > ends[i + 1] for i in range(len(ends) - 1)),
      " -> ".join(str(e.date()) for e in ends))
check("chunk-days respected", all(c[2] == 30 for c in CALLS))
steps = [(ends[i] - ends[i + 1]).days for i in range(len(ends) - 1)]
check("each step <= chunk_days (no runaway)", all(s <= 30 for s in steps), f"steps={steps}")
# The final request's window must reach back to (or past) the start bound --
# the loop exits once the cursor can no longer advance, not when cur_end == start.
check("final request reaches the start bound",
      (ends[-1] - timedelta(days=30)).date() <= pd.Timestamp("2024-01-01").date(),
      f"final request {ends[-1].date()} covers from {(ends[-1] - timedelta(days=30)).date()}")

print(); print("=" * 70); print("output correctness"); print("=" * 70)
check("output file exists", os.path.exists(out))
n = pq.ParquetFile(out).metadata.num_rows
df = pd.read_parquet(out)
# 61 weekdays in Jan1-Mar31 2024; seeded rows are at a 2024-01-01 epoch so they
# overlap the mocked window for AAA/BBB.
exp_days = len([d for d in pd.date_range("2024-01-01", "2024-03-31", freq="B")])
print(f"  rows={n:,}  assets={df.asset.nunique()}  trading days mocked={len(set(DATES_SEEN))}")
check("all 3 assets present", set(df.asset.unique()) == {"AAA", "BBB", "CCC"},
      str(sorted(df.asset.unique())))
check("every trading day in range was covered", len(set(DATES_SEEN)) == exp_days,
      f"{len(set(DATES_SEEN))} vs {exp_days} weekdays")
check("row count == 3 assets x trading days x 78 bars",
      n == 3 * exp_days * 78, f"{n:,} vs {3*exp_days*78:,}")
check("no duplicate (ts,asset)", int(df.duplicated(subset=["ts_epoch_ms", "asset"]).sum()) == 0)
check("sorted by (asset, ts)",
      df.sort_values(["asset", "ts_epoch_ms"], kind="stable").index.equals(df.index))
ts = df.ts_epoch_ms.to_numpy()
et = pd.DatetimeIndex(pd.to_datetime(ts, unit="ms", utc=True)).tz_convert(ET)
mod = et.hour * 60 + et.minute
check("all bars inside RTH 09:30-16:00 ET",
      int((mod < 570).sum()) == 0 and int((mod >= 960).sum()) == 0,
      f"range {mod.min()}..{mod.max()}")
check("all bars on 5-min grid", int((ts % 300000 != 0).sum()) == 0)
check("no weekend bars", int((et.dayofweek >= 5).sum()) == 0)
check("schema is canonical", pq.ParquetFile(out).schema_arrow.equals(P.SCHEMA))
check("no leftover chunk files", not [f for f in os.listdir(tmpd) if f.startswith("_5min_chunk")],
      str([f for f in os.listdir(tmpd) if f.startswith("_5min_chunk")]))
check("no leftover .tmp", not os.path.exists(out + ".tmp"))

print(); print("=" * 70); print("idempotency: re-run must not duplicate"); print("=" * 70)
CALLS.clear(); DATES_SEEN.clear()
rc2 = P.main()
df2 = pd.read_parquet(out)
check("second run returns 0", rc2 == 0, f"rc={rc2}")
check("second run adds no rows", len(df2) == n, f"{len(df2):,} vs {n:,}")
check("second run issued no chunk requests", len(CALLS) == 0,
      f"{len(CALLS)} calls (coverage should short-circuit everything)")

shutil.rmtree(tmpd)
print(); print("=" * 70)
print(f"RESULT: {'ALL PASS' if not fails else str(len(fails)) + ' FAILURES: ' + str(fails)}")
print("=" * 70)
sys.exit(1 if fails else 0)

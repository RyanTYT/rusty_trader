"""E2E test of pull_bars.py repair mode with a mocked gateway.

Verifies the audit logic (repairable vs unrecoverable split), the per-day
repair fetch, dedup, and that a repaired day comes back complete.
"""
import importlib.util, os, shutil, sys, tempfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

S = "/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/scripts/data_puller"
sys.path.insert(0, S)          # pull_bars imports ib_throttle lazily
spec = importlib.util.spec_from_file_location("PB", f"{S}/pull_bars.py")
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


def day_bars(day, n=390, start_min=570):
    """`start_min` is minutes-from-midnight ET of the first bar, so a
    late-open fixture actually starts late."""
    base = datetime(day.year, day.month, day.day, tzinfo=ET) + timedelta(minutes=start_min)
    return [int((base + timedelta(minutes=i)).astimezone(UTC).timestamp() * 1000)
            for i in range(n)]


REQUESTED = []


class FakeBar:
    """Mimics ib_async's BarData under formatDate=2: `date` is epoch SECONDS."""

    def __init__(self, ts_ms):
        self.date = ts_ms // 1000          # seconds, as IB sends them
        self.open, self.high = 1., 2.
        self.low, self.close, self.volume = .5, 1.5, 10.


class FakeIB:
    def disconnect(self):
        pass


def make_fetcher(truncated_dates, permission_symbols=frozenset()):
    """Return a fake fetch_day.

    Serves full days except for dates in `truncated_dates` (240 bars). Symbols
    in `permission_symbols` raise an IBKR error-162 style failure, recorded
    via the `errors` out-dict, so permission handling can be exercised.
    """
    def fake(ib, asset, day, *a, **k):
        REQUESTED.append((asset, day, a[3]))   # a[3] = bar_size (exchanges, throttle, retries, bar_size, ...)
        errors = k.get("errors")
        if asset in permission_symbols:
            msg = (f"Error 162: Historical Market Data Service error "
                   f"message:No market data permissions for {asset}")
            if errors is not None:
                errors[asset] = msg
            return []
        n = 240 if day in truncated_dates else 390
        return M.bars_to_rows([FakeBar(ts) for ts in day_bars(day, n)], asset)
    return fake


M.request_chunk = make_fetcher(set())      # kept for parity with old tests
M.connect = lambda *a, **k: True           # kept for parity with old tests

tmpd = tempfile.mkdtemp(prefix="pb_")
path = os.path.join(tmpd, "raw_1min_bars.parquet")

# Build a synthetic file with three defect shapes.
rows = []
# AAA: fully clean
for d in pd.bdate_range("2026-06-01", "2026-06-10"):
    rows += [{"ts_epoch_ms": ts, "asset": "AAA", "open": 1., "high": 2.,
              "low": .5, "close": 1.5, "volume": 10.} for ts in day_bars(d.date())]
# BBB: one truncated day (2026-06-05, ends 13:29) + one clean
for d in pd.bdate_range("2026-06-01", "2026-06-10"):
    if d.date() == pd.Timestamp("2026-06-05").date():
        ts_list = day_bars(d.date(), 240)
    else:
        ts_list = day_bars(d.date())
    rows += [{"ts_epoch_ms": ts, "asset": "BBB", "open": 1., "high": 2.,
              "low": .5, "close": 1.5, "volume": 10.} for ts in ts_list]
# CCC: one late-open day (starts 13:30)
for d in pd.bdate_range("2026-06-01", "2026-06-10"):
    if d.date() == pd.Timestamp("2026-06-03").date():
        ts_list = day_bars(d.date(), 150, start_min=810)   # 13:30 -> 15:59
    else:
        ts_list = day_bars(d.date())
    rows += [{"ts_epoch_ms": ts, "asset": "CCC", "open": 1., "high": 2.,
              "low": .5, "close": 1.5, "volume": 10.} for ts in ts_list]
# DDD: a truncated day far in the past (outside IBKR retention)
old = pd.Timestamp("2021-03-23").date()
rows += [{"ts_epoch_ms": ts, "asset": "DDD", "open": 1., "high": 2.,
          "low": .5, "close": 1.5, "volume": 10.} for ts in day_bars(old, 240)]
pd.DataFrame(rows, columns=M.COLS).to_parquet(path, index=False)

store = M.Bars(out=path, label="1min", duckdb_memory="2GB")

print("=" * 70); print("1. audit: defect detection"); print("=" * 70)
df, counts, t, _b = store.scan()
print(f"  rows={len(df):,} assets={df.asset.nunique()}")
short = counts[(counts > 0) & (counts < 390)]
check("detects 3 short asset-days", len(short) == 3, f"{dict(short)}")
check("BBB truncated day detected",
      short.get(("BBB", pd.Timestamp('2026-06-05').date())) == 240)
check("CCC late-open day detected",
      short.get(("CCC", pd.Timestamp('2026-06-03').date())) == 150)
late = M.late_open_days(t)
check("CCC late-open flagged separately",
      pd.Timestamp('2026-06-03').date() in late.get("CCC", []),
      str({k: [str(d) for d in v] for k, v in late.items()}))

last_day = max(d for _, d in counts.index)
rep = M.find_short_days(counts, 390, last_day)
rep_all = {k: v for k, v in rep.items()}
for a, ds in M.late_open_days(t).items():
    rep_all.setdefault(a, []).extend(ds)
# The 2021 date is older than two years. Under the CORRECTED model it is
# still a candidate: IBKR has no multi-year cutoff for 1-min bars.
check("old (2021) truncated day IS a repair candidate",
      old in rep.get("DDD", []), f"DDD repair days={[str(x) for x in rep.get('DDD', [])]}")
check("recent truncated day is a repair candidate",
      pd.Timestamp('2026-06-05').date() in rep_all.get("BBB", []))
check("no cutoff/unrecoverable concept remains",
      not hasattr(M, "IBKR_1MIN_RETENTION_DAYS"),
      "retention constant removed")

print(); print("=" * 70); print("1b. step-size + error classification"); print("=" * 70)
check("1-min requests are capped at 1 day", M.STEP_TABLE.get("1 min") == 1,
      f"STEP_TABLE['1 min']={M.STEP_TABLE.get('1 min')}")
check("error 162 classified as permission",
      M.classify_error("Error 162: Historical Market Data Service error "
                       "message:No market data permissions for TSLA") == "permission")
check("'not subscribed' classified as permission",
      M.classify_error("You are not subscribed to this data") == "permission")
check("timeout classified as other",
      M.classify_error("TimeoutError: reqHistoricalData timed out") == "other")
check("empty string classified as other", M.classify_error("") == "other")

print(); print("=" * 70); print("2. dry-run plan"); print("=" * 70)
M.connect_or_die = lambda *a, **k: FakeIB()
M.fetch_day = make_fetcher(set())
sys.argv = ["pull_bars.py", "--out", path, "--tf", "1 min", "--dry-run",
            "--mode", "repair"]
rc = M.main()
check("dry-run exits 0", rc == 0, f"rc={rc}")

print(); print("=" * 70); print("3. repair pass with mocked gateway"); print("=" * 70)
REQUESTED.clear()
# Serve full days on repair (nothing truncated).
M.fetch_day = make_fetcher(set())
sys.argv = ["pull_bars.py", "--out", path, "--tf", "1 min", "--mode", "repair",
            "--throttle", "0", "--assets", "BBB,CCC", "--merge-every", "100"]
rc = M.main()
check("repair run exits 0", rc == 0, f"rc={rc}")
check("made one request per short day", len(REQUESTED) == 2, f"{len(REQUESTED)}")
check("used 1-min bar size", all(r[2] == "1 min" for r in REQUESTED),
      str([r[2] for r in REQUESTED]))

print(); print("=" * 70); print("4. repaired output"); print("=" * 70)
df2 = pq.read_table(path).to_pandas()
et2 = pd.DatetimeIndex(pd.to_datetime(df2.ts_epoch_ms.to_numpy(), unit="ms",
                                      utc=True)).tz_convert(ET)
t2 = pd.DataFrame({"asset": df2.asset.to_numpy(), "day": et2.date})
c2 = t2.groupby(["asset", "day"]).size()
check("no duplicate (ts,asset)",
      int(df2.duplicated(subset=["ts_epoch_ms", "asset"]).sum()) == 0)
check("BBB 2026-06-05 now full (390)",
      c2.get(("BBB", pd.Timestamp('2026-06-05').date())) == 390,
      str(c2.get(("BBB", pd.Timestamp('2026-06-05').date()))))
check("CCC 2026-06-03 now full (390)",
      c2.get(("CCC", pd.Timestamp('2026-06-03').date())) == 390,
      str(c2.get(("CCC", pd.Timestamp('2026-06-03').date()))))
check("AAA untouched", c2.get(("AAA", pd.Timestamp('2026-06-01').date())) == 390)
check("DDD old truncated day left as-is (not in --assets filter)",
      c2.get(("DDD", old)) == 240, str(c2.get(("DDD", old))))
check("row count grew by exactly 390-240 + 390-150 = 390",
      len(df2) == len(df) + 150 + 240, f"{len(df2):,} vs {len(df)+390:,}")
mod = et2.hour * 60 + et2.minute
check("all bars still inside RTH",
      int((mod < 570).sum()) == 0 and int((mod >= 960).sum()) == 0,
      f"range {mod.min()}..{mod.max()}")
check("all bars on 1-min grid", int((df2.ts_epoch_ms.to_numpy() % 60000 != 0).sum()) == 0)
check("no leftover chunk files",
      not [f for f in os.listdir(tmpd) if f.startswith("_1min_chunk")])

print(); print("=" * 70); print("5. permission (error 162) is reported, not swallowed"); print("=" * 70)
tmpd2 = tempfile.mkdtemp(prefix="pbp_")
path2 = os.path.join(tmpd2, "raw_1min_bars.parquet")
rows2 = []
for d in pd.bdate_range("2026-06-01", "2026-06-03"):
    ts_list = day_bars(d.date(), 240) if d.date() == pd.Timestamp("2026-06-02").date() \
        else day_bars(d.date())
    for a in ("EEE", "FFF"):
        rows2 += [{"ts_epoch_ms": ts, "asset": a, "open": 1., "high": 2.,
                   "low": .5, "close": 1.5, "volume": 10.} for ts in ts_list]
pd.DataFrame(rows2, columns=M.COLS).to_parquet(path2, index=False)
M.fetch_day = make_fetcher(set(), permission_symbols={"FFF"})
sys.argv = ["pull_bars.py", "--out", path2, "--tf", "1 min", "--mode", "repair",
            "--throttle", "0", "--merge-every", "100"]
rc = M.main()
check("run with a 162 failure still exits 0", rc == 0, f"rc={rc}")
df3 = pq.read_table(path2).to_pandas()
check("permitted symbol EEE was still repaired",
      df3[(df3.asset == "EEE")].shape[0] > 0)
shutil.rmtree(tmpd2)

shutil.rmtree(tmpd)
print(); print("=" * 70)
print(f"RESULT: {'ALL PASS' if not fails else str(len(fails)) + ' FAILURES: ' + str(fails)}")
print("=" * 70)
sys.exit(1 if fails else 0)

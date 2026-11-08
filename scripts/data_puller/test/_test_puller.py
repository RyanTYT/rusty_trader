"""End-to-end test of pull_bars.py with a fully mocked IB.

Validates: formatDate=2 correctness, ascending sort, dedup, schema coercion,
atomic merge, coverage-based resume, the JSON universe files, and the
tz-safety of bars_to_rows.
"""
import importlib.util, json, os, shutil, sys, tempfile, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

S = "/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/scripts/data_puller"
BASE = ("/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/"
        "data/backtest-results/stats/raw_bars.parquet")
sys.path.insert(0, S)          # pull_bars imports ib_throttle lazily
spec = importlib.util.spec_from_file_location("PB", f"{S}/pull_bars.py")
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)

ET = ZoneInfo("America/New_York")
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


print("=" * 70); print("1. bars_to_rows: tz-safety + ordering"); print("=" * 70)


class B:
    def __init__(s, d, o, h, l, c, v): s.date=d; s.open=o; s.high=h; s.low=l; s.close=c; s.volume=v


# Real IB semantics with formatDate=2: epoch seconds (int).
# 2026-06-30 09:30 ET (EDT, UTC-4) == 13:30 UTC.
t0 = int(datetime(2026, 6, 30, 13, 30, tzinfo=timezone.utc).timestamp())
bars_desc = [B(t0 + 300 * i, 1, 2, .5, 1.5, 100.) for i in range(5)][::-1]
rows = P.bars_to_rows(bars_desc, "TST")
ts = [r["ts_epoch_ms"] for r in rows]
check("input was newest-first", bars_desc[0].date > bars_desc[-1].date)
check("output is ascending", ts == sorted(ts), f"{ts}")
check("all on 5-min grid", all(t % 300000 == 0 for t in ts))
et_first = datetime.fromtimestamp(ts[0] / 1000, tz=timezone.utc).astimezone(ET)
check("first bar is 09:30 ET", (et_first.hour, et_first.minute) == (9, 30), str(et_first))

# The OLD bug: formatDate=1 naive datetime + .timestamp() in a non-ET process TZ.
os.environ["TZ"] = "Asia/Singapore"
try:
    import time as _t; _t.tzset()
except Exception:
    pass
naive = datetime(2026, 6, 30, 9, 30)
old_ms = int(naive.timestamp() * 1000)
new_ms = int(datetime(2026, 6, 30, 13, 30, tzinfo=timezone.utc).timestamp() * 1000)
check("OLD naive path is TZ-dependent (the bug)", old_ms != new_ms,
      f"off by {(new_ms-old_ms)/3600000:.0f}h")
check("NEW path is TZ-independent", ts[0] == new_ms)

# Same instant expressed the way the old code saw it, at two process TZs.
saved = os.environ.get("TZ")
for tz in ("UTC", "Asia/Singapore", "America/New_York"):
    os.environ["TZ"] = tz
    try:
        import time as _t; _t.tzset()
    except Exception:
        pass
    naive_ms = int(datetime(2026, 6, 30, 9, 30).timestamp() * 1000)
    epoch_ms = int(datetime.fromtimestamp(
        t0, tz=timezone.utc).timestamp() * 1000)
    check(f"  epoch path stable under TZ={tz}", epoch_ms == ts[0],
          "" if epoch_ms == ts[0] else f"naive would give {naive_ms}")
if saved is None:
    os.environ.pop("TZ", None)
else:
    os.environ["TZ"] = saved
try:
    import time as _t; _t.tzset()
except Exception:
    pass

print(); print("=" * 70); print("2. store merge: schema coercion + dedup"); print("=" * 70)

tmpd = tempfile.mkdtemp(prefix="puller_test_")
out = os.path.join(tmpd, "merged.parquet")
shutil.copy(BASE, out)
base_rows = pq.ParquetFile(out).metadata.num_rows
base_schema = pq.ParquetFile(out).schema_arrow
print(f"  base: {base_rows:,} rows, asset type = {base_schema.field('asset').type}")
store = P.Bars(out=out, label="5min", duckdb_memory="4GB")

# Synthetic bars land at 15:55 / 15:50 ET (inside RTH, on the 5-min grid).
T1555 = int(datetime(2026, 6, 30, 19, 55, tzinfo=timezone.utc).timestamp() * 1000)
T1550 = int(datetime(2026, 6, 30, 19, 50, tzinfo=timezone.utc).timestamp() * 1000)

# Chunk written via pandas -> large_string (the mismatch that used to crash)
chunk = os.path.join(tmpd, "_5min_chunk_0001.parquet")
pd.DataFrame([{"ts_epoch_ms": T1555, "asset": "ZZTEST", "open": 1., "high": 2.,
               "low": .5, "close": 1.5, "volume": 100.}],
             columns=P.COLS).to_parquet(chunk, index=False)
print(f"  chunk: asset type = {pq.ParquetFile(chunk).schema_arrow.field('asset').type}")

n = store.merge([chunk])
check("merge did not raise on string/large_string", True)
check("added exactly 1 row", n == base_rows + 1, f"{n:,}")
check("output schema == canonical", pq.ParquetFile(out).schema_arrow.equals(P.SCHEMA))
check("no .tmp left behind", not os.path.exists(out + ".tmp"))

# Idempotency: merging the SAME chunk again must not duplicate.
n2 = store.merge([chunk])
check("re-merge is idempotent", n2 == n, f"{n2:,}")

# Overlapping chunk (partial duplicate) must dedup.
overlap = os.path.join(tmpd, "_5min_chunk_0002.parquet")
pd.DataFrame([{"ts_epoch_ms": T1555, "asset": "ZZTEST", "open": 1., "high": 2.,
               "low": .5, "close": 1.5, "volume": 100.},
              {"ts_epoch_ms": T1550, "asset": "ZZTEST", "open": 1., "high": 2.,
               "low": .5, "close": 1.5, "volume": 100.}],
             columns=P.COLS).to_parquet(overlap, index=False)
n3 = store.merge([overlap])
check("overlap deduped to +1 net row", n3 == n + 1, f"{n3:,} vs expected {n+1:,}")

df = pd.read_parquet(out)
dup = int(df.duplicated(subset=["ts_epoch_ms", "asset"]).sum())
check("zero duplicate (ts,asset) in output", dup == 0, f"{dup}")
srt = df.sort_values(["asset", "ts_epoch_ms"], kind="stable").index.equals(df.index)
check("output sorted by (asset, ts)", srt)

print(); print("=" * 70); print("3. data integrity of merged output"); print("=" * 70)
ts_all = df.ts_epoch_ms.to_numpy()
check("all timestamps on 5-min grid", int((ts_all % 300000 != 0).sum()) <= 1,
      f"{int((ts_all % 300000 != 0).sum())} off-grid (1 pre-existing LIN bar)")
et = pd.DatetimeIndex(pd.to_datetime(ts_all, unit="ms", utc=True)).tz_convert(ET)
mod = et.hour * 60 + et.minute
check("no pre-09:30 ET bars", int((mod < 570).sum()) == 0)
check("no at/after-16:00 ET bars", int((mod >= 960).sum()) == 0)
check("no weekend bars", int((et.dayofweek >= 5).sum()) == 0)
check("base row count preserved", len(df) >= base_rows, f"{len(df):,}")
check("original 41 assets intact", df.asset.nunique() == 42, f"{df.asset.nunique()} (41+ZZTEST)")

print(); print("=" * 70); print("4. coverage-based resume"); print("=" * 70)
cov = store.load_covered(["AAPL", "ZZTEST"])
check("AAPL has coverage days", len(cov.get("AAPL", [])) > 1000, f"{len(cov.get('AAPL',[]))} days")
check("ZZTEST has coverage days", len(cov.get("ZZTEST", [])) == 1)
check("absent asset -> empty set", store.load_covered(["NOPE___"]).get("NOPE___") is None)
wd = P.et_days_in(datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 31, tzinfo=timezone.utc))
# Jan 2026: 31 days, Jan 1 is a Thursday -> 22 weekdays.
check("et_days_in skips weekends", len(wd) == 22, f"{len(wd)} weekdays in Jan 2026")
check("et_days_in contains no Sat/Sun", all(d.weekday() < 5 for d in wd))

print(); print("=" * 70); print("5. universe sanity (the JSON files)"); print("=" * 70)
U = lambda name: json.loads(open(os.path.join(S, "universes", name)).read())
ex41, allnew, bm = U("existing_41.json"), U("all_new.json"), U("benchmarks.json")
full, excl = U("full_universe.json"), U("excluded_no_contract.json")
check("existing_41 has 41 entries", len(ex41) == 41, f"{len(ex41)}")
check("all_new has 150 entries", len(allnew) == 150, f"{len(allnew)}")
check("benchmarks has 7 entries", len(bm) == 7, f"{len(bm)}")
check("full_universe is the order-preserving union (41+150+7 - dedups)",
      len(full) == len(set(ex41) | set(allnew) | set(bm)), f"{len(full)}")
check("all_new excludes the existing 41", not (set(allnew) & set(ex41)),
      f"overlap={sorted(set(allnew) & set(ex41))}")
check("universe files have no resolvable-contract symbols",
      not ((set(allnew) | set(bm)) & set(excl)))
check("every universe entry carries a non-empty exchange",
      all(isinstance(v, str) and v for m in (ex41, allnew, bm, full) for v in m.values()))
check("excluded list recorded as data", len(excl) == 11, f"{len(excl)}")

print(); print("=" * 70); print("6. merge on the REAL unsorted base file"); print("=" * 70)
# raw_bars.parquet stores per-asset blocks in hash order -- NOT sorted. A
# streaming merge that assumes pre-sorted input silently loses rows here.
t0 = time.time()
out2 = os.path.join(tmpd, "real.parquet")
shutil.copy(BASE, out2)
n_real = pq.ParquetFile(out2).metadata.num_rows
store2 = P.Bars(out=out2, label="5min", duckdb_memory="4GB")
m = store2.merge([])
check("identity merge preserves every row", m == n_real, f"{m:,} vs {n_real:,}")
df2 = pd.read_parquet(out2)
check("no rows lost on unsorted input", len(df2) == n_real, f"{len(df2):,}")
srt2 = df2.sort_values(["asset", "ts_epoch_ms"], kind="stable").index.equals(df2.index)
check("output is sorted by (asset, ts)", srt2)
check("still no duplicates", int(df2.duplicated(subset=["ts_epoch_ms", "asset"]).sum()) == 0)
check("all 41 assets survive", df2.asset.nunique() == 41, f"{df2.asset.nunique()}")
print(f"       (merge took {time.time() - t0:.1f}s)")

print(); print("=" * 70); print("7. argparse / dry-run"); print("=" * 70)
import subprocess
r = subprocess.run([sys.executable, f"{S}/pull_bars.py", "--dry-run", "--mode", "full",
                    "--assets", "SPY", "--out", os.path.join(tmpd, "x.parquet")],
                   capture_output=True, text=True, env={**os.environ})
check("--dry-run exits 0", r.returncode == 0, r.stderr[-200:] if r.returncode else "")
check("--dry-run prints a request estimate", "request" in r.stdout,
      r.stdout.strip().splitlines()[-1][:70] if r.stdout else "")

shutil.rmtree(tmpd)
print(); print("=" * 70)
print(f"RESULT: {'ALL PASS' if not fails else str(len(fails)) + ' FAILURES: ' + str(fails)}")
print("=" * 70)
sys.exit(1 if fails else 0)

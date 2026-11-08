"""Verify crash-resumability: chunk files written mid-pass must be picked up
and merged by the next run's stale-chunk sweep."""
import importlib.util, os, shutil, sys, tempfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

# pull_bars.py lives at the data_puller root (NOT experiments/ — that is the
# archive of the retired per-tf pullers). sys.path must carry the root too:
# pull_bars imports ib_throttle (its pacing twin) lazily inside the run paths,
# and direct script execution gets sys.path[0] for free — specloading does not.
S = ("/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/"
     "scripts/data_puller")
sys.path.insert(0, S)
spec = importlib.util.spec_from_file_location("PB", f"{S}/pull_bars.py")
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)

ET = ZoneInfo("America/New_York"); UTC = timezone.utc
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


d = tempfile.mkdtemp(prefix="resume_")
path = os.path.join(d, "raw_1min_bars.parquet")
COLS = M.COLS


def bars(day, n=390):
    base = datetime(day.year, day.month, day.day, 9, 30, tzinfo=ET)
    return [int((base + timedelta(minutes=i)).astimezone(UTC).timestamp() * 1000)
            for i in range(n)]


rows = []
for dd in pd.bdate_range("2026-06-01", "2026-06-05"):
    ts = bars(dd.date(), 240 if dd.date() == pd.Timestamp("2026-06-03").date() else 390)
    rows += [{"ts_epoch_ms": t, "asset": "AAA", "open": 1., "high": 2.,
              "low": .5, "close": 1.5, "volume": 10.} for t in ts]
pd.DataFrame(rows, columns=COLS).to_parquet(path, index=False)
n0 = pq.ParquetFile(path).metadata.num_rows
print(f"seed: {n0:,} rows, 1 short asset-day (AAA 2026-06-03 has 240)")

# Simulate an interrupted run: leave chunk files behind, as
# run_repair_concurrent does now that it persists incrementally via the store.
# The chunk label MUST match the run's --tf slug ("1 min" -> "1min") or the
# sweep would never find the work it already did.
store = M.Bars(out=path, label="1min")
orphan_rows = M.bars_to_rows(
    [type("B", (), {"date": int(t / 1000), "open": 1., "high": 2., "low": .5,
                    "close": 1.5, "volume": 10.})() for t in
     bars(pd.Timestamp("2026-06-03").date(), 390)], "AAA")
cf = store.write_chunk(orphan_rows, 1)
check("orphan chunk exists (simulating a killed run)", os.path.exists(cf))

# Now run --dry-run with the stale sweep; it should merge the orphan first.
sys.argv = ["pull_bars.py", "--out", path, "--tf", "1 min",
            "--mode", "repair", "--dry-run"]
rc = M.main()
check("dry-run exits 0", rc == 0, f"rc={rc}")

# The sweep happens before the dry-run gate, so verify the merge directly.
n1 = pq.ParquetFile(path).metadata.num_rows
check("stale chunk was merged on next start", n1 == n0 + 150,
      f"{n1:,} vs expected {n0+150:,}")
check("orphan chunk file removed after merge", not os.path.exists(cf))
df = pd.read_parquet(path)
et = pd.DatetimeIndex(pd.to_datetime(df.ts_epoch_ms.to_numpy(), unit="ms", utc=True)).tz_convert(ET)
cnt = pd.DataFrame({"asset": df.asset.to_numpy(), "day": et.date}).groupby(["asset", "day"]).size()
check("AAA 2026-06-03 now complete (390)",
      cnt.get(("AAA", pd.Timestamp("2026-06-03").date())) == 390,
      str(cnt.get(("AAA", pd.Timestamp("2026-06-03").date()))))
check("no duplicate (ts,asset)",
      int(df.duplicated(subset=["ts_epoch_ms", "asset"]).sum()) == 0)
check("sorted by (asset, ts)",
      df.sort_values(["asset", "ts_epoch_ms"], kind="stable").index.equals(df.index))

# After the merge, the day is no longer "short" -> plan is empty -> idempotent
print()
sys.argv = ["pull_bars.py", "--out", path, "--tf", "1 min",
            "--mode", "repair", "--dry-run"]
M.main()

shutil.rmtree(d)
print()
print("=" * 66)
print(f"RESULT: {'ALL PASS' if not fails else str(len(fails)) + ' FAILURES: ' + str(fails)}")
print("=" * 66)
sys.exit(1 if fails else 0)

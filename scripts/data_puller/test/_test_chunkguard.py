"""Guard: run_repair_concurrent must refuse to start when unmerged chunk files
are still present, rather than overwriting them (its counter restarts at 1)."""
import importlib.util, os, shutil, tempfile
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

# pull_bars.py lives at the data_puller root (NOT experiments/ — that is the
# archive of the retired per-tf pullers).
S = "/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/scripts/data_puller"
spec = importlib.util.spec_from_file_location("PB", f"{S}/pull_bars.py")
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)

ET = ZoneInfo("America/New_York"); UTC = timezone.utc
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


class Args:
    concurrency = 1; persist_every = 10; throttle = 0.0; retries = 1
    retry_passes = 0; request_delay = 0.0
    bar_seconds = 60; bar_minutes = 1; bar_size = "1 min"


class Mon:
    total = 0

    def begin(self, name): pass
    def end(self, name, n, err): pass


def store(dirname):
    s = M.Bars(out=os.path.join(dirname, "base.parquet"), label="1min",
               duckdb_memory="2GB")
    return s


def minute_ts(day, tod):
    return int((datetime(day.year, day.month, day.day, tzinfo=ET)
                + timedelta(minutes=tod)).astimezone(UTC).timestamp())


class FakeIB:
    def __init__(self): self.requests = []

    async def reqHistoricalDataAsync(self, contract, end_str, duration,
                                     bar_size, what, rth, f, keep, o, t):
        self.requests.append(1)
        return [type("B", (), {"date": minute_ts(date(2026, 6, 3), tod),
                               "open": 1., "high": 2., "low": .5,
                               "close": 1.5, "volume": 10.})()
                for tod in range(570, 960)]


DAY = date(2026, 6, 3)
print("=" * 70); print("orphan chunks are never overwritten"); print("=" * 70)

# 1. with an un-swept orphan present -> must refuse
d1 = tempfile.mkdtemp()
orphan = os.path.join(d1, "_1min_chunk_0001.parquet")
with open(orphan, "wb") as f:
    f.write(b"pre-existing orphan")
s1 = store(d1)
s1.write_chunk = MagicMock()
ib = FakeIB()
raised_msg = ""
try:
    M.run_repair_concurrent(ib, [("AAA", DAY)], {}, Args(),
                            {("AAA", DAY): (570, 809)}, set(), s1, Mon())
except RuntimeError as e:
    raised_msg = str(e)
withRaised = bool(raised_msg)
check("refuses to start when orphans exist", withRaised, raised_msg[:100])
check("error names the leftover chunks", "refusing to start" in raised_msg,
      raised_msg[:80])
check("no request was issued", len(ib.requests) == 0, str(len(ib.requests)))
check("no chunk was written", not s1.write_chunk.called)
check("orphan file still on disk (not clobbered)", os.path.exists(orphan))
with open(orphan, "rb") as f:
    check("orphan contents intact", f.read() == b"pre-existing orphan")
shutil.rmtree(d1)

# 2. clean directory -> must proceed normally
d2 = tempfile.mkdtemp()
s2 = store(d2)
written = []


def fake_write(rows, n):
    written.append((os.path.basename(rows[0]["asset"] if rows else ""), n))
    return os.path.join(d2, f"_1min_chunk_{n:04d}.parquet")


s2.write_chunk = fake_write
ib2 = FakeIB()
m, r, cfs = M.run_repair_concurrent(ib2, [("AAA", DAY)], {}, Args(),
                                    {("AAA", DAY): (570, 809)}, set(),
                                    s2, Mon())
check("proceeds on a clean directory", len(ib2.requests) == 1,
      str(len(ib2.requests)))
check("returns its chunk list for main() to merge", len(cfs) == 1,
      str([os.path.basename(c) for c in cfs]))
check("persisted the missing tail (tod 810..959 = 150 rows)",
      len(written) == 1 and written[0][1] == 1, str(written))
shutil.rmtree(d2)

print()
print("=" * 70)
print(f"RESULT: {'ALL PASS' if not fails else str(len(fails)) + ' FAILURES: ' + str(fails)}")
print("=" * 70)
raise SystemExit(1 if fails else 0)

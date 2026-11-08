"""Exercise run_repair_concurrent with a mocked gateway.

This is the path that failed twice with NameError (`bounds` unthreaded, then
`buf_n` unbound inside the nested coroutine). A static linter was too noisy to
be worth shipping; this drives the real code and asserts on real behaviour:
the narrowed window is requested, already-stored bars are filtered out, and
rows actually reach the chunk files on disk.
"""
import importlib.util, os, shutil, sys, tempfile
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

S = "/Users/ryantan/Downloads/Personal Projects/auto ibkr/py_book/APP/scripts/data_puller"
sys.path.insert(0, S)          # pull_bars imports ib_throttle lazily
spec = importlib.util.spec_from_file_location("PB", f"{S}/pull_bars.py")
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)

ET = ZoneInfo("America/New_York"); UTC = timezone.utc
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


DAY = date(2026, 6, 3)
LAST_HELD = 13 * 60 + 29          # file stops at 13:29 -> 240 bars on disk


def minute_ts(day, tod):
    return int((datetime(day.year, day.month, day.day, tzinfo=ET)
                + timedelta(minutes=tod)).astimezone(UTC).timestamp() * 1000)


class FakeIB:
    """Answers reqHistoricalDataAsync for the whole session."""
    def __init__(self):
        self.requests = []

    async def reqHistoricalDataAsync(self, contract, end_str, duration,
                                     bar_size, what, rth, fmt, keep, opts,
                                     timeout):
        self.requests.append({"end": end_str, "dur": duration,
                              "bar_size": bar_size})
        rows = []
        for tod in range(570, 960):          # full session
            rows.append(type("B", (), {
                "date": minute_ts(DAY, tod) // 1000, "open": 1., "high": 2.,
                "low": .5, "close": 1.5, "volume": 10.})())
        return rows


class Args:
    concurrency = 1
    persist_every = 10_000
    throttle = 0.0
    retries = 1
    retry_passes = 0
    duckdb_memory = "2GB"
    request_delay = 0.0        # pace() off in tests; no real gateway here
    bar_seconds = 60
    bar_minutes = 1
    bar_size = "1 min"


class Mon:
    """Counter stub for the ThrottleMonitor seam."""
    total = 0

    def begin(self, name):
        self.total += 1

    def end(self, name, n, err):
        pass


tmpd = tempfile.mkdtemp(prefix="conc_")
# bounds is a plain dict {(asset, day): (first_tod, last_tod)} -- matching
# scan()'s return type. Passing a DataFrame here used to hide a real bug:
# DataFrame.get() is a COLUMN lookup, so lookups silently returned None and
# every request degraded to a full-day fetch.
bounds = {("AAA", DAY): (570, LAST_HELD)}
plan = [("AAA", DAY)]
ib = FakeIB()
store = M.Bars(out=os.path.join(tmpd, "base.parquet"), label="1min",
               duckdb_memory="2GB")
chunk_files = []
# instance-level write spy: the old tests patched module-level
# write_chunk_file; the chunk writer now lives on the Bars store instance.
store.write_chunk = lambda rows, n: (
    chunk_files.append(f"{tmpd}/_1min_chunk_{n:04d}.parquet") or chunk_files[-1])

monitor, results, _cf0 = M.run_repair_concurrent(
    ib, plan, {"AAA": "SMART"}, Args(), bounds, None, store, Mon())

print("=" * 70); print("1. request shaping"); print("=" * 70)
check("exactly one request issued", len(ib.requests) == 1, str(len(ib.requests)))
r = ib.requests[0]
check("bar size is 1 min", r["bar_size"] == "1 min", r["bar_size"])
check("duration narrowed below a full day", r["dur"].endswith(" S"),
      f"{r['dur']} (vs a full '1 D')")
dur_s = int(r["dur"].split()[0])
check("duration ~ only the missing tail", 5000 < dur_s < 12000, f"{dur_s}s")

print(); print("=" * 70); print("2. filtering: nothing already on disk"); print("=" * 70)
rows, err = results[("AAA", DAY)]
check("request returned no error", err is None, str(err))
tod = [datetime.fromtimestamp(x["ts_epoch_ms"] / 1000, tz=UTC)
       .astimezone(ET).hour * 60 + datetime.fromtimestamp(
           x["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET).minute for x in rows]
check("only the missing tail kept (150 bars)", len(rows) == 150, f"{len(rows)}")
check("every kept bar is after 13:29", all(t > LAST_HELD for t in tod),
      f"min={min(tod)} max={max(tod)}")
check("kept tail ends at 15:59", max(tod) == 959, str(max(tod)))

print(); print("=" * 70); print("3. persistence (the earlier UnboundLocalError)"); print("=" * 70)
check("rows were written to a chunk file", len(chunk_files) >= 1,
      str([os.path.basename(c) for c in chunk_files]))
check("chunk file uses the label prefix",
      all("_1min_chunk_" in c for c in chunk_files),
      str([os.path.basename(c) for c in chunk_files]))
check("monitor counted the request", monitor.total == 1, str(monitor.total))

print(); print("=" * 70); print("4. window arithmetic"); print("=" * 70)
cases = [("ends 13:29", 570, 809, "tail", 9120),
         ("ends 14:29", 570, 869, "tail", 5520),
         ("starts 13:30", 810, 959, "head", 14460),
         ("complete", 570, 959, None, None)]
for label, f, l, want_mode, want_dur in cases:
    got = M.missing_window(DAY, f, l, bar_seconds=60, bar_minutes=1)
    if want_mode is None:
        check(f"{label} -> skipped", got is None, str(got))
    else:
        _, dur, mode = got
        check(f"{label} -> {want_mode}", mode == want_mode, mode)
        check(f"{label} -> {want_dur}S", int(dur.split()[0]) == want_dur, dur)

print(); print("=" * 70); print("5. half-days are skipped, not requested"); print("=" * 70)
HD = date(2026, 6, 5)
# half_day_dates needs >=5 assets on the date before it will judge (a single
# truncated symbol is not evidence of a market-wide early close).
hd_bounds = {(f"S{i}", HD): (570, 779) for i in range(8)}   # 09:30-12:59 close
check("half_day_dates detects the date", HD in M.half_day_dates(hd_bounds, 1),
      str(M.half_day_dates(hd_bounds, 1)))
check("missing_window -> None for a half-day",
      M.missing_window(HD, 570, 779, bar_seconds=60, bar_minutes=1,
                       skip_half_days={HD}) is None)
check("missing_window still returns a tail for a TRUNCATED session "
      "on the same stamp",
      M.missing_window(HD, 570, 809, bar_seconds=60, bar_minutes=1,
                       skip_half_days={HD}) is not None,
      "13:29 cut must not be mistaken for a half-day")

ib2 = FakeIB()
store2 = M.Bars(out=os.path.join(tmpd, "base.parquet"), label="1min",
                duckdb_memory="2GB")
chunk_files2 = []
store2.write_chunk = lambda rows, n: (
    chunk_files2.append(f"{tmpd}/_1min_chunk_{n:04d}.parquet") or chunk_files2[-1])
# iterate the dict's KEYS, which are already (symbol, day) tuples
m2, r2, _cf3 = M.run_repair_concurrent(
    ib2, list(hd_bounds), {}, Args(), hd_bounds, {HD}, store2, Mon())
check("half-day issued ZERO requests", len(ib2.requests) == 0,
      str(len(ib2.requests)))
check("half-day persisted nothing", len(chunk_files2) == 0,
      str(len(chunk_files2)))

shutil.rmtree(tmpd)
print(); print("=" * 70)
print(f"RESULT: {'ALL PASS' if not fails else str(len(fails)) + ' FAILURES: ' + str(fails)}")
print("=" * 70)

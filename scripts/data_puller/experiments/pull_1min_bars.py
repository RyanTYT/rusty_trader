#!/usr/bin/env python3
"""Repair / extend the 1-min RTH bar file (``raw_1min_bars.parquet``).

Why this exists
---------------
An audit of the existing ``raw_1min_bars.parquet`` found **4,154 truncated
asset-days across 131 dates** -- sessions that stop early (13:29 or 14:29 ET)
or start late, while the matching 5-min file is complete for the same date.
On 87 of those dates *every* asset cut at the same clock time, which is the
signature of a request-size overrun rather than a per-symbol failure.

Root cause: the original puller asked IBKR for **60 days of 1-min bars in a
single request** (~23,400 bars). IBKR publishes a *step size* table limiting
how much data one request may span, and asks for "only a few thousand bars at
a time". For 1-min bars the table's longest permitted duration is **1 day**.
Overrunning it does not raise -- IBKR silently returns a partial session.

There is NO two-year retention wall for 1-min bars. IBKR's docs state that
"Historical Data Limitations for barSize = '1 mins' and greater have been
lifted"; the only age cutoff applies to bars of 30 seconds or less (six
months). So the truncated days are recoverable, and this script refetches
them one session at a time.

The one real failure mode to distinguish is **error 162** ("No market data
permissions") -- a subscription problem, reported separately so it is never
confused with a short session.

Reuses the correctness-critical primitives from ``pull_5min_bars.py``:
``formatDate=2`` UTC timestamps, explicit ascending sort, and the DuckDB
external-sort merge (the base files are not pre-sorted).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

_SYS = str(Path(__file__).resolve().parent)
if _SYS not in sys.path:
    sys.path.insert(0, _SYS)
# Archived: throttle + universe chain live one level up (same as pull_5min).
_SYS2 = str(Path(__file__).resolve().parent.parent)
if _SYS2 not in sys.path:
    sys.path.insert(0, _SYS2)
from pull_5min_bars import (  # noqa: E402
    bars_to_rows, connect, et_days_in, make_contract, merge_chunks, report,
    request_chunk, write_chunk_file,
)
from ib_async import IB  # noqa: E402

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

BAR_SIZE = "1 min"
BAR_MINUTES = 1
BARS_PER_DAY = 390          # 09:30-16:00 ET
OPEN_MIN = 570              # 09:30 ET, minutes from midnight

# IBKR's published step-size table (Historical Data Limitations) maps a request
# duration to the bar sizes allowed with it. Longest duration permitting 1-min
# bars is 1 day. Asking for more does not error -- it silently truncates, which
# is exactly how the original file was damaged.
MAX_DAYS_PER_REQUEST = 1

# Substrings identifying a permissions/subscription failure rather than a
# genuinely absent session. Error 162 is "Historical Market Data Service error
# message: No market data permissions for <SYM>".
PERMISSION_ERR_MARKERS = ("162", "no market data permissions",
                          "not subscribed", "requires additional subscription",
                          "market data permissions")

DEFAULT_OUT = str(
    Path(__file__).resolve().parent.parent / "data" / "backtest-results"
    / "stats" / "raw_1min_bars.parquet")


# ── analysis ─────────────────────────────────────────────────────────────────
def scan(path: str, assets: list[str] | None = None):
    """Per-(asset, day) bar counts for the existing file.

    The counts are also the baseline for judging a repair: a session is
    "repaired" when the refetch returns MORE bars than the file already had.
    Scoring against a flat 390 would permanently fail genuine half-days
    (210 bars), which can never reach 390.
    """
    df = pq.read_table(path, columns=["ts_epoch_ms", "asset"]).to_pandas()
    et = pd.DatetimeIndex(
        pd.to_datetime(df["ts_epoch_ms"].to_numpy(), unit="ms", utc=True)
    ).tz_convert(ET)
    t = pd.DataFrame({"asset": df.asset.to_numpy(), "day": et.date,
                      "tod": et.hour * 60 + et.minute})
    counts = t.groupby(["asset", "day"]).size()
    # First/last minute-of-day per asset-day, so the fetch can be narrowed to
    # just the missing head or tail instead of the whole session. This is a
    # plain dict on purpose: `DataFrame.get(key)` is a COLUMN lookup, so using
    # it for row keys silently returned None and every request degraded to a
    # full-day fetch.
    agg = t.groupby(["asset", "day"])["tod"].agg(["min", "max"])
    bounds = {(a, d): (int(r["min"]), int(r["max"])) for (a, d), r in agg.iterrows()}
    return df, counts, t, bounds


def find_short_days(counts: pd.Series, last_day: _date) -> dict[str, list[_date]]:
    """Per-asset list of short session dates (0 < bars < a full session).

    Days with zero bars are absent entirely and are handled separately (they
    may be market holidays).
    """
    short: dict[str, list[_date]] = {}
    sub = counts[(counts > 0) & (counts < BARS_PER_DAY)]
    for (asset, day), _n in sub.items():
        if day <= last_day:
            short.setdefault(asset, []).append(day)
    return short


def late_open_days(t: pd.DataFrame) -> dict[str, list[_date]]:
    """asset-days whose session starts after 09:30 ET (missing the open)."""
    first = t.groupby(["asset", "day"])["tod"].min()
    late = first[first > OPEN_MIN]
    out: dict[str, list[_date]] = {}
    for (asset, day), _v in late.items():
        out.setdefault(asset, []).append(day)
    return out


# ── fetch ────────────────────────────────────────────────────────────────────
OPEN_MIN = 570                 # 09:30 ET
CLOSE_MIN = 960                # 16:00 ET
LAST_BAR_MIN = CLOSE_MIN - 1   # 15:59 -- final stamped 1-min bar


def half_day_dates(bounds: dict) -> set:
    """Dates where the session ended at the canonical 13:00 close.

    A genuine half-day (day after Thanksgiving, July 3, Christmas Eve) ends at
    12:59 and has NOTHING missing -- requesting 13:00-16:00 correctly returns 0
    bars, which looks identical to throttling if you only count empties. Those
    dates are identified by the majority of assets ending at 12:59 together,
    which distinguishes a market-wide early close from a truncation artifact.
    """
    per_day = {}
    for (asset, day), (_lo, hi) in bounds.items():
        d = per_day.setdefault(day, [0, 0])
        d[1] += 1
        if hi == 779:            # 12:59 == 13:00 close
            d[0] += 1
    return {day for day, (n779, tot) in per_day.items()
            if tot >= 5 and n779 / tot >= 0.8}


def missing_window(day: _date, first_tod: int, last_tod: int,
                   skip_half_days: set | None = None):
    """Smallest (endDateTime, durationStr) that yields ONLY the bars we lack.

    IBKR returns bars in [endDateTime - duration, endDateTime]. The file
    already holds the intact part of a truncated session, so re-requesting the
    whole day wastes most of the payload -- a session cut at 13:29 keeps 240
    of 390 bars and needs only the 150 after it.

    Returns (end_dt, duration_str, mode) where mode is "tail" (session ends
    early -> fetch the afternoon), "head" (starts late -> fetch the morning),
    or None when the session is already complete or is a genuine half-day.
    """
    if (skip_half_days and day in skip_half_days
            and first_tod == OPEN_MIN and last_tod == 779):
        return None                     # 09:30-12:59 early close: nothing missing
    if last_tod < LAST_BAR_MIN:                      # ends early -> need tail
        # window [last_tod, close]; +1min slack so the boundary bar is
        # included in the response and can then be filtered out precisely.
        dur = (CLOSE_MIN - last_tod) * 60 + 60
        end = datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=ET)
        return end, f"{dur} S", "tail"
    if first_tod > OPEN_MIN:                        # starts late -> need head
        dur = (first_tod - OPEN_MIN) * 60 + 60
        end = datetime(day.year, day.month, day.day,
                       first_tod // 60, first_tod % 60, tzinfo=ET)
        return end, f"{dur} S", "head"
    return None


def fetch_day(ib: IB, symbol: str, day: _date, exchanges: dict,
              throttle: float, retries: int,
              errors: dict | None = None,
              first_tod: int | None = None,
              last_tod: int | None = None) -> list[dict]:
    """Fetch ONLY the missing portion of one asset-day's RTH session.

    With `first_tod`/`last_tod` (minute-of-day of the bars already on disk)
    the request is narrowed to the missing head or tail. Without them it falls
    back to the full session. In both cases the result is filtered to the date
    and to the half that is actually absent, so nothing already stored is
    re-downloaded or re-merged.
    """
    span = None
    if first_tod is not None and last_tod is not None:
        span = missing_window(day, first_tod, last_tod)
    if span is None:
        end = datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=ET)
        duration, mode = f"{MAX_DAYS_PER_REQUEST} D", "all"
    else:
        end, duration, mode = span

    rows = request_chunk(ib, [symbol], end, duration, throttle, retries,
                         bar_size=BAR_SIZE, exchanges=exchanges,
                         errors=errors).get(symbol, [])

    keep = []
    for r in rows:
        d = datetime.fromtimestamp(r["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET)
        if d.date() != day:
            continue
        tod = d.hour * 60 + d.minute
        if mode == "tail" and last_tod is not None and tod <= last_tod:
            continue                      # already on disk
        if mode == "head" and first_tod is not None and tod >= first_tod:
            continue                      # already on disk
        keep.append(r)
    return keep


def classify_error(msg: str) -> str:
    low = (msg or "").lower()
    return "permission" if any(m in low for m in PERMISSION_ERR_MARKERS) else "other"


# ── main ─────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.environ.get("IB_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("IB_PORT", "4002")))
    p.add_argument("--client-id", type=int,
                   default=int(os.environ.get("IB_CLIENT_ID", "16")))
    p.add_argument("--out", default=os.environ.get("OUTPUT", DEFAULT_OUT))
    p.add_argument("--throttle", type=float,
                   default=float(os.environ.get("THROTTLE", "0.5")))
    p.add_argument("--retries", type=int, default=int(os.environ.get("RETRIES", "3")))
    p.add_argument("--merge-every", type=int,
                   default=int(os.environ.get("MERGE_EVERY", "400")))
    p.add_argument("--duckdb-memory", default=os.environ.get("DUCKDB_MEMORY", "6GB"),
                   help="memory_limit for the merge sort")
    p.add_argument("--timeout", type=int, default=int(os.environ.get("IB_TIMEOUT", "60")))
    p.add_argument("--mode", choices=["repair", "extend", "full"], default="repair",
                   help="repair=refetch short days only; extend=fill the tail; "
                        "full=repair+extend")
    p.add_argument("--extend-days", type=int,
                   default=int(os.environ.get("EXTEND_DAYS", "0")),
                   help="in extend/full mode, also pull this many days past "
                        "the file's last date (0 = up to today)")
    p.add_argument("--assets", default=os.environ.get("ASSETS", ""),
                   help="comma-separated whitelist (default: all in the file)")
    p.add_argument("--max-days", type=int,
                   default=int(os.environ.get("MAX_DAYS", "0")),
                   help="cap the number of asset-days repaired (smoke tests)")
    p.add_argument("--concurrency", type=int,
                   default=int(os.environ.get("CONCURRENCY", "1")),
                   help=">1 fires all repair requests concurrently with no "
                        "inter-request sleep, and measures IBKR throttling. "
                        "Empties are always retried serially afterwards, "
                        "because IBKR drops over-concurrent requests silently.")
    p.add_argument("--request-delay", type=float,
                   default=float(os.environ.get("REQUEST_DELAY", "0.35")),
                   help="floor on the gap between request STARTS. This, not "
                        "concurrency, is what keeps the gateway socket alive: "
                        "at ~0 it sustained ~4.8 req/s and IBKR disconnected "
                        "mid-run. 0.35s caps the rate near 2.8 req/s.")
    p.add_argument("--retry-passes", type=int,
                   default=int(os.environ.get("RETRY_PASSES", "4")),
                   help="serial retry passes for requests that came back empty")
    p.add_argument("--persist-every", type=int,
                   default=int(os.environ.get("PERSIST_EVERY", "50000")),
                   help="flush fetched rows to a chunk file every N rows so a "
                        "kill loses at most this much work")
    p.add_argument("--dry-run", action="store_true",
                   help="analyse and report the plan without connecting")
    return p.parse_args()


def run_concurrent(ib, plan: list[tuple[str, _date]], exchanges: dict,
                   args, baseline: dict, out_dir: str, chunk_num: int,
                   since_merge: int, t0: float, bounds=None,
                   skip_half_days=None):
    """Fire every asset-day request concurrently and measure IBKR's real limits.

    This is the empirical counterpart to the published pacing rules: rather than
    trusting a sleep interval, run at full speed and record peak concurrency,
    rolling-window request counts and any throttle responses so we can see
    where IBKR actually pushes back.
    """
    import asyncio
    import nest_asyncio
    from ib_throttle import MAX_SIMULTANEOUS, ThrottleMonitor

    nest_asyncio.apply()

    # IBKR documents a max of 50 simultaneous open historical-data requests.
    # The 200-request run at concurrency 200 hit peak_in_flight=200 and came
    # back with 125/200 EMPTY and no error -- silent throttling, not holidays.
    # Cap in-flight requests at the documented limit.
    conc = max(1, min(args.concurrency, MAX_SIMULTANEOUS))
    if conc != args.concurrency:
        print(f"  capping concurrency {args.concurrency} -> {conc} "
              f"(IBKR documented max {MAX_SIMULTANEOUS} simultaneous)")
    sem = asyncio.Semaphore(conc)

    # Sustained RATE is what actually breaks the gateway, not concurrency.
    # At concurrency 12 the first 16 min were clean, then IBKR dropped the
    # socket on 80% of requests (2,645 ConnectionErrors) once the cumulative
    # rate hit ~4.8 req/s. A floor on the gap between request STARTS bounds
    # that rate regardless of how many are in flight.
    gap = max(0.0, args.request_delay)
    gate_lock = asyncio.Lock()
    last_start = [0.0]

    async def pace():
        if gap <= 0:
            return
        async with gate_lock:
            loop = asyncio.get_event_loop()
            wait = last_start[0] + gap - loop.time()
            if wait > 0:
                await asyncio.sleep(wait)
            last_start[0] = loop.time()

    monitor = ThrottleMonitor(
        log_path=Path(out_dir) / "_throttle_events.jsonl")

    contracts: dict[str, object] = {}
    results: dict[tuple[str, _date], tuple[list[dict], str | None]] = {}

    # ── Crash-resumable persistence ──────────────────────────────────────
    # Rows are flushed to a chunk file every `persist_every` completions, so a
    # kill (or a crash) never loses more than that many requests. Previously
    # chunks were only written after the whole pass finished, which meant a
    # kill threw away every completed request -- and the parquet merge happens
    # at the very end regardless, so the stale-chunk sweep on the next start
    # is what makes a resume actually recover the work.
    buf: list[dict] = []
    buf_n = 0
    seq = {"n": 0}

    def flush_buffer(force: bool = False):
        nonlocal buf, buf_n
        if not buf or (not force and buf_n < args.persist_every):
            return
        seq["n"] += 1
        cf = write_chunk_file(buf, seq["n"], out_dir, prefix="1min")
        chunk_files.append(cf)
        persisted.append((cf, buf_n))
        buf, buf_n = [], 0
        print(f"  [persisted {buf_n or persisted[-1][1]:,} rows -> "
              f"{os.path.basename(cf)}] "
              f"({len(persisted)} chunk files, "
              f"{monitor.total:,} requests done)", flush=True)

    chunk_files: list[str] = []
    persisted: list[tuple[str, int]] = []

    # Safety net against silently clobbering unmerged work. main() sweeps and
    # DELETES orphans before we get here, and the counter below restarts at 1,
    # so any `_1min_chunk_*` still present means the sweep did not match (a
    # prefix mismatch previously made resume impossible for exactly this
    # reason). Refuse rather than overwrite.
    leftovers = sorted(Path(out_dir).glob("_1min_chunk_*.parquet"))
    if leftovers:
        raise RuntimeError(
            f"refusing to start: {len(leftovers)} unmerged chunk file(s) in "
            f"{out_dir} (e.g. {leftovers[0].name}). The stale sweep should "
            f"have merged and removed them; not overwriting them.")

    def fmt(key, end_epoch):
        asset, day = key
        if asset not in contracts:
            contracts[asset] = make_contract(asset, exchanges)
        end = datetime.fromtimestamp(end_epoch, tz=UTC).astimezone(ET)
        return (contracts[asset],
                end.strftime("%Y%m%d %H:%M:%S US/Eastern"))

    async def go():
        tasks = []
        for asset, day in plan:
            # Narrow the request to the missing head/tail using what is already
            # on disk, instead of re-requesting the whole session.
            b = bounds.get((asset, day))
            if b is None:
                span = (datetime(day.year, day.month, day.day, 16, 0, tzinfo=ET),
                        f"{MAX_DAYS_PER_REQUEST} D", "all")
            else:
                # Returns None for a complete session OR a half-day; both mean
                # there is nothing to request, so bail before unpacking.
                span = missing_window(day, b[0], b[1], skip_half_days)
            if span is None:
                continue
            end_dt, dur, mode = span
            end_epoch = int(end_dt.astimezone(UTC).timestamp())
            tasks.append(((asset, day), end_epoch, dur, mode))

        async def one(key, end_epoch, dur, mode):
            # `nonlocal` is REQUIRED here: `buf_n += ...` inside a nested
            # function makes the name local to that function, so without this
            # every request raises UnboundLocalError and NO rows are ever
            # persisted -- silently, since the fetch itself succeeds.
            nonlocal buf, buf_n
            asset, day = key
            contract, end_str = fmt(key, end_epoch)
            lo = hi = None
            b = bounds.get((asset, day))
            if b is not None:
                lo, hi = b
            await pace()               # bound the SUSTAINED request rate
            async with sem:            # bound in-flight count
                monitor.begin(f"{asset}@{day}")
                try:
                    bars = await ib.reqHistoricalDataAsync(
                        contract, end_str, dur,
                        BAR_SIZE, "TRADES", True, 2, False, [], 120)
                    rows = []
                    for r in bars_to_rows(list(bars), asset):
                        d = datetime.fromtimestamp(
                            r["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET)
                        if d.date() != day:
                            continue
                        tod = d.hour * 60 + d.minute
                        if mode == "tail" and hi is not None and tod <= hi:
                            continue                # already on disk
                        if mode == "head" and lo is not None and tod >= lo:
                            continue                # already on disk
                        rows.append(r)
                    monitor.end(f"{asset}@{day}", len(rows), None)
                    results[key] = (rows, None)
                    if rows:
                        buf.extend(rows)
                        buf_n += len(rows)
                        flush_buffer()
                except Exception as e:  # noqa: BLE001
                    msg = f"{type(e).__name__}: {e}"
                    monitor.end(f"{asset}@{day}", 0, msg)
                    results[key] = ([], msg)

        await asyncio.gather(*(one(k, e, d, m) for k, e, d, m in tasks))

    asyncio.get_event_loop().run_until_complete(go())

    # ── Serial retry pass ────────────────────────────────────────────────
    # IBKR silently drops requests under concurrency: it returns 0 bars with
    # NO error. Measured on this account: conc 200 -> 62.5% empty, conc 50 ->
    # 28% empty, but a serial request returns 390 bars for every one of those
    # same dates. So empties are retried serially until they resolve, which
    # makes the result independent of the throttle threshold. Genuine
    # half-days come back at 210 and are simply recorded, not retried forever.
    # ── Serial retry pass ────────────────────────────────────────────────
    # MUST stay on the same event loop and use the ASYNC API. The synchronous
    # ib_async wrapper (reqHistoricalData) calls run_until_complete() itself,
    # and re-entering the loop after go() already ran DEADLOCKS -- it sat idle
    # in epoll for 40+ minutes with zero output. Hence: async, one at a time.
    async def retry_all():
        for attempt in range(1, args.retry_passes + 1):
            # Retry BOTH silent empties AND hard errors. Previously only
            # empties were retried, so a gateway socket disconnect mid-run
            # lost those asset-days outright (2,645 in one run).
            missing = [k for k, (rows, err) in results.items() if not rows]
            if not missing:
                print(f"  retry pass {attempt}: nothing outstanding", flush=True)
                break
            print(f"  retry pass {attempt}: {len(missing)} outstanding "
                  f"request(s) -- refetching one at a time", flush=True)
            recovered = 0
            for key in missing:
                asset, day = key
                b = bounds.get(key)
                if b is None:
                    continue
                span = missing_window(day, b[0], b[1], skip_half_days)
                if span is None:
                    results[key] = ([], "nothing missing")
                    continue
                end_dt, dur, mode = span
                end_str = end_dt.strftime("%Y%m%d %H:%M:%S US/Eastern")
                c = contracts.get(asset) or make_contract(asset, exchanges)
                contracts[asset] = c
                monitor.begin(f"retry:{asset}@{day}")
                try:
                    bars = await asyncio.wait_for(
                        ib.reqHistoricalDataAsync(
                            c, end_str, dur, BAR_SIZE, "TRADES", True,
                            2, False, [], 60), timeout=75)
                    rows = []
                    for r in bars_to_rows(list(bars), asset):
                        d = datetime.fromtimestamp(
                            r["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET)
                        if d.date() != day:
                            continue
                        tod = d.hour * 60 + d.minute
                        if mode == "tail" and tod <= b[1]:
                            continue
                        if mode == "head" and tod >= b[0]:
                            continue
                        rows.append(r)
                    monitor.end(f"retry:{asset}@{day}", len(rows), None)
                    if rows:
                        results[key] = (rows, None)
                        recovered += 1
                        buf.extend(rows)
                        buf_n += len(rows)
                        flush_buffer()
                except Exception as e:  # noqa: BLE001
                    monitor.end(f"retry:{asset}@{day}", 0, f"{type(e).__name__}: {e}")
                    results[key] = ([], f"{type(e).__name__}: {e}")
                await asyncio.sleep(0.4)
            print(f"    recovered {recovered}/{len(missing)}", flush=True)
            if recovered == 0:
                print("    no further progress; remaining are genuine empties",
                      flush=True)
                break

    asyncio.get_event_loop().run_until_complete(retry_all())

    flush_buffer(force=True)
    # Return the chunk list too: run_concurrent owns the `chunk_files` local
    # that flush_buffer appends to, so main cannot see them unless handed over.
    # (Returning results only made main merge its own EMPTY list.)
    return monitor, results, chunk_files


def main() -> int:
    args = parse_args()
    out = os.path.abspath(args.out)
    if not os.path.exists(out):
        print(f"FATAL: {out} not found -- nothing to repair", file=sys.stderr)
        return 1
    out_dir = os.path.dirname(out)
    today = datetime.now(UTC).date()

    df, counts, t, bounds = scan(out)
    last_day = max(d for _, d in counts.index)
    first_day = min(d for _, d in counts.index)
    assets = sorted(df.asset.unique())
    if args.assets:
        wanted = {s.strip().upper() for s in args.assets.split(",") if s.strip()}
        assets = [a for a in assets if a in wanted]
    exchanges = {a: "SMART" for a in assets}

    print("=" * 70)
    print(f"1-min file: {out}")
    print(f"  {len(df):,} bars | {len(assets)} assets | "
          f"{first_day} -> {last_day}")
    print(f"  request span: {MAX_DAYS_PER_REQUEST} day(s) per IB request "
          f"(IBKR step-size limit for 1-min bars)")
    print("=" * 70)

    repair_days: dict[str, list[_date]] = {}

    if args.mode in ("repair", "full"):
        repair_days = find_short_days(counts, last_day)
        # Sessions that start after the open are short too -- refetch them.
        for asset, days in late_open_days(t).items():
            if asset not in assets:
                continue
            tgt = repair_days.setdefault(asset, [])
            tgt.extend(d for d in days if d <= last_day)
            repair_days[asset] = sorted(set(tgt))

    total_short = sum(len(v) for v in repair_days.values())
    print(f"\nshort asset-days to refetch : {total_short:,}")
    print("  all are candidates: IBKR has no multi-year cutoff for 1-min bars.")
    print("  A failure here means no subscription (error 162), not age.")

    extend_from = None
    if args.mode in ("extend", "full"):
        target = last_day + timedelta(days=args.extend_days or 0)
        if args.extend_days == 0:
            target = today
        extend_from = (last_day, target)
        ndays = len(et_days_in(datetime(last_day.year, last_day.month, last_day.day, tzinfo=UTC),
                               datetime(target.year, target.month, target.day, tzinfo=UTC)))
        print(f"\nextend mode: {last_day} -> {target} (~{ndays} weekdays)")

    if args.mode == "repair" and not repair_days:
        print("\nnothing to repair -- every session has a full bar count.")

    plan = [(a, d) for a in assets for d in sorted(repair_days.get(a, []))]
    half_days = half_day_dates(bounds)
    if args.max_days > 0:
        plan = plan[:args.max_days]
    print(f"\nplan: {len(plan):,} asset-day fetches "
          f"(+ tail extension)" if extend_from else
          f"\nplan: {len(plan):,} asset-day fetches")

    # ── Fold in stale chunks from an interrupted run ─────────────────────
    # This MUST run before the --dry-run gate: the chunks are already-fetched
    # work written by a previous run that was killed before its final merge.
    # Returning early for a dry run would leave them on disk forever, and the
    # next dry run would report a stale "short asset-days" plan because the
    # repaired bars were never folded in.
    stale = sorted(Path(out_dir).glob("_1min_chunk_*.parquet"))
    if stale:
        print(f"\nmerging {len(stale)} stale chunk files from a previous run...")
        n = merge_chunks(out, [str(f) for f in stale],
                         memory_limit=args.duckdb_memory)
        print(f"  stale merge -> {n:,} bars")
        for f in stale:
            f.unlink()
        # counts are now stale; rescan so the plan reflects what was recovered
        df, counts, t, bounds = scan(out)

    if stale:
        # The plan was built before the sweep, so rebuild it: rows that were
        # sitting in orphan chunks are now in the parquet and must drop out.
        repair_days = find_short_days(counts, last_day)
        for asset, days in late_open_days(t).items():
            if asset not in assets:
                continue
            tgt = repair_days.setdefault(asset, [])
            tgt.extend(d for d in days if d <= last_day)
            repair_days[asset] = sorted(set(tgt))
        last_day = max(d for _, d in counts.index)
        first_day = min(d for _, d in counts.index)
        plan = [(a, d) for a in assets for d in sorted(repair_days.get(a, []))]
        half_days = half_day_dates(bounds)
        if args.max_days > 0:
            plan = plan[:args.max_days]
        print(f"  plan after recovery: {len(plan):,} asset-days "
              f"(was {len(plan) + len(stale)} before the sweep)")

    if args.dry_run:
        print("\ndry-run: not connecting to IB Gateway.")
        return 0

    if not plan and not extend_from:
        print("nothing to do.")
        return 0

    ib = IB()
    if not connect(ib, args.host, args.port, args.client_id, args.timeout):
        print("FATAL: could not connect to IB Gateway", file=sys.stderr)
        return 1
    print(f"\nconnected to {args.host}:{args.port} (clientId={args.client_id})")

    chunk_files: list[str] = []
    since_merge = 0
    chunk_num = 0
    fixed = 0
    still_short = 0
    empty: list[tuple[str, _date]] = []
    unchanged: list[tuple[str, _date, int, int]] = []
    gained: list[tuple[int, int]] = []
    permission_errors: list[tuple[str, _date, str]] = []
    other_errors: list[tuple[str, _date, str]] = []
    t0 = time.time()

    def flush(force: bool = False):
        nonlocal chunk_files, since_merge
        if chunk_files and (force or since_merge >= args.merge_every):
            n = merge_chunks(out, chunk_files)
            print(f"  merged -> {n:,} bars "
                  f"({(time.time() - t0) / 60:.1f} min)")
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files, since_merge = [], 0

    # ---- repair pass -----------------------------------------------------
    if plan:
        baseline = {k: int(v) for k, v in counts.items()}
        if args.concurrency > 1:
            gained_rows: list[tuple[str, _date, int]] = []
            print(f"\nrepair pass (CONCURRENT): {len(plan):,} asset-days, "
                  f"concurrency {args.concurrency}, no inter-request sleep")
            print(f"  measuring IBKR throttling "
                  f"(events -> _throttle_events.jsonl)")
            monitor, cres, chunk_files = run_concurrent(
                ib, plan, exchanges, args, baseline, out_dir, chunk_num,
                since_merge, t0, bounds, half_days)
            print(f"  persisted {len(chunk_files)} chunk file(s) "
                  f"({monitor.total:,} requests issued)")
            print(monitor.report())
            for (asset, day), (rows, err) in cres.items():
                n_got, n_had = len(rows), baseline.get((asset, day), 0)
                if err:
                    if classify_error(err) == "permission":
                        permission_errors.append((asset, day, err))
                    else:
                        other_errors.append((asset, day, err))
                    continue
                # With a narrowed request `n_got` counts ONLY the missing bars,
                # so comparing it to `n_had` is meaningless -- a day holding 300
                # of 390 correctly yields 90. Success is simply "we got the
                # bars that were absent".
                if n_got > 0:
                    fixed += 1
                    gained.append((n_had, n_had + n_got))
                else:
                    empty.append((asset, day))
            # Chunks were already persisted incrementally inside run_concurrent
            # (crash-resumable), so all that remains is one merge at the end.
            print(f"\nmerging {len(chunk_files)} persisted chunk files once...")
            n = merge_chunks(out, chunk_files, memory_limit=args.duckdb_memory)
            print(f"  merged -> {n:,} bars")
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files, since_merge = [], 0
        else:
            print(f"\nrepair pass: {len(plan):,} asset-days "
                  f"({MAX_DAYS_PER_REQUEST}-day requests)")
            errs: dict[str, str] = {}
            for i, (asset, day) in enumerate(plan, 1):
                errs.pop(asset, None)
                b = bounds.get((asset, day))
                rows = fetch_day(ib, asset, day, exchanges,
                                 args.throttle, args.retries, errors=errs,
                                 first_tod=b[0] if b else None,
                                 last_tod=b[1] if b else None)
                n_got = len(rows)
                n_had = baseline.get((asset, day), 0)
                if asset in errs:
                    if classify_error(errs[asset]) == "permission":
                        permission_errors.append((asset, day, errs[asset]))
                    else:
                        other_errors.append((asset, day, errs[asset]))
                if n_got > n_had:
                    fixed += 1
                    gained.append((n_had, n_got))
                elif n_got > 0:
                    still_short += 1
                    unchanged.append((asset, day, n_had, n_got))
                else:
                    empty.append((asset, day))
                if rows:
                    chunk_num += 1
                    chunk_files.append(write_chunk_file(rows, chunk_num, out_dir, prefix="1min"))
                    since_merge += 1
                if i % 50 == 0 or i == len(plan):
                    print(f"  [{i}/{len(plan)}] improved={fixed} "
                          f"unchanged={len(unchanged)} empty={len(empty)} "
                          f"({(time.time() - t0) / 60:.1f} min)")
                flush()

    # ---- extend pass -----------------------------------------------------
    if extend_from:
        lo, hi = extend_from
        print(f"\nextend pass: {lo} -> {hi} "
              f"({MAX_DAYS_PER_REQUEST}-day requests)")
        cur = datetime(hi.year, hi.month, hi.day, tzinfo=UTC)
        while cur.date() > lo:
            chunk_num += 1
            results = request_chunk(ib, assets, cur, MAX_DAYS_PER_REQUEST,
                                    args.throttle, args.retries,
                                    bar_size=BAR_SIZE, exchanges=exchanges)
            rows: list[dict] = []
            for sym, r in results.items():
                if r:
                    rows.extend([x for x in r if datetime.fromtimestamp(
                        x["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET).date() > lo])
            if rows:
                chunk_files.append(write_chunk_file(rows, chunk_num, out_dir, prefix="1min"))
                since_merge += 1
                print(f"  chunk ending {cur.date()}: {len(rows):,} bars")
            flush()
            cur = cur - timedelta(days=MAX_DAYS_PER_REQUEST)

    flush(force=True)
    ib.disconnect()

    print(f"\nDONE  improved={fixed:,}  unchanged={len(unchanged):,}  "
          f"empty={len(empty):,}  ({(time.time() - t0) / 60:.1f} min)")
    if gained:
        import statistics
        before = statistics.median(g[0] for g in gained)
        after = statistics.median(g[1] for g in gained)
        total_new = sum(g[1] - g[0] for g in gained)
        print(f"  bars per repaired day: median {before:.0f} -> {after:.0f} "
              f"(+{total_new:,} bars total)")

    if permission_errors:
        syms = sorted({s for s, _, _ in permission_errors})
        print(f"\nMARKET-DATA PERMISSION FAILURES: "
              f"{len(permission_errors):,} requests across {len(syms)} symbols")
        print("  IBKR error 162 = no subscription for that symbol. These")
        print("  asset-days are NOT age-limited; add a subscription and re-run.")
        print(f"  symbols: {syms[:12]}{'...' if len(syms) > 12 else ''}")
        print(f"  e.g. {permission_errors[0][2][:120]}")
    if other_errors:
        print(f"\nOTHER ERRORS: {len(other_errors):,}")
        print(f"  e.g. {other_errors[0][2][:120]}")
    if empty:
        print(f"\nEMPTY RESPONSES: {len(empty):,} asset-days returned 0 bars.")
        print("  Usually a market holiday that had partial coverage, or a")
        print("  delisted/suspended session. Sample: "
              f"{[(s, str(d)) for s, d in empty[:5]]}")
    if unchanged:
        print(f"\nUNCHANGED: {len(unchanged):,} asset-days came back with the "
              f"same bar count as before.")
        print("  Expected for genuine half-days and late-open (IPO) sessions,")
        print("  where the fetched count IS the correct count. Sample:")
        for s, d, had, got in unchanged[:8]:
            print(f"    {s:6s} {d}  {had} -> {got}")
    if still_short:
        print(f"\nSTILL SHORT after repair: {still_short:,} asset-days "
              f"still need another pass.")

    report(out, assets, {}, bar_minutes=BAR_MINUTES,
           expected_bars_per_day=BARS_PER_DAY, expect_assets=assets)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""pull_bars.py — the ONE bar puller for any IBKR bar size.

██ HOW TO RUN — READ THIS FIRST (2026-11-08, paid for in hours, do not
██ re-derive it):
██
██   1. `docker compose -f scripts/data_puller/docker-compose.yml up -d tws`
██   2. WAIT for `docker logs scripts-tws-1` to print
██      "gateway ready -- running: sleep infinity"  (login takes ~1-3 min;
██      `nc 127.0.0.1 4002` succeeding is NOT a readiness signal — docker
██      binds the proxy port at container start, long before IBC logs in).
██   3. RUN THE PULLER INSIDE THE CONTAINER:
██        docker cp <path>/pull_bars.py scripts-tws-1:/app/pull_bars.py
██        docker exec -w /app scripts-tws-1 \
██            python3 /app/pull_bars.py --mode full --out /tmp/x.parquet ...
██      (or the compose `pull-1min` service once its entrypoint points here).
██
██   DO NOT try to connect host-side (deps installed on the Mac, `nc`-style
██   self-checks, ib_async from the laptop against 127.0.0.1:4002). The
██   gateway itself was never the problem — its trusted-IP list already
██   accepts 127.0.0.1,172.18.0.1 (IBC `config.ini` TrustedTwsApiClientIPs,
██   baked into the image and mirrored in the container `jts.ini`). It is
██   the Docker-Desktop/mac host->container TCP path that stalls the API
██   handshake forever; the old scripts solved this YEARS ago by simply
██   running container-side where 127.0.0.1 IS the gateway. Container-side
██   was verified live 2026-11-08 (SPY 1,170 bars, report clean) after a
██   day was wasted trying host-side TCP fixes. Run it inside. That's it.

Replaces the archived ``experiments/pull_{5min,1min}_bars.py`` pair. The
correctness-critical machinery is ported VERBATIM from them; everything they
learned (see "Learnings" below) is carried, not re-derived.

Structure — three parts, and only one part takes the interesting flags:

  (a) data-store
      class ``Bars`` owns the parquet output: coverage day-sets (resume),
      crash-resumable chunk persistence, the DuckDB external-sort merge,
      the stale-chunk sweep, and the refuse-not-overwrite guard.

  (b) connect
      connect-or-die loudly. Booting the gateway is NOT this script's job:
      the ``tws`` service in ``scripts/data_puller/docker-compose.yml`` (ibc image) does
      that — run the stack first and this script only attaches to it.

  (c) the pull loop
      the ONLY part that takes ``--tf`` / ``--universe``. Modes:

        full   backfill --start -> --end for every asset in the universe,
               walking the cursor backwards from --end to --end minus
               chunk-days (then past the earliest fetched bar so chunks
               continue without gaps). Resume: assets already holding >= 60%
               of a chunk's expected weekdays are skipped for that chunk.
        extend last-file-date -> --end (today by default). Cheapest way to
               keep a file fresh; does not rescan coverage.
        repair scan the file per (asset, day) for short/late-open sessions
               and refetch ONLY the missing head or tail of each one
               ( intraday steps already on disk are never re-downloaded);
               empties are retried serially because IBKR silently drops
               over-concurrent requests.
      All three share the SAME fetch / persist / merge code paths.

CLI shape (per review):
  --tf         ANY IBKR barSizeSetting ("5 mins", "1 min", "30 secs",
               "10 mins", "1 hour", ...). NOT restricted to a preset list.
               The step-size table below caps --chunk-days per bar size.
  --universe   ONE file: a JSON {symbol: primaryExchange} map (written by
               the universe chain). No built-in symbol tables live here;
               module-level presets were the thing that silently fetched the
               wrong list when an import failed, so presets are GONE.
               (--assets = ad-hoc comma whitelist instead, for smoke runs.)
  --host/--port: DELETED. Connection target is hardcoded below on purpose:
      a wrong deployment should fail in one obvious place, not silently
      succeed against the wrong gateway through env drift.

Step-size table (why --chunk-days is validated, not free)
  IBKR publishes a step-size table: how much history one request may span
  per bar size. Overrunning it does NOT raise — IBKR silently returns a
  partial session. That silence is exactly how the original 1-min file
  accumulated 4,154 truncated asset-days across 131 dates (every asset cut
  at the same clock time: a request-size overrun signature). So:

      --chunk-days defaults to this table's entry for the --tf you pass
      and is HARD-VALIDATED against it — no invocation may overrun
      silently. Bar sizes not in the table have no verified cap here, so
      they REQUIRE an explicit --chunk-days (you own the risk).

      "1 min":  1 day   (documented: longest duration permitting 1-min bars)
      "5 mins": 60 days (default optimum; measured 30/60/90/120/180d all
                         return COMPLETE 5-min spans but latency jumps
                         ~5s -> ~70s past 60d, so 60d minimises
                         requests x latency)

Learnings ported verbatim from the archived pullers
  * formatDate=2 MANDATORY: =1 returns naive datetimes parsed in the
    process TZ (a UTC+8 host puts bars 12h off and corrupts stratlab.io
    minutes_since_open).
  * Layout contract: ts_epoch_ms int64 UTC ms | asset str | ohlcv
    float64 — matches stratlab/io.py byte-for-byte. The Arrow schema pins
    asset as `string` (NOT pandas large_string) for Rust-loader
    compat; the duckdb merge CASTs anyway because pa.concat_tables
    raises on the mismatch.
  * IB returns bars newest-first => sort ascending explicitly; the
    resume cursor derives from the earliest fetched bar - 1s so chunks
    continue without gaps.
  * Coverage = explicit (asset, et_date) day-set, not a min/max range,
    so mid-history holes get refetched; a >=0.6-of-expected-weekdays
    fold tolerates holidays without refetching whole chunks.
  * Merging: base file written by the Rust StatsLogger is Arrow
    `string`, pandas writes `large_string` — pa.concat_tables raises;
    duckdb CASTs every column, dedups with QUALIFY row_number()=1
    (streaming, never a second full copy), caps threads=2 (buffer pool
    is sized per thread; unbounded threads OOM before spilling), and
    writes to a temp + os.replace so an interrupted merge can never
    truncate the output.
  * resolve_contract() is a GATE, not an optimization: an unqualified
    contract makes IB answer Error 200 and ib_async hands back a SILENT
    EMPTY result (that silently cost 12 tickers in the first full
    run). Memoised whole-run including `False`, so delisted tickers are
    probed once, and a bare-SMART retry covers wrong exchange hints.
  * Sustained RATE breaks the gateway, not in-flight count: measured,
    ~4.8 req/s sustained => socket dropped (80% ConnectionErrors at
    conc 12; at conc 8/delay 0 25% of 5-min requests cancelled). The
    control that matters is a floor on the gap BETWEEN REQUEST STARTS
    (default 0.35s ~ 2.8 req/s). In-flight is separately capped at the
    documented 50 simultaneous (measured: conc 200 -> 125/200 EMPTY
    with no error at all — silent throttling).
  * Concurrent-mode empties are retried SERIALLY on the same loop via
    the ASYNC api: measured conc 200 -> 62.5% empty, conc 50 -> 28%,
    and every one of those same dates returns full bars serially.
    The sync wrapper (reqHistoricalData) calls run_until_complete()
    itself and DEADLOCKS (sat 40+ min in epoll) if re-entered after
    the outer `go()` already ran.
  * `nonlocal buf` is REQUIRED in async completions — without it
    `buf_n +=` binds local and every fetch "succeeds" while persisting
    nothing.
  * Chunk-file prefix MUST match the resume glob (a mismatch made
    resume impossible once already).
  * Error 162-family markers are permissions/subscription failures —
    reported SEPARATELY, never confused with short sessions.
  * A contract with no security definition is a UNIVERSE-LIST defect,
    not a data gap; it is called out separately at the end.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ib_async import IB, Stock

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

# ── connection target (deliberately HARDCODED) ───────────────────────────────
# The gateway is a fixed deployment fact, not a knob: compose boots tws on
# 127.0.0.1:4002 and this script talks to exactly that. Env/flag drift made
# this class of bug possible; a hardcoded wrong value at least fails loudly
# and obviously in one place.
HOST = "127.0.0.1"
PORT = 4002

# ── canonical output contract ────────────────────────────────────────────────
COLS = ["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"]
SCHEMA = pa.schema([
    ("ts_epoch_ms", pa.int64()),
    ("asset", pa.string()),          # rust StatsLogger: plain string, NOT large_string
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
])

OPEN_MIN = 570        # 09:30 ET, minutes from midnight
CLOSE_MIN = 960       # 16:00 ET
RTH_SECONDS = (CLOSE_MIN - OPEN_MIN) * 60   # 23,400s = 6.5h

# ── --tf handling ────────────────────────────────────────────────────────────
BAR_UNIT_SECONDS = {
    "sec": 1, "secs": 1, "min": 60, "mins": 60,
    "hour": 3600, "hours": 3600,
    "day": 86400, "days": 86400, "week": 604800, "weeks": 604800, "month": 2592000,
}
SLUG_SUFFIX = {"sec": "sec", "secs": "sec", "min": "min", "mins": "min",
               "hour": "hour", "hours": "hour", "day": "day", "days": "day",
               "week": "week", "weeks": "week", "month": "month"}

# (bar size -> max days per request). ONLY verified entries live here —
# documented IBKR caps or measured-on-this-account optima (see module
# docstring). Anything else demands an explicit --chunk-days.
STEP_TABLE = {
    "1 min": 1,
    "5 mins": 60,
}


def parse_tf(tf: str) -> tuple[str, int, int, int]:
    """-> (bar_size_str, bar_seconds, bar_minutes, bars_per_rth_day).

    Accepts any IBKR barSizeSetting string: "<n> <unit>" with unit in
    sec(s)/min(s)/hour(s)/day(s)/week(s)/month(s).
    """
    parts = tf.strip().split()
    if len(parts) != 2 or not parts[0].isdigit():
        raise ValueError(f"--tf must look like \"5 mins\" (got {tf!r})")
    n, unit = int(parts[0]), parts[1].lower()
    if unit not in BAR_UNIT_SECONDS:
        raise ValueError(f"--tf unit {unit!r} not an IBKR bar unit "
                         f"(sec/min/hour/day/week/month)")
    canon = f"{n} {unit}"          # canonical IBKR spelling: "5 mins", "1 hour"
    bar_seconds = n * BAR_UNIT_SECONDS[unit]
    if bar_seconds < 1 or bar_seconds > 2592000:
        raise ValueError(f"--tf {tf} out of range")
    bar_minutes = max(1, bar_seconds // 60)
    # RTH day bar count. Sub-request-sized bar sizes tile 6.5h in whole
    # stamps (an uneven tail still STAMPS as one bar), so ceil.
    bars_per_day = max(1, math.ceil(RTH_SECONDS / bar_seconds)) \
        if bar_seconds <= RTH_SECONDS else 1
    return canon, bar_seconds, bar_minutes, bars_per_day


def tf_slug(canonical: str) -> str:
    n, unit = canonical.split()
    return f"{n}{SLUG_SUFFIX[unit]}"


# ── part (a): the data store ────────────────────────────────────────────────
class Bars:
    """Owns the output parquet: coverage, chunk persistence, merging."""

    def __init__(self, out: str, label: str, duckdb_memory: str = "6GB"):
        self.out = out
        self.dir = os.path.dirname(os.path.abspath(out))
        self.label = label
        self.memory_limit = duckdb_memory

    # -- coverage ------------------------------------------------------------
    def load_covered(self, assets: list[str]) -> dict[str, set]:
        """Per-asset set of ET dates already present in `self.out`.

        A day-SET, not a min/max range: mid-history holes are then refetched
        instead of being silently skipped.
        """
        covered: dict[str, set] = {}
        if not os.path.exists(self.out):
            return covered
        df = pq.read_table(self.out, columns=["ts_epoch_ms", "asset"]).to_pandas()
        ts = pd.to_datetime(df["ts_epoch_ms"], unit="ms", utc=True)
        days = ts.dt.tz_convert(ET).dt.date
        wanted = set(assets)
        for sym, day in zip(df["asset"].to_numpy(), days):
            if sym in wanted:
                covered.setdefault(sym, set()).add(day)
        print(f"  resume: {len(df):,} existing bars, "
              f"{len(covered)}/{len(assets)} assets present")
        return covered

    @staticmethod
    def fold(covered: dict[str, set], sym: str, rows: list[dict]) -> None:
        covered.setdefault(sym, set()).update(
            datetime.fromtimestamp(r["ts_epoch_ms"] / 1000, tz=UTC)
            .astimezone(ET).date() for r in rows)

    # -- chunk persistence ----------------------------------------------------
    def leftovers_guard(self) -> None:
        """Refuse to start with unmerged chunk files of OUR prefix.

        The stale sweep should have merged and removed them; if any remain
        the sweep glob did not match (a prefix mismatch once made resume
        impossible for exactly this reason). Never silently overwrite.
        """
        leftovers = sorted(Path(self.dir).glob(f"_{self.label}_chunk_*.parquet"))
        if leftovers:
            raise RuntimeError(
                f"refusing to start: {len(leftovers)} unmerged chunk file(s) in "
                f"{self.dir} (e.g. {leftovers[0].name}). The stale sweep should "
                f"have merged and removed them; not overwriting them.")

    def write_chunk(self, rows: list[dict], chunk_num: int) -> str:
        """Crash-resumable chunk write; `self.label` MUST match the resume glob."""
        path = os.path.join(self.dir, f"_{self.label}_chunk_{chunk_num:04d}.parquet")
        df = pd.DataFrame(rows, columns=COLS)
        tbl = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
        pq.write_table(tbl, path, compression="snappy")
        return path

    # -- merging --------------------------------------------------------------
    def merge(self, chunk_files: list[str]) -> int:
        """Merge base + chunks into `self.out`: sorted (asset, ts), deduped.

        DuckDB because the inputs are NOT pre-sorted (the base stores
        per-asset blocks in hash order): it does the external sort, spills
        under `memory_limit`, and CASTs away the Arrow string vs large_string
        mismatch for free. Temp-file + os.replace = an interrupted merge can
        never truncate the output.
        """
        import duckdb
        inputs = [p for p in [self.out, *chunk_files] if os.path.exists(p)]
        if not inputs:
            return 0
        tmp = self.out + ".tmp"
        con = duckdb.connect()
        try:
            # Threads capped: the buffer pool is sized per thread, so
            # unbounded threads OOM before reaching the spill threshold.
            con.execute("SET threads=2;")
            con.execute(f"SET memory_limit='{self.memory_limit}';")
            con.execute(f"SET temp_directory='{self.dir}';")
            con.execute("SET preserve_insertion_order=false;")
            union = " UNION ALL ".join(
                f"SELECT CAST(ts_epoch_ms AS BIGINT) AS ts_epoch_ms, "
                f"CAST(asset AS VARCHAR) AS asset, "
                f"CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, "
                f"CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS close, "
                f"CAST(volume AS DOUBLE) AS volume "
                f"FROM read_parquet('{p}')" for p in inputs)
            # QUALIFY row_number()=1 is a streaming dedup once sorted, so
            # this never materialises a second full copy of the dataset.
            con.execute(
                f"COPY (SELECT * FROM ({union}) "
                f"QUALIFY row_number() OVER "
                f"(PARTITION BY asset, ts_epoch_ms ORDER BY ts_epoch_ms) = 1 "
                f"ORDER BY asset, ts_epoch_ms) "
                f"TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
            n = con.execute(
                f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
        finally:
            con.close()
        os.replace(tmp, self.out)   # atomic
        return int(n)

    def sweep_stale(self) -> int:
        """Fold + delete this run's orphan chunks. Returns new row count (0=none)."""
        stale = sorted(Path(self.dir).glob(f"_{self.label}_chunk_*.parquet"))
        if not stale:
            return 0
        print(f"  merging {len(stale)} stale chunk files from a previous run...")
        n = self.merge([str(f) for f in stale])
        print(f"  ✓ stale merge -> {n:,} bars")
        for f in stale:
            f.unlink()
        return n

    # -- repair-mode scan -------------------------------------------------------
    def scan(self, assets: list[str] | None = None):
        """Per-(asset, day) bar counts + first/last minute-of-day bounds.

        Counts are the repair baseline: a session is "repaired" when the
        refetch returns MORE bars than the file already had (scoring against
        a flat bars/day would permanently fail genuine half-days). The
        per-day bounds let the fetch be narrowed to just the missing head or
        tail. `dict` on purpose: DataFrame.get(key) is a COLUMN lookup and
        silently returned None for row keys once, degrading every request to
        a full-day fetch.
        """
        df = pq.read_table(self.out, columns=["ts_epoch_ms", "asset"]).to_pandas()
        et = pd.DatetimeIndex(
            pd.to_datetime(df["ts_epoch_ms"].to_numpy(), unit="ms", utc=True)
        ).tz_convert(ET)
        t = pd.DataFrame({"asset": df.asset.to_numpy(), "day": et.date,
                          "tod": et.hour * 60 + et.minute})
        if assets is not None:
            t = t[t.asset.isin(assets)]
        counts = t.groupby(["asset", "day"]).size()
        agg = t.groupby(["asset", "day"])["tod"].agg(["min", "max"])
        bounds = {(a, d): (int(r["min"]), int(r["max"])) for (a, d), r in agg.iterrows()}
        return df, counts, t, bounds


def et_days_in(start: datetime, end: datetime) -> list[_date]:
    """Weekdays between two dates (ET). Used only for gap *estimation*."""
    out, cur = [], start.date()
    while cur <= end.date():
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


# ── part (b): connect ───────────────────────────────────────────────────────
def connect_or_die(client_id: int, timeout: int) -> IB:
    """Attach to the gateway at HOST:PORT or exit loudly.

    Booting the gateway is NOT our job — compose (`tws`, ibc image) does
    that; if nothing is listening the retry loop below times out and we
    die with a message that says exactly what to start.

    WHICH SIDE OF THE SOCKET YOU RUN ON:
      this script is meant to run INSIDE the gateway's container (docker
      exec scripts-tws-1 ... or the compose pull-1min service). A host-side
      run on Docker Desktop/mac stalls below on TimeoutError no matter
      what you do to the gateway — do not debug it, do not retry it; the
      module docstring ("HOW TO RUN") has the one correct invocation.
    """
    ib = IB()
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            ib.connect(HOST, PORT, clientId=client_id, timeout=10)
            print(f"  connected to IB Gateway {HOST}:{PORT} (clientId={client_id})")
            return ib
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  connect failed ({e}); retrying in 5s...", file=sys.stderr)
            time.sleep(5)
    print(f"FATAL: could not connect to IB Gateway {HOST}:{PORT}: {last}\n"
          f"       the gateway is NOT this script's job — start it with "
          f"`docker compose -f scripts/data_puller/docker-compose.yml up tws`, and check "
          f"the ibc health page. HOST/PORT are hardcoded on purpose; edit "
          f"pull_bars.py if the deployment really moved.\n"
          f"       Then run THIS SCRIPT INSIDE the container\n"
          f"       (docker exec -w /app scripts-tws-1 python3\n"
          f"       /app/pull_bars.py ...) — a host-side run stalls exactly "
          f"like this forever; see module docstring HOW TO RUN. Do not "
          f"debug the host-side path.", file=sys.stderr)
    raise SystemExit(1)


# ── part (c): fetch primitives (verbatim-ported) ────────────────────────────
def make_contract(symbol: str, exchanges: dict | None = None) -> Stock:
    primary = (exchanges or {}).get(symbol, "SMART")
    return Stock(symbol, "SMART", "USD", primaryExchange=primary)


def bars_to_rows(bars, symbol: str) -> list[dict]:
    """Convert IB BarData -> row dicts, sorted ascending by timestamp.

    `formatDate=2` makes `bar.date` tz-aware UTC (or an int epoch), so
    `.timestamp()` is unambiguous here.
    """
    rows = []
    for b in bars:
        d = b.date
        ts_ms = int(d.timestamp() * 1000) if hasattr(d, "timestamp") else int(d) * 1000
        rows.append({
            "ts_epoch_ms": ts_ms,
            "asset": symbol,
            "open": float(b.open), "high": float(b.high), "low": float(b.low),
            "close": float(b.close), "volume": float(b.volume),
        })
    rows.sort(key=lambda r: r["ts_epoch_ms"])
    return rows


def resolve_contract(ib: IB, symbol: str, exchanges: dict, cache: dict):
    """Return a *qualified* contract for `symbol`, or False if unresolvable.

    Qualification is mandatory, not optional: an unqualified contract makes
    IB reply Error 200 "No security definition has been found for the
    request", and ib_async hands that back as an empty bar list rather than
    raising. The caller cannot tell "delisted" from "I never asked properly".

    Results are memoised in `cache` for the whole run -- qualification is a
    blocking round-trip, and redoing it per chunk would cost
    n_assets x n_chunks calls. `False` is cached too, so a delisted ticker
    is not re-probed every chunk.
    """
    if symbol in cache:
        return cache[symbol]
    contract = make_contract(symbol, exchanges)
    try:
        ib.qualifyContracts(contract)
    except Exception:  # noqa: BLE001
        pass
    if getattr(contract, "conId", 0):
        cache[symbol] = contract
        return contract
    cache[symbol] = False
    print(f"    ! {symbol}: no security definition "
          f"(delisted/renamed? trying without primaryExchange)", flush=True)
    # Retry once as a bare SMART contract -- our exchange hint is sometimes
    # simply wrong for a symbol that has since changed primary listing.
    bare = make_contract(symbol, {})
    try:
        ib.qualifyContracts(bare)
    except Exception:  # noqa: BLE001
        pass
    if getattr(bare, "conId", 0):
        print(f"    + {symbol}: resolved without primaryExchange hint", flush=True)
        cache[symbol] = bare
        return bare
    print(f"    x {symbol}: genuinely unresolvable, skipping", flush=True)
    return False


def request_chunk(ib: IB, symbols: list[str], end_dt: datetime,
                  duration_str: str, throttle: float, retries: int,
                  bar_size: str, exchanges: dict | None = None,
                  errors: dict | None = None) -> dict[str, list]:
    """Fetch one (symbols x window) slice serially. Returns symbol -> rows.

    Serial path: qualify + sync reqHistoricalData per symbol. `errors` maps
    symbol -> last exception text so callers can tell a permissions failure
    (IBKR error 162) apart from a genuinely empty session.
    """
    end_str = end_dt.astimezone(ET).strftime("%Y%m%d %H:%M:%S US/Eastern")
    results: dict[str, list] = {}
    for n, sym in enumerate(symbols, 1):
        contract = make_contract(sym, exchanges)
        rows: list[dict] = []
        for attempt in range(1, retries + 1):
            try:
                ib.qualifyContracts(contract)
                bars = ib.reqHistoricalData(
                    contract, endDateTime=end_str, durationStr=duration_str,
                    barSizeSetting=bar_size, whatToShow="TRADES",
                    useRTH=True, formatDate=2, timeout=120,
                )
                rows = bars_to_rows(bars or [], sym)
                break
            except Exception as e:  # noqa: BLE001
                if errors is not None:
                    errors[sym] = str(e)
                if attempt == retries:
                    print(f"    {sym} window@{end_dt.date()} FAILED after "
                          f"{retries} attempts: {e}", file=sys.stderr)
                    rows = []
                else:
                    back = 2 ** attempt
                    print(f"    {sym} error ({e}); retry {attempt}/{retries} "
                          f"in {back}s", file=sys.stderr)
                    time.sleep(back)
        results[sym] = rows
        # IB pacing: a flat sleep between every symbol request is what keeps
        # us under the request ceiling.
        if throttle > 0 and n < len(symbols):
            time.sleep(throttle)
    return results


def fetch_chunk_concurrent(ib: IB, symbols: list[str], end_dt: datetime,
                           duration_str: str, args, exchanges: dict,
                           monitor, persist, contracts: dict) -> dict:
    """Fetch one (symbols x window) slice concurrently, paced + persisted.

    Two things the archived pullers established the hard way, both applied:
      * SUSTAINED RATE is what breaks the gateway, not in-flight count.
        Measured at conc 8 / delay 0: 25% of 5-min requests cancelled by
        IB. The floor on gap-between-request-STARTS is the control.
      * Rows must flush to chunk files as they arrive — a kill before the
        final merge otherwise loses everything since the last merge.
    """
    import asyncio
    import nest_asyncio

    nest_asyncio.apply()
    from ib_throttle import MAX_SIMULTANEOUS

    conc = max(1, min(args.concurrency, MAX_SIMULTANEOUS))
    sem = asyncio.Semaphore(conc)
    gap = max(0.0, args.request_delay)
    lock = asyncio.Lock()
    last = [0.0]
    results: dict[str, tuple[list[dict], str | None]] = {}
    cache = contracts

    def end_str():
        return end_dt.astimezone(ET).strftime("%Y%m%d %H:%M:%S US/Eastern")

    async def one(sym):
        if gap:
            async with lock:
                loop = asyncio.get_event_loop()
                w = last[0] + gap - loop.time()
                if w > 0:
                    await asyncio.sleep(w)
                last[0] = loop.time()
        async with sem:
            c = cache.get(sym)
            if c is None:
                # An UNQUALIFIED contract makes IB answer Error 200 "No
                # security definition has been found", which surfaces as a
                # SILENT empty result, not an exception. That silently cost
                # 12 tickers in the first full run. resolve_contract() is
                # the gate.
                c = resolve_contract(ib, sym, exchanges, cache)
            if c is False:
                results[sym] = ([], "unresolvable: no security definition")
                return
            rows: list[dict] = []
            err: str | None = None
            for attempt in range(1, max(1, args.retries) + 1):
                monitor.begin(sym)
                try:
                    bars = await ib.reqHistoricalDataAsync(
                        c, end_str(), duration_str, args.bar_size,
                        "TRADES", True, 2, False, [], 300)
                    rows = bars_to_rows(list(bars), sym)
                    monitor.end(sym, len(rows), None)
                    err = None
                    break
                except Exception as e:  # noqa: BLE001
                    err = f"{type(e).__name__}: {e}"
                    monitor.end(sym, 0, err)
                    # Error 162 is IB cancelling us for load. Back off so
                    # the retry doesn't add to the pressure that caused it.
                    if attempt < max(1, args.retries):
                        await asyncio.sleep(2.0 * attempt)
            results[sym] = (rows, err)
            if rows:
                persist(rows)

    async def go():
        await asyncio.gather(*(one(s) for s in symbols))

    asyncio.get_event_loop().run_until_complete(go())
    return results


# permission/subscription failure markers (reported separately, never
# confused with short sessions). Error 162 = "Historical Market Data Service
# error message: No market data permissions for <SYM>".
PERMISSION_ERR_MARKERS = ("162", "no market data permissions",
                          "not subscribed", "requires additional subscription",
                          "market data permissions")


def classify_error(msg: str) -> str:
    low = (msg or "").lower()
    return "permission" if any(m in low for m in PERMISSION_ERR_MARKERS) else "other"


# ── repair-mode machinery (the 1-min script, generalized) ────────────────────
def find_short_days(counts: pd.Series, bars_per_day: int,
                    last_day: _date) -> dict[str, list[_date]]:
    """Per-asset dates with 0 < bars < a full RTH session.

    Days with zero bars are absent entirely and are handled separately
    (they are typically market holidays).
    """
    short: dict[str, list[_date]] = {}
    sub = counts[(counts > 0) & (counts < bars_per_day)]
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


def half_day_dates(bounds: dict, bar_minutes: int) -> set:
    """Dates where the session ended at the canonical 13:00 close.

    A genuine half-day (day after Thanksgiving, July 3, Christmas Eve) ends
    at 12:59 with NOTHING missing — requesting 13:00-16:00 correctly
    returns 0 bars, which looks identical to throttling if you only count
    empties. Identified by the majority of assets ending at the half-day's
    last-stamp time together (12:59 stamped for 1-min bars, 12:55 for
    5-min bars), which distinguishes a market-wide early close from a
    truncation artifact.
    """
    half_last_min = 780 - bar_minutes   # last stamp of a 12:59-close session
    per_day = {}
    for (asset, day), (_lo, hi) in bounds.items():
        d = per_day.setdefault(day, [0, 0])
        d[1] += 1
        if hi == half_last_min:
            d[0] += 1
    return {day for day, (nh, tot) in per_day.items()
            if tot >= 5 and nh / tot >= 0.8}


def missing_window(day: _date, first_tod: int, last_tod: int, bar_seconds: int,
                   bar_minutes: int, skip_half_days: set | None = None):
    """Smallest (endDateTime, durationStr) that yields ONLY the bars we lack.

    IBKR returns bars in [endDateTime - duration, endDateTime]. The file
    already holds the intact part of a truncated session, so re-requesting
    the whole day wastes most of the payload — a session cut at 13:29 keeps
    240 of 390 1-min bars and needs only the 150 after it.

    Returns (end_dt, duration_str, mode) where mode is "tail" (session ends
    early -> fetch the afternoon), "head" (starts late -> fetch the
    morning), or None when the session is already complete or a genuine
    half-day.
    """
    if (skip_half_days and day in skip_half_days
            and first_tod == OPEN_MIN and last_tod == 780 - bar_minutes):
        return None                     # 09:30->12:59 early close: nothing missing
    last_stamp = CLOSE_MIN - bar_minutes            # final stamped bar minute
    if last_tod < last_stamp:                       # ends early -> need tail
        # +1 bar of slack so the boundary bar is included in the response
        # and can then be filtered out precisely.
        dur = (CLOSE_MIN - last_tod) * 60 + bar_seconds
        end = datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=ET)
        return end, f"{dur} S", "tail"
    if first_tod > OPEN_MIN:                        # starts late -> need head
        dur = (first_tod - OPEN_MIN) * 60 + bar_seconds
        end = datetime(day.year, day.month, day.day,
                       first_tod // 60, first_tod % 60, tzinfo=ET)
        return end, f"{dur} S", "head"
    return None


def fetch_day(ib: IB, symbol: str, day: _date, exchanges: dict, throttle: float,
              retries: int, bar_size: str, bar_seconds: int, bar_minutes: int,
              errors: dict | None = None,
              first_tod: int | None = None,
              last_tod: int | None = None) -> list[dict]:
    """Fetch ONLY the missing portion of one asset-day's RTH session.

    With bounds (minute-of-day of the bars already on disk) the request is
    narrowed to the missing head or tail; without them it falls back to the
    full session. In both cases the result is filtered to the date and the
    missing half, so nothing already stored is re-downloaded or re-merged.
    """
    span = None
    if first_tod is not None and last_tod is not None:
        span = missing_window(day, first_tod, last_tod, bar_seconds, bar_minutes)
    if span is None:
        window_days = max(1, min(STEP_TABLE.get(bar_size, 1),
                                 1 if bar_seconds >= 3600 else STEP_TABLE.get(bar_size, 1)))
        end = datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=ET)
        duration, mode = f"{window_days} D", "all"
    else:
        end, duration, mode = span

    rows = request_chunk(ib, [symbol], end, duration, throttle, retries,
                         bar_size=bar_size, exchanges=exchanges,
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


def run_repair_concurrent(ib, plan, exchanges, args, bounds, skip_half_days,
                          store: Bars, monitor):
    """Fire every asset-day request concurrently, then retry empties serially.

    Empirical counterpart of the published pacing rules: run at the given
    rate, record peak in-flight + throttle responses, then use the serial
    path for everything that came back empty — because IBKR silently drops
    requests under concurrency (measured: conc 200 -> 62.5% empty, conc 50
    -> 28%, every one of those same dates full serially). The result is
    therefore independent of the throttle threshold; genuine half-days come
    back short and are recorded, not retried forever.
    """
    import asyncio
    import nest_asyncio

    nest_asyncio.apply()

    # Documented max simultaneous open historical-data requests. The
    # 200-request run at concurrency 200 hit peak_in_flight=200 and got
    # 125/200 EMPTY with no error — silent throttling, not holidays.
    conc = max(1, min(args.concurrency, 50))
    if conc != args.concurrency:
        print(f"  capping concurrency {args.concurrency} -> {conc} "
              f"(IBKR documented max 50 simultaneous)")
    sem = asyncio.Semaphore(conc)

    # Sustained RATE is what actually breaks the gateway, not concurrency:
    # at conc 12 the first 16 min were clean, then the socket dropped on 80%
    # of requests once the cumulative rate hit ~4.8 req/s. A floor on the
    # gap between request STARTS bounds that rate regardless of in-flight.
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

    contracts: dict[str, object] = {}
    results: dict[tuple[str, _date], tuple[list[dict], str | None]] = {}

    # Crash-resumable persistence: rows flush to chunk files as they arrive
    # (a kill costs at most `persist_every` rows; the stale sweep on the
    # next start is what makes a resume recover the work).
    buf: list[dict] = []
    buf_n = 0
    seq = {"n": 0}
    chunk_files: list[str] = []

    def flush_buffer(force: bool = False):
        nonlocal buf, buf_n       # REQUIRED: without it `buf_n +=` binds local
        #                              # and every fetch "succeeds" while
        #                              # persisting nothing.
        if not buf or (not force and buf_n < args.persist_every):
            return
        n_rows = buf_n
        seq["n"] += 1
        cf = store.write_chunk(buf, seq["n"])
        chunk_files.append(cf)
        buf, buf_n = [], 0
        print(f"  [persisted {n_rows:,} rows -> {os.path.basename(cf)}] "
              f"({len(chunk_files)} chunk files, "
              f"{monitor.total:,} requests done)", flush=True)

    store.leftovers_guard()

    def fmt(key, end_epoch):
        asset, day = key
        if asset not in contracts:
            contracts[asset] = make_contract(asset, exchanges)
        end = datetime.fromtimestamp(end_epoch, tz=UTC).astimezone(ET)
        return contracts[asset], end.strftime("%Y%m%d %H:%M:%S US/Eastern")

    async def go():
        tasks = []
        for asset, day in plan:
            b = bounds.get((asset, day))
            if b is None:
                span = (datetime(day.year, day.month, day.day, 16, 0, tzinfo=ET),
                        f"1 D", "all")
            else:
                # Returns None for a complete session OR a half-day; both
                # mean nothing to request, so bail before unpacking.
                span = missing_window(day, b[0], b[1], args.bar_seconds,
                                      args.bar_minutes, skip_half_days)
            if span is None:
                continue
            end_dt, dur, mode = span
            end_epoch = int(end_dt.astimezone(UTC).timestamp())
            tasks.append(((asset, day), end_epoch, dur, mode))

        async def one(key, end_epoch, dur, mode):
            nonlocal buf, buf_n
            asset, day = key
            contract, end_str_x = fmt(key, end_epoch)
            lo = hi = None
            b = bounds.get((asset, day))
            if b is not None:
                lo, hi = b
            await pace()               # bound the SUSTAINED request rate
            async with sem:            # bound the in-flight count
                monitor.begin(f"{asset}@{day}")
                try:
                    bars = await ib.reqHistoricalDataAsync(
                        contract, end_str_x, dur, args.bar_size,
                        "TRADES", True, 2, False, [], 120)
                    rows = []
                    for r in bars_to_rows(list(bars), asset):
                        d = datetime.fromtimestamp(
                            r["ts_epoch_ms"] / 1000, tz=UTC).astimezone(ET)
                        if d.date() != day:
                            continue
                        tod = d.hour * 60 + d.minute
                        if mode == "tail" and hi is not None and tod <= hi:
                            continue        # already on disk
                        if mode == "head" and lo is not None and tod >= lo:
                            continue        # already on disk
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

    # Serial retry pass. MUST stay on the same event loop and use the ASYNC
    # api: the synchronous wrapper (reqHistoricalData) calls
    # run_until_complete() itself, and re-entering the loop after go()
    # already ran DEADLOCKS (sat idle in epoll for 40+ minutes with zero
    # output). Hence async, one at a time.
    async def retry_all():
        # buf/buf_n MUST be nonlocal here or `buf_n += len(rows)` below
        # raises UnboundLocalError and the blanket `except` then OVERWRITES
        # the successful results[key] assignment with ([], error) -- every
        # pass "recovers" data the runner can't see and refetches forever
        # (same class as the flush_buffer nonlocal note at L774).
        nonlocal buf, buf_n
        for attempt in range(1, args.retry_passes + 1):
            # Retry BOTH silent empties AND hard errors: a gateway socket
            # disconnect mid-run would otherwise lose those asset-days
            # outright (2,645 lost in one archived run).
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
                span = missing_window(day, b[0], b[1], args.bar_seconds,
                                      args.bar_minutes, skip_half_days)
                if span is None:
                    results[key] = ([], "nothing missing")
                    continue
                end_dt, dur, mode = span
                end_s = end_dt.strftime("%Y%m%d %H:%M:%S US/Eastern")
                c = contracts.get(asset) or make_contract(asset, exchanges)
                contracts[asset] = c
                monitor.begin(f"retry:{asset}@{day}")
                try:
                    bars = await asyncio.wait_for(
                        ib.reqHistoricalDataAsync(
                            c, end_s, dur, args.bar_size, "TRADES", True,
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
                    monitor.end(f"retry:{asset}@{day}", 0,
                                f"{type(e).__name__}: {e}")
                    results[key] = ([], f"{type(e).__name__}: {e}")
                await asyncio.sleep(0.4)
            print(f"    recovered {recovered}/{len(missing)}", flush=True)
            if recovered == 0:
                print("    no further progress; remaining are genuine empties",
                      flush=True)
                break

    asyncio.get_event_loop().run_until_complete(retry_all())
    flush_buffer(force=True)
    return monitor, results, chunk_files


# ── args ─────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tf", default=os.environ.get("TF", "5 mins"),
                   help="IBKR barSizeSetting, ANY size: \"30 secs\", "
                        "\"1 min\", \"2 mins\", \"3 mins\", \"5 mins\", "
                        "\"10 mins\", \"15 mins\", \"20 mins\", \"30 mins\", "
                        "\"1 hour\", ... . --chunk-days is capped per "
                        "step-size entry (see STEP_TABLE).")
    p.add_argument("--universe", default=os.environ.get("UNIVERSE", ""),
                   help="ONE file: JSON {symbol: primaryExchange}, written by "
                        "the universe chain. Used as-is; no built-in symbol "
                        "lists exist here (presets fetched the WRONG list "
                        "silently once an import failed).")
    p.add_argument("--mode", choices=["full", "extend", "repair"],
                   default=os.environ.get("MODE", "extend"),
                   help="full   = backfill --start -> --end for the universe "
                        "(resume-skips covered chunks); "
                        "extend = last-file-date -> --end (today by default), "
                        "the cheap way to keep a file fresh; "
                        "repair = scan per (asset,day) short/late sessions "
                        "and refetch ONLY the missing head/tail (intraday "
                        "steps only; deepest on file). All share the same "
                        "fetch/persist/merge machinery.")
    p.add_argument("--start", default=os.environ.get("START_DATE", "2021-01-01"),
                   help="full mode: backfill start (inclusive)")
    p.add_argument("--end", default=os.environ.get(
        "END_DATE", datetime.now(UTC).strftime("%Y-%m-%d")),
        help="full/extend mode: window end (inclusive; today by default)")
    p.add_argument("--chunk-days", type=int,
                   default=int(os.environ.get("CHUNK_DAYS", "0")),
                   help="days per request. 0 = use the STEP_TABLE entry for "
                        "--tf (1 min -> 1 D, 5 mins -> 60 D measured "
                        "30/60/90/120/180d all COMPLETE but latency ~5s -> "
                        "~70s past 60d so 60d minimises requests x latency). "
                        "HARD-VALIDATED: values past the step cap exit loudly "
                        "instead of silently truncating sessions (that silence "
                        "is what damaged 4,154 1-min asset-days).")
    p.add_argument("--concurrency", type=int,
                   default=int(os.environ.get("CONCURRENCY", "1")),
                   help=">1 fires a chunk's symbol requests concurrently "
                        "(in-flight capped at IBKR's documented 50; empties "
                        "are always retried serially afterwards because IBKR "
                        "drops over-concurrent requests silently)")
    p.add_argument("--request-delay", type=float,
                   default=float(os.environ.get("REQUEST_DELAY", "0.35")),
                   help="floor on the gap between request STARTS. This, not "
                        "concurrency, is what keeps the gateway socket alive: "
                        "at ~0 it sustained ~4.8 req/s and IBKR disconnected "
                        "mid-run. 0.35s caps the rate near 2.8 req/s.")
    p.add_argument("--throttle", type=float,
                   default=float(os.environ.get("THROTTLE", "0.5")),
                   help="serial path: seconds between symbol requests")
    p.add_argument("--retries", type=int, default=int(os.environ.get("RETRIES", "3")))
    p.add_argument("--retry-passes", type=int,
                   default=int(os.environ.get("RETRY_PASSES", "4")),
                   help="repair mode: serial retry passes for empty responses")
    p.add_argument("--persist-every", type=int,
                   default=int(os.environ.get("PERSIST_EVERY", "200000")),
                   help="flush fetched rows to a chunk file every N rows so a "
                        "kill loses at most this much work")
    p.add_argument("--merge-every", type=int,
                   default=int(os.environ.get("MERGE_EVERY", "5")),
                   help="merge chunk files into the output after this many "
                        "(serial full/extend path)")
    p.add_argument("--duckdb-memory", default=os.environ.get("DUCKDB_MEMORY", "6GB"),
                   help="memory_limit for the merge sort")
    p.add_argument("--out", default=os.environ.get("OUTPUT", ""),
                   help="output parquet. Default: data/backtest-results/stats/"
                        "raw_<tf-slug>_bars.parquet")
    p.add_argument("--seed", default=os.environ.get("SEED", ""),
                   help="full mode only: base parquet to seed --out from if "
                        "absent (for e.g. growing the original 41-asset file)")
    p.add_argument("--timeout", type=int, default=int(os.environ.get("IB_TIMEOUT", "60")))
    p.add_argument("--client-id", type=int,
                   default=int(os.environ.get("IB_CLIENT_ID", "15")))
    p.add_argument("--max-assets", type=int,
                   default=int(os.environ.get("MAX_ASSETS", "0")),
                   help="limit asset count (0 = all); for smoke tests")
    p.add_argument("--assets", default=os.environ.get("ASSETS", ""),
                   help="comma-separated symbol whitelist (overrides "
                        "--universe; for ad-hoc/smoke runs)")
    p.add_argument("--max-days", type=int,
                   default=int(os.environ.get("MAX_DAYS", "0")),
                   help="repair mode: cap asset-days repaired (smoke tests)")
    p.add_argument("--extend-days", type=int,
                   default=int(os.environ.get("EXTEND_DAYS", "0")),
                   help="extend mode: days past the file's last date "
                        "(0 = up to --end/today)")
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan (chunks x assets) and exit without "
                        "connecting")
    return p.parse_args()


# ── verification report (tf-parameterized) ──────────────────────────────────
def report(out: str, symbols: list[str], empty_assets: dict, bar_seconds: int,
           bars_per_day: int, expect_assets: list[str] | None = None) -> None:
    step_ms = bar_seconds * 1000
    bar_minutes = bar_seconds // 60
    full_rows = bars_per_day
    last_bar_min = OPEN_MIN + full_rows * bar_minutes - bar_minutes
    pf = pq.ParquetFile(out)
    print(f"\n{'=' * 66}\nVERIFY {out}\n{'=' * 66}")
    print(f"rows={pf.metadata.num_rows:,}  row_groups={pf.metadata.num_row_groups}")
    # Nullability-agnostic compare: duckdb's merge inherits NOT NULL from the
    # Rust StatsLogger seed's required fields, so a seeded file's arrow schema
    # carries non-nullable annotations on every column while SCHEMA's fields
    # are nullable. The types/names are what the loaders depend on; a strict
    # .equals() would false-alarm on every seeded full-universe run.
    got = pf.schema_arrow
    schema_ok = len(got) == len(SCHEMA) and all(
        f.name == s.name and f.type == s.type for f, s in zip(got, SCHEMA))
    print(f"schema matches canonical: {schema_ok}")
    tbl = pf.read(columns=["ts_epoch_ms", "asset"])
    df = tbl.to_pandas()
    ts = df["ts_epoch_ms"].to_numpy()
    off = int((ts % step_ms != 0).sum())
    print(f"grid: {tf_slug_label(bar_seconds)} (step {step_ms // 1000}s)  "
          f"off-grid timestamps: {off:,}")
    et = pd.DatetimeIndex(pd.to_datetime(ts, unit="ms", utc=True)).tz_convert(ET)
    mod = et.hour * 60 + et.minute
    print(f"ET minute-of-day: {mod.min()}..{mod.max()} "
          f"(expect {OPEN_MIN}..{last_bar_min})")
    print(f"pre-09:30 bars: {int((mod < OPEN_MIN).sum()):,}   "
          f"at/after-16:00 bars: {int((mod >= CLOSE_MIN).sum()):,}")
    print(f"weekend bars: {int((et.dayofweek >= 5).sum()):,}")
    dup = int(df.duplicated(subset=["ts_epoch_ms", "asset"]).sum())
    print(f"duplicate (ts,asset): {dup:,}")
    per = pd.DataFrame({"asset": df.asset.to_numpy(), "day": et.date}) \
        .groupby(["asset", "day"]).size()
    full = int((per == full_rows).sum())
    print(f"asset-days: {len(per):,}  full({full_rows})={full:,}  "
          f"partial={len(per) - full:,}")
    have = set(df.asset.unique())
    print(f"assets present: {len(have):,}")
    if expect_assets:
        missing = [s for s in expect_assets if s not in have]
        print(f"  expected-but-absent: {len(missing)}")
        if missing:
            print(f"  {missing[:12]}{'...' if len(missing) > 12 else ''}")
    if empty_assets:
        top = sorted(empty_assets.items(), key=lambda kv: -kv[1])[:10]
        print(f"symbols that returned 0 bars on >=1 window: {len(empty_assets)}")
        print(f"  worst: {top}")


def tf_slug_label(bar_seconds: int) -> str:
    if bar_seconds % 86400 == 0:
        d = bar_seconds // 86400
        return f"{d}-day bars" if d > 1 else "1-day bars"
    if bar_seconds % 3600 == 0:
        h = bar_seconds // 3600
        return f"{h}-hour bars" if h > 1 else "1-hour bars"
    if bar_seconds >= 60:
        m = bar_seconds // 60
        return f"{m}-min bars" if m > 1 else "1-min bars"
    return f"{bar_seconds}-sec bars"


# ── main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    args = parse_args()

    canonical, bar_seconds, bar_minutes, bars_per_day = parse_tf(args.tf)
    slug = tf_slug(canonical)
    args.bar_size = canonical
    args.bar_seconds = bar_seconds
    args.bar_minutes = bar_minutes
    args.bars_per_day = bars_per_day

    step_default = STEP_TABLE.get(canonical)
    if args.chunk_days == 0:
        if step_default is None:
            print(f"FATAL: no verified step-size entry for --tf {canonical!r}; "
                  f"pass --chunk-days explicitly. Known caps: "
                  f"{', '.join(f'{k} -> {v} D' for k, v in STEP_TABLE.items())}",
                  file=sys.stderr)
            return 2
        args.chunk_days = step_default
    elif step_default is not None and args.chunk_days > step_default:
        # Overrunning the step size does NOT error at IBKR's side: it
        # silently returns partial sessions. That is exactly how the
        # original 1-min file accumulated 4,154 truncated asset-days. Refuse.
        print(f"FATAL: --chunk-days {args.chunk_days} exceeds the step-size "
              f"cap for {canonical!r} ({step_default} D). Overrunning it "
              f"silently TRUNCATES sessions instead of failing — use "
              f"--chunk-days <= {step_default} or pass a larger --tf.",
              file=sys.stderr)
        return 2

    if args.out:
        out = os.path.abspath(args.out)
    else:
        out = os.path.abspath(str(here.parent / "data" / "backtest-results"
                                  / "stats" / f"raw_{slug}_bars.parquet"))
    store = Bars(out, slug, args.duckdb_memory)

    # Universe: ONE file, {symbol: primaryExchange}. No built-in presets.
    # EXCEPTION — repair mode with no asset flags: the asset list comes from
    # the FILE itself (scan(a=None) reads everything present), and an
    # exchange map defaults to SMART-only (refetch machinery doesn't need
    # the primary exchange hint to re-hit the same contract).
    if args.mode != "repair" or args.assets or args.universe:
        if args.assets:
            symbols = [s.strip().upper() for s in args.assets.split(",") if s.strip()]
            exchanges = {s: "SMART" for s in symbols}
            print(f"assets: ad-hoc whitelist ({len(symbols)})")
        elif args.universe:
            with open(args.universe) as fh:
                uni = json.load(fh)
            exchanges = {str(k).upper(): v for k, v in uni.items()}
            symbols = list(exchanges)     # file order, preserved
            print(f"assets: universe file {args.universe} ({len(symbols)} symbols)")
            unknown = [s for s in symbols if not isinstance(uni.get(s), str)]
            if unknown:
                print(f"FATAL: universe entries without an exchange string: "
                      f"{unknown[:10]}", file=sys.stderr)
                return 2
        else:
            print("FATAL: nothing to pull — pass --universe FILE (JSON "
                  "{sym: exchange}) or --assets SYM,SYM.", file=sys.stderr)
            return 2
    else:
        symbols = []
        exchanges = {}
        print("assets: (repair) scanning the FILE — no universe needed")

    # ORDER-PRESERVING dedup. Duplicates are not merely wasted requests:
    # dup rows survive the merge as exact duplicates and the verifier's
    # duplicate check fails on an otherwise-clean file.
    seen: set[str] = set()
    symbols = [s for s in symbols if not (s in seen or seen.add(s))]
    if args.max_assets > 0:
        symbols = symbols[:args.max_assets]

    os.makedirs(store.dir, exist_ok=True)
    today = datetime.now(UTC).date()
    t0 = time.time()

    print(f"Pulling {canonical} bars [{args.mode}] : {len(symbols)} assets, "
          f"{args.chunk_days}D/request (step cap for this tf: "
          f"{step_default if step_default else 'UNVERIFIED — explicit'}), "
          f"delay={args.request_delay}s conc={args.concurrency}")
    print(f"  out={out}")

    if args.mode == "repair":
        if not os.path.exists(out):
            print(f"FATAL: {out} not found — nothing to repair "
                  f"(use --mode extend or full)", file=sys.stderr)
            return 1
        return repair_mode(args, symbols, exchanges, store, t0)

    # ---- full / extend: chunked backfill ------------------------------------
    if args.mode == "full":
        start = datetime.fromisoformat(args.start).replace(tzinfo=UTC)
        if not os.path.exists(out):
            seed = args.seed or os.path.join(store.dir, "raw_bars.parquet")
            if os.path.exists(seed):
                import shutil
                print(f"  seeding {out} from {seed}")
                shutil.copy(seed, out)
    else:  # extend
        if not os.path.exists(out):
            print(f"FATAL: {out} not found — extend needs an existing file "
                  f"(use --mode full)", file=sys.stderr)
            return 1

    end = datetime.fromisoformat(args.end).replace(tzinfo=UTC)

    if args.mode == "extend":
        covered = {}
        df0 = store.load_covered(symbols)
        # last-file-date from the file itself (cheap front-page read)
        ts0 = pd.to_datetime(
            pq.read_table(out, columns=["ts_epoch_ms"]).to_pandas()["ts_epoch_ms"],
            unit="ms", utc=True)
        last_day0 = ts0.dt.tz_convert(ET).dt.date.max()
        start = datetime(last_day0.year, last_day0.month, last_day0.day,
                         tzinfo=UTC) + timedelta(days=args.extend_days or 0)
        if args.extend_days == 0:
            start = datetime(today.year, today.month, today.day, tzinfo=UTC)
        # extend pulls STRICTLY NEW days; rewrite start to the day AFTER the
        # file's last so the resume fold does not fight the file
        start = max(start, datetime(last_day0.year, last_day0.month, last_day0.day,
                                    tzinfo=UTC) + timedelta(days=1))
        if start > end:
            print(f"extend: file already holds through {last_day0}; "
                  f"target {end.date()} — nothing to do.")
            return 0
        start = start - timedelta(days=args.chunk_days)   # chunk containing the next new day

    covered = store.load_covered(symbols)
    n_stale = store.sweep_stale()
    if n_stale:
        covered = store.load_covered(symbols)

    n_chunks = max(1, (end - start).days // args.chunk_days + 1)
    print(f"  plan: ~{n_chunks} chunks x {args.chunk_days}d "
          f"({start.date()} -> {end.date()})")
    if args.dry_run:
        print(f"  dry-run: ~{n_chunks * len(symbols)} requests "
              f"(~{n_chunks * len(symbols) * max(args.throttle, args.request_delay) / 3600:.1f}h "
              f"of pacing alone)")
        return 0

    ib = connect_or_die(args.client_id, args.timeout)

    from ib_throttle import ThrottleMonitor
    monitor = ThrottleMonitor(
        log_path=Path(store.dir) / f"_throttle_events_{slug}.jsonl")

    contracts: dict = {}
    errs: dict[str, str] = {}
    other_errors: list[tuple[str, int, str]] = []
    buf: list[dict] = []
    _p = {"n": 0, "rows": 0, "files": []}

    def persist(rows):
        buf.extend(rows)
        _p["rows"] += len(rows)
        if _p["rows"] >= args.persist_every:
            _p["n"] += 1
            cf = store.write_chunk(buf, _p["n"])
            _p["files"].append(cf)
            print(f"    [persisted {_p['rows']:,} rows -> "
                  f"{os.path.basename(cf)}] ({monitor.total:,} requests)",
                  flush=True)
            buf.clear()
            _p["rows"] = 0

    cur_end = end
    chunk_num = 0
    chunk_files: list[str] = []
    since_merge = 0
    total_new = 0
    empty_assets: dict[str, int] = {}

    while cur_end > start:
        chunk_num += 1
        chunk_start = cur_end - timedelta(days=args.chunk_days)
        if chunk_start <= start:
            chunk_start = start

        if args.mode == "full":
            # Only request symbols not already covering this window. A
            # symbol counts as covered at ~60% of expected weekdays, which
            # tolerates holidays without refetching whole chunks.
            expected = et_days_in(chunk_start, cur_end)
            need = []
            for s in symbols:
                have = covered.get(s, set())
                if sum(1 for d in expected if d in have) >= len(expected) * 0.6:
                    continue
                need.append(s)
        else:
            need = list(symbols)   # extend: only new days exist beyond the file

        if need:
            print(f"  chunk {chunk_num}: {len(need)}/{len(symbols)} assets need "
                  f"data, {chunk_start.date()} -> {cur_end.date()}")
            duration = f"{args.chunk_days} D"
            results = (
                fetch_chunk_concurrent(
                    ib, need, cur_end, duration, args, exchanges,
                    monitor, persist, contracts)
                if args.concurrency > 1 else
                request_chunk(ib, need, cur_end, duration, args.throttle,
                              args.retries, bar_size=canonical,
                              exchanges=exchanges, errors=errs))

            new_rows: list[dict] = []
            pairs = (list(results.items()) if args.concurrency > 1
                     else [(sym, (rows, None)) for sym, rows in results.items()])
            for sym, (rows, err) in pairs:
                if err:
                    other_errors.append((sym, chunk_num, err))
                if rows:
                    new_rows.extend(rows)
                    Bars.fold(covered, sym, rows)
                else:
                    empty_assets[sym] = empty_assets.get(sym, 0) + 1

            if new_rows:
                total_new += len(new_rows)
                if args.concurrency > 1:
                    pass          # persisted incrementally via persist()
                else:
                    cf = store.write_chunk(new_rows, chunk_num)
                    chunk_files.append(cf)
                    since_merge += 1
                # Cursor steps to just before the earliest bar seen, so the
                # next chunk continues WITHOUT a gap; flat fallback when a
                # symbol returned nothing at all.
                min_ts = min(r["ts_epoch_ms"] for r in new_rows)
                earliest = datetime.fromtimestamp(min_ts / 1000, tz=UTC)
                next_end = earliest - timedelta(seconds=1)
                print(f"    -> {len(new_rows):,} new bars "
                      f"(earliest {earliest.astimezone(ET):%Y-%m-%d})")
            else:
                print("    -> no new bars this window")
                next_end = chunk_start
        else:
            next_end = chunk_start

        if since_merge >= args.merge_every and chunk_files:
            print(f"  merging {len(chunk_files)} chunk files...")
            n = store.merge(chunk_files)
            print(f"  ✓ merged: {n:,} bars "
                  f"({(time.time() - t0) / 60:.1f} min elapsed)")
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files = []
            since_merge = 0

        if next_end <= start:
            break
        cur_end = next_end

    ib.disconnect()

    if buf:
        _p["n"] += 1
        cf = store.write_chunk(buf, _p["n"])
        _p["files"].append(cf)
        print(f"  final persist: {len(buf):,} rows -> {os.path.basename(cf)}")
        buf.clear()
    # Chunk-files reconciliation: concurrent mode persisted through `_p`;
    # the serial path appended directly. Overwriting unconditionally would
    # DISCARD the serial chunks and leave them unmerged on disk.
    if args.concurrency > 1:
        chunk_files = _p["files"]

    print(monitor.report())

    if chunk_files:
        print(f"  final merge of {len(chunk_files)} chunk files...")
        store.merge(chunk_files)
        for cf in chunk_files:
            os.unlink(cf)

    n_final = pq.ParquetFile(out).metadata.num_rows if os.path.exists(out) else 0
    print(f"\nDONE: {n_final:,} bars in {out}  ({total_new:,} newly fetched, "
          f"{(time.time() - t0) / 60:.1f} min)")

    finish_report(out, symbols, contracts, other_errors, empty_assets, canonical,
                  bar_seconds, bars_per_day)
    return 0


def finish_report(out, symbols, contracts, other_errors, empty_assets,
                  canonical, bar_seconds, bars_per_day):
    # Unresolvable contracts MUST be called out separately: they land in
    # empty_assets alongside genuine pre-IPO windows, where they read as
    # "delisted" — but the real cause is that IB has NO contract at all,
    # which is a universe-list defect, not a data gap.
    dead = sorted(s for s, c in contracts.items() if c is False)
    if dead:
        print(f"\n*** {len(dead)} ticker(s) have NO resolvable contract on "
              f"this account and were SKIPPED:")
        print(f"    {', '.join(dead)}")
        print("    These need a universe-list fix (delisted/renamed, or not "
              "available on this paper account). They are NOT fetch failures.")
    if other_errors:
        by_sym: dict[str, int] = {}
        for s, _c, _e in other_errors:
            by_sym[s] = by_sym.get(s, 0) + 1
        print(f"\nrequest errors: {len(other_errors):,} across "
              f"{len(by_sym)} ticker(s)")
        for s, n in sorted(by_sym.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    {s:<8} {n} window(s) failed")
    report(out, symbols, empty_assets, bar_seconds, bars_per_day,
           expect_assets=symbols)


# ── repair mode ──────────────────────────────────────────────────────────────
def repair_mode(args, symbols, exchanges, store: Bars, t0) -> int:
    from ib_throttle import ThrottleMonitor

    df, counts, t, bounds = store.scan(assets=symbols if args.assets else None)
    all_assets = sorted(df.asset.unique())
    last_day = max(d for _, d in counts.index)
    first_day = min(d for _, d in counts.index)

    print("=" * 70)
    print(f"file: {store.out}")
    print(f"  {len(df):,} bars | {len(all_assets)} assets | "
          f"{first_day} -> {last_day}")
    print(f"  request span: {STEP_TABLE.get(args.bar_size, 1)} day(s) per IB "
          f"request for {args.bar_size} bars")
    print(f"  full session = {args.bars_per_day} bars (09:30-16:00 ET)")
    print("=" * 70)

    repair_days: dict[str, list[_date]] = {}
    repair_days = find_short_days(counts, args.bars_per_day, last_day)
    # Sessions that start after the open are short too -- refetch them.
    for asset, days in late_open_days(t).items():
        if asset not in all_assets:
            continue
        tgt = repair_days.setdefault(asset, [])
        tgt.extend(d for d in days if d <= last_day)
        repair_days[asset] = sorted(set(tgt))

    plan = [(a, d) for a in all_assets for d in sorted(repair_days.get(a, []))]
    half_days = half_day_dates(bounds, args.bar_minutes)
    if args.max_days > 0:
        plan = plan[:args.max_days]

    # Fold stale chunks BEFORE the dry-run gate: chunks are already-fetched
    # work a killed run never merged; a dry-run that returned early would
    # leave them on disk forever and repeatedly report a stale plan.
    store.sweep_stale()
    df, counts, t, bounds = store.scan(assets=symbols if args.assets else None)
    last_day = max(d for _, d in counts.index)
    repair_days = find_short_days(counts, args.bars_per_day, last_day)
    for asset, days in late_open_days(t).items():
        if asset not in all_assets:
            continue
        tgt = repair_days.setdefault(asset, [])
        tgt.extend(d for d in days if d <= last_day)
        repair_days[asset] = sorted(set(tgt))
    plan = [(a, d) for a in all_assets for d in sorted(repair_days.get(a, []))]
    half_days = half_day_dates(bounds, args.bar_minutes)
    if args.max_days > 0:
        plan = plan[:args.max_days]

    total_short = sum(len(v) for v in repair_days.values())
    print(f"short asset-days to refetch : {total_short:,} "
          f"(+ genuine half-days exempted: {len(half_days)} dates)")
    print(f"plan: {len(plan):,} asset-day fetches")
    if args.dry_run:
        print("dry-run: not connecting to IB Gateway.")
        return 0
    if not plan:
        print("nothing to repair — every session has a full bar count.")
        return 0

    ib = connect_or_die(args.client_id, args.timeout)
    monitor = ThrottleMonitor(
        log_path=Path(store.dir) / f"_throttle_events_{store.label}.jsonl")

    out = store.out
    fixed = 0
    still_short = 0
    empty: list[tuple[str, _date]] = []
    unchanged: list[tuple[str, _date, int, int]] = []
    gained: list[tuple[int, int]] = []
    permission_errors: list[tuple[str, _date, str]] = []
    other_errors: list[tuple[str, _date, str]] = []
    chunk_files: list[str] = []
    since_merge = 0
    chunk_num = 0

    def flush(force: bool = False):
        nonlocal chunk_files, since_merge
        if chunk_files and (force or since_merge >= args.merge_every):
            n = store.merge(chunk_files)
            print(f"  merged -> {n:,} bars "
                  f"({(time.time() - t0) / 60:.1f} min)")
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files, since_merge = [], 0

    baseline = {k: int(v) for k, v in counts.items()}

    if args.concurrency > 1:
        print(f"\nrepair pass (CONCURRENT): {len(plan):,} asset-days, "
              f"concurrency {args.concurrency}, delay {args.request_delay}s; "
              f"empties retried serially (events -> "
              f"_throttle_events_{store.label}.jsonl)")
        monitor, cres, chunk_files = run_repair_concurrent(
            ib, plan, exchanges, args, bounds, half_days, store, monitor)
        print(monitor.report())
        for (asset, day), (rows, err) in cres.items():
            n_had = baseline.get((asset, day), 0)
            if err:
                if classify_error(err) == "permission":
                    permission_errors.append((asset, day, err))
                else:
                    other_errors.append((asset, day, err))
                continue
            # With a narrowed request `len(rows)` counts ONLY the missing
            # bars, so comparing it to `n_had` is meaningless — a day
            # holding 300 of 390 correctly yields 90. Success is simply
            # "we got the bars that were absent".
            if rows:
                fixed += 1
                gained.append((n_had, n_had + len(rows)))
            else:
                empty.append((asset, day))
        # Chunks were persisted incrementally inside run_repair_concurrent
        # (crash-resumable); one merge at the end folds them all.
        if chunk_files:
            print(f"\nmerging {len(chunk_files)} persisted chunk files once...")
            store.merge(chunk_files)
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files, since_merge = [], 0
    else:
        print(f"\nrepair pass: {len(plan):,} asset-days "
              f"(narrowed head/tail requests)")
        errs: dict[str, str] = {}
        for i, (asset, day) in enumerate(plan, 1):
            errs.pop(asset, None)
            b = bounds.get((asset, day))
            rows = fetch_day(ib, asset, day, exchanges, args.throttle,
                             args.retries, args.bar_size, args.bar_seconds,
                             args.bar_minutes, errors=errs,
                             first_tod=b[0] if b else None,
                             last_tod=b[1] if b else None)
            n_got, n_had = len(rows), baseline.get((asset, day), 0)
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
                chunk_files.append(store.write_chunk(rows, chunk_num))
                since_merge += 1
            if i % 50 == 0 or i == len(plan):
                print(f"  [{i}/{len(plan)}] improved={fixed} "
                      f"unchanged={len(unchanged)} empty={len(empty)} "
                      f"({(time.time() - t0) / 60:.1f} min)")
            flush()

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
        print("  are NOT short sessions and NOT age-limited; add a "
              "subscription and re-run.")
        print(f"  symbols: {syms[:12]}{'...' if len(syms) > 12 else ''}")
        print(f"  e.g. {permission_errors[0][2][:120]}")
    if other_errors:
        print(f"\nOTHER ERRORS: {len(other_errors):,}")
        print(f"  e.g. {other_errors[0][2][:120]}")
    if empty:
        print(f"\nEMPTY RESPONSES: {len(empty):,} asset-days returned 0 bars.")
        print("  Usually a market holiday with partial coverage, or a "
              "delisted/suspended session. Sample: "
              f"{[(s, str(d)) for s, d in empty[:5]]}")
    if unchanged:
        print(f"\nUNCHANGED: {len(unchanged):,} asset-days came back with the "
              f"same bar count as before.")
        print("  Expected for genuine half-days and late-open (IPO) sessions, "
              "where the fetched count IS the correct count. Sample:")
        for s, d, had, got in unchanged[:8]:
            print(f"    {s:6s} {d}  {had} -> {got}")
    if still_short:
        print(f"\nSTILL SHORT after repair: {still_short:,} asset-days "
              f"still need another pass.")

    report(out, all_assets, {}, args.bar_seconds, args.bars_per_day,
           expect_assets=all_assets)
    return 0


if __name__ == "__main__":
    sys.exit(main())

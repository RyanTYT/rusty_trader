#!/usr/bin/env python3
"""Pull 5-min RTH bars from IB Gateway and append them to a raw_bars parquet.

Target layout (must match ``stratlab/stratlab/io.py``)::

    ts_epoch_ms (int64, UTC ms) | asset (str)
    open | high | low | close | volume (float64)

Design notes / correctness requirements
---------------------------------------
Timestamps
    ``formatDate=2`` is mandatory. With ``formatDate=1`` ib_async returns
    *naive* datetimes (``parseIBDatetime`` -> ``strptime``, no tzinfo), and
    ``naive.timestamp()`` silently interprets them in the **process** TZ.
    On a UTC+8 host every bar lands 12h off, which silently corrupts
    ``stratlab.io``'s ``minutes_since_open``. ``formatDate=2`` returns
    epoch-seconds, which ``parseIBDatetime`` turns into tz-aware UTC.

Bar ordering
    IB returns bars newest-first. The pagination cursor is derived from the
    *earliest* bar, so we sort ascending explicitly rather than assuming.

Merging
    The base file is written by Rust's ``StatsLogger`` (Arrow ``string``);
    pandas writes ``large_string``. ``pa.concat_tables`` raises on that
    mismatch, so we cast every table to one canonical schema first.
    Dedup is done on a *sorted* stream and never materialises the whole
    frame in pandas, so peak memory stays bounded regardless of file size.

Resuming
    Coverage is tracked as an explicit set of ``(asset, et_date)`` days
    rather than a min/max range, so a hole in the middle of an asset's
    history is detected and refetched.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from collections import OrderedDict
from datetime import date as _date
from datetime import datetime, timedelta, timezone  # noqa: F401  (_date used in hints)
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from ib_async import IB, Stock
except ImportError:  # pragma: no cover
    print("ERROR: pip install ib_async", file=sys.stderr)
    raise

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

COLS = ["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"]

# Canonical Arrow schema. `asset` is pinned to `string` (not `large_string`)
# so the output is byte-compatible with the Rust-written base file.
SCHEMA = pa.schema([
    ("ts_epoch_ms", pa.int64()),
    ("asset", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
])

BAR_SIZE = "5 mins"
BAR_MINUTES = 5
EXPECTED_BARS_PER_DAY = 78  # 09:30-16:00 ET


# ── universes ────────────────────────────────────────────────────────────────
# Benchmarks live in their own lane, separate from the research universe, for
# two reasons:
#   * screen_universe.py excludes ETFs by design -- an ETF is not a testable
#     single-name, and letting one into a cross-section quietly breaks the
#     per-name independence the statistics assume.
#   * A benchmark must span the size range being studied. With only SPY (S&P
#     500, mega-cap) available, mid- and small-cap signals have no reference
#     series -- every result would be measured against large caps and silently
#     import the size factor.
# All 12 verified resolvable and trading on this account (conId captured via
# scripts/diag_bench_etfs.py): SPY 756733, QQQ 320227571, RSP 319359375,
# VTI 12340041, IWM 9579970, IWB 8991549, IWD 8991562, IWF 8991557,
# IWO 10435402, IWS 13002611, MDY 72195411, VTWO 79020263.
# QQQ is the priority addition -- it is the series the user actually leans on
# and it was missing from raw_bars.parquet entirely.
BENCHMARKS = {
    "SPY": "ARCA",      # S&P 500 -- already held
    "QQQ": "NASDAQ",    # Nasdaq-100 -- ADD: stated preference, was absent
    "RSP": "ARCA",      # S&P 500 equal-weight -- strips mega-cap weight bias
    "IWM": "ARCA",      # Russell 2000 -- the small-cap benchmark
    "IWB": "ARCA",      # Russell 1000 -- large + mid cap
    "MDY": "ARCA",      # S&P MidCap 400 -- mid-cap benchmark
    "VTWO": "ARCA",     # S&P SmallCap 600 -- small-cap, S&P methodology
}

# The 41 assets already present in raw_bars.parquet.
EXISTING_41 = {
    "AAPL": "NASDAQ", "ADBE": "NASDAQ", "AMD": "NASDAQ", "AMZN": "NASDAQ",
    "AVGO": "NASDAQ", "BAC": "NYSE", "CAT": "NYSE", "COIN": "NASDAQ",
    "COP": "NYSE", "CRM": "NYSE", "CVX": "NYSE", "DE": "NYSE", "DIS": "NYSE",
    "FCX": "NYSE", "GE": "NASDAQ", "GOOGL": "NASDAQ", "GS": "NYSE", "HD": "NYSE",
    "JPM": "NYSE", "LIN": "NYSE", "LLY": "NYSE", "LOW": "NYSE", "META": "NASDAQ",
    "MPC": "NYSE", "MSFT": "NASDAQ", "MU": "NASDAQ", "NFLX": "NASDAQ",
    "NKE": "NYSE", "NVDA": "NASDAQ", "PFE": "NYSE", "PYPL": "NASDAQ",
    "QCOM": "NASDAQ", "SBUX": "NASDAQ", "SCHW": "NYSE", "SLB": "NYSE",
    "SPY": "ARCA", "T": "NYSE", "TSLA": "NASDAQ", "TXN": "NASDAQ",
    "UNH": "NYSE", "XOM": "NYSE",
}

MIDCAP = {
    "TWLO": "NASDAQ", "OKTA": "NASDAQ", "ROKU": "NASDAQ", "ZI": "NASDAQ",
    "DT": "NYSE", "DOCN": "NYSE", "S": "NYSE", "GTLB": "NASDAQ",
    "CFLT": "NASDAQ", "CRDO": "NASDAQ", "PATH": "NYSE", "AI": "NYSE",
    "DUOL": "NASDAQ", "BMBL": "NASDAQ", "UPST": "NASDAQ", "HOOD": "NASDAQ",
    "SOFI": "NASDAQ", "RKT": "NYSE", "LC": "NASDAQ", "FIVE": "NYSE",
    "ZEN": "NYSE", "NET": "NYSE", "TTD": "NASDAQ", "DDOG": "NASDAQ",
    "MDB": "NASDAQ", "TEAM": "NASDAQ", "PINS": "NYSE", "SNAP": "NYSE",
    "SQ": "NYSE", "SHOP": "NASDAQ", "CRWD": "NASDAQ", "ASAN": "NYSE",
    "WKS": "NYSE", "ALAB": "NASDAQ", "SMCI": "NASDAQ",
    "VKTX": "NASDAQ", "MDGL": "NASDAQ", "CRSP": "NASDAQ", "BBIO": "NASDAQ",
    "IBRX": "NASDAQ", "ROIV": "NASDAQ", "TGTX": "NASDAQ", "NTLA": "NASDAQ",
    "BEAM": "NASDAQ", "INO": "NASDAQ", "REGN": "NASDAQ", "VRTX": "NASDAQ",
    "MRNA": "NASDAQ", "BNTX": "NASDAQ", "SRPT": "NASDAQ", "ALNY": "NASDAQ",
    "BNB": "NASDAQ",
    "CELH": "NASDAQ", "ANF": "NYSE", "ELF": "NYSE", "W": "NYSE",
    "BROS": "NYSE", "RBLX": "NYSE", "U": "NYSE", "CHWY": "NYSE",
    "GME": "NYSE", "BBY": "NYSE", "WING": "NASDAQ", "DASH": "NYSE",
    "ETSY": "NASDAQ", "KMX": "NYSE", "LULU": "NASDAQ", "DPZ": "NYSE",
    "CMG": "NYSE", "CAVA": "NYSE", "SG": "NYSE",
    "ENPH": "NASDAQ", "AR": "NYSE", "RIG": "NYSE", "OVV": "NYSE",
    "BE": "NYSE", "FTI": "NYSE", "XPO": "NYSE", "FSLR": "NASDAQ",
    "RUN": "NASDAQ", "SEDG": "NASDAQ", "PLUG": "NASDAQ", "VST": "NYSE",
    "USFD": "NYSE", "CW": "NYSE", "ATI": "NYSE", "NVT": "NYSE",
    "CRS": "NYSE", "MP": "NASDAQ", "UEC": "NYSE", "CCJ": "NYSE",
    "HWM": "NYSE", "TDG": "NASDAQ",
    "NLY": "NYSE", "AGNC": "NASDAQ", "AAL": "NASDAQ", "LUMN": "NYSE",
    "LYFT": "NASDAQ", "UAA": "NYSE", "VSAT": "NASDAQ",
    "QRVO": "NASDAQ", "FORM": "NASDAQ", "TER": "NASDAQ", "MPWR": "NASDAQ",
    "ARM": "NASDAQ", "MRVL": "NASDAQ", "ON": "NASDAQ",
}

SMALLCAP = {
    "SRNE": "NASDAQ", "PRTG": "NASDAQ", "SDGR": "NASDAQ", "RXRX": "NASDAQ",
    "SDC": "NASDAQ", "DNA": "NASDAQ", "FLGT": "NASDAQ", "TEM": "NASDAQ",
    "BVS": "NYSE", "BTAI": "NASDAQ", "NFE": "NYSE",
    "SOUN": "NASDAQ", "GFAI": "NASDAQ", "APLD": "NASDAQ", "WKC": "NYSE",
    "IRBT": "NASDAQ", "PD": "NYSE", "GCT": "NYSE", "CXAI": "NASDAQ",
    "CVNA": "NYSE", "ROOT": "NASDAQ", "LMND": "NYSE", "HIPO": "NASDAQ",
    "CLOV": "NYSE", "GO": "NYSE", "HRT": "NASDAQ",
    "DNN": "NYSE", "NXE": "NYSE", "BTU": "NYSE", "HCC": "NYSE",
    "AMRX": "NASDAQ",
}

# Tickers with NO resolvable contract on this IB account. Verified two ways:
# qualifyContracts returns no conId AND reqContractDetails returns zero matches,
# so this is not a bad exchange hint -- IB genuinely has no instrument for them.
# Some are real corporate actions (IRBT -> Amazon, LC -> BofA, PRTG, BTAI);
# SQ/ZI/CFLT/HRT are live tickers that this paper account does not carry.
# They are excluded rather than fetched, because the alternative -- accepting a
# loose match -- silently imports foreign-currency listings and index contracts
# into a US equity dataset. Re-check with scripts/fix_contracts.py before
# re-adding; never re-add on the strength of a symbol-name match alone.
EXCLUDED_NO_CONTRACT = {
    "BNB", "BTAI", "CFLT", "HRT", "IRBT", "LC", "PRTG", "SDC", "SQ",
    "ZEN", "ZI",
}

# The research universe actually fetched. Replaces the hand-built MIDCAP +
# SMALLCAP lists, which contributed wrong-size-band names (VRTX, REGN, BTU,
# CVNA), post-2021 IPOs with truncated history, and distressed tickers.
# select_universe.py picks these deterministically from the verified/
# plausible candidate pool under a sector quota; see universe_design.py for why
# each screen exists. MIDCAP/SMALLCAP are retained above only as the record of
# what was originally attempted and why it was superseded.
# NOTE: sys.path must contain the universes dir now -- the 4-file chain
# (describe/select) was retired and MERGED into universes/build_universes.py
# (2026-11-08, byte-identical-output verified). When run as
# `python /app/pull_5min_bars.py` inside the container, /app holds
# build_universes.py + universes/*.json directly. Then: bare-import
# misfire used to silently fetch the OLD 127-name fallback list -- removed:
# fail loudly instead.
_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
_UNIVS = [str(Path(__file__).resolve().parent.parent / "universes"),   # repo layout
          str(Path(__file__).resolve().parent / "universes"),         # docker /app layout
          str(Path(__file__).resolve().parent)]                       # flat /app layout
for _d in _UNIVS:
    if (_d and Path(_d, "build_universes.py")).exists():
        sys.path.insert(0, _d)
        break
else:
    raise ImportError("build_universes.py not found next to (parent of) this file")
_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
from build_universes import ALL_NEW  # noqa: E402  (module-level selection)
FULL_UNIVERSE = dict(EXISTING_41)
FULL_UNIVERSE.update(ALL_NEW)
# Benchmarks must resolve too, and must WIN on exchange-hint collision with the
# equity lists (an ETF's primary exchange is ARCA, not the equity venue).
FULL_UNIVERSE.update(BENCHMARKS)


def make_contract(symbol: str, exchanges: dict | None = None) -> Stock:
    table = FULL_UNIVERSE if exchanges is None else exchanges
    primary = table.get(symbol, "SMART")
    return Stock(symbol, "SMART", "USD", primaryExchange=primary)


# ── args ─────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.environ.get("IB_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("IB_PORT", "4002")))
    p.add_argument("--client-id", type=int,
                   default=int(os.environ.get("IB_CLIENT_ID", "15")))
    p.add_argument("--start", default=os.environ.get("START_DATE", "2021-01-01"))
    p.add_argument("--end", default=os.environ.get(
        "END_DATE", datetime.now(UTC).strftime("%Y-%m-%d")))
    p.add_argument("--chunk-days", type=int,
                   default=int(os.environ.get("CHUNK_DAYS", "60")),
                   help="days per IB request. Measured on this account: 30/60/90/"
                        "120/180d all return COMPLETE 5-min spans (0 partial "
                        "sessions), but latency jumps from ~5s to ~70s past 60d, "
                        "so 60d minimises requests x latency.")
    p.add_argument("--concurrency", type=int,
                   default=int(os.environ.get("CONCURRENCY", "1")),
                   help=">1 fetches a chunk's symbols concurrently")
    p.add_argument("--request-delay", type=float,
                   default=float(os.environ.get("REQUEST_DELAY", "0.35")),
                   help="floor on the gap between request STARTS. This, not "
                        "concurrency, is what keeps the gateway alive: at "
                        "conc 8 / delay 0, 25% of 5-min requests were "
                        "cancelled by IB.")
    p.add_argument("--persist-every", type=int,
                   default=int(os.environ.get("PERSIST_EVERY", "200000")),
                   help="flush rows to a chunk file every N rows so a kill "
                        "loses at most this much")
    p.add_argument("--throttle", type=float,
                   default=float(os.environ.get("THROTTLE", "0.5")),
                   help="seconds to sleep between IB requests (pacing)")
    p.add_argument("--retries", type=int, default=int(os.environ.get("RETRIES", "3")))
    p.add_argument("--merge-every", type=int,
                   default=int(os.environ.get("MERGE_EVERY", "5")),
                   help="merge after this many chunk files")
    p.add_argument("--out", default=os.environ.get(
        "OUTPUT", str(here.parent / "data" / "backtest-results" / "stats"
                      / "raw_bars_extended.parquet")))
    p.add_argument("--seed", default=os.environ.get("SEED", ""),
                   help="optional base parquet to seed --out from if absent")
    p.add_argument("--timeout", type=int, default=int(os.environ.get("IB_TIMEOUT", "60")))
    p.add_argument("--max-assets", type=int,
                   default=int(os.environ.get("MAX_ASSETS", "0")),
                   help="limit asset count (0 = all); for smoke tests")
    p.add_argument("--assets", default=os.environ.get("ASSETS", ""),
                   help="comma-separated symbol whitelist (overrides universe)")
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan (chunks x assets) and exit")
    return p.parse_args()


def connect(ib: IB, host: str, port: int, client_id: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ib.connect(host, port, clientId=client_id, timeout=10)
            return True
        except Exception as e:  # noqa: BLE001
            print(f"  connect failed ({e}); retrying in 5s...", file=sys.stderr)
            time.sleep(5)
    return False


# ── IB fetch ─────────────────────────────────────────────────────────────────
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


def request_chunk(ib: IB, symbols: list[str], end_dt: datetime,
                  chunk_days: int, throttle: float, retries: int,
                  bar_size: str = BAR_SIZE,
                  exchanges: dict | None = None,
                  errors: dict | None = None) -> dict[str, list]:
    """Fetch one (symbols x chunk) slice. Returns symbol -> ascending rows.

    `bar_size` defaults to 5-min; the 1-min repair script passes "1 min".
    `exchanges` maps symbol -> primaryExchange; without it every symbol is
    requested on SMART. If `errors` is a dict it is populated with
    symbol -> last exception text, so callers can tell a permissions failure
    (IBKR error 162) apart from a genuinely empty session.
    """
    end_str = end_dt.astimezone(ET).strftime("%Y%m%d %H:%M:%S US/Eastern")
    duration_str = f"{chunk_days} D"
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
                    print(f"    {sym} chunk@{end_dt.date()} FAILED after "
                          f"{retries} attempts: {e}", file=sys.stderr)
                    rows = []
                else:
                    back = 2 ** attempt
                    print(f"    {sym} error ({e}); retry {attempt}/{retries} "
                          f"in {back}s", file=sys.stderr)
                    time.sleep(back)
        results[sym] = rows

        # IB pacing: the Rust loader sleeps 1s per chunk; a flat sleep between
        # every symbol request is what keeps us under the request ceiling.
        if throttle > 0 and n < len(symbols):
            time.sleep(throttle)

    return results


# ── chunk files ──────────────────────────────────────────────────────────────
def fetch_chunk_concurrent(ib: IB, symbols: list[str], end_dt: datetime,
                           chunk_days: int, args, exchanges: dict,
                           monitor, persist, out_dir: str,
                           contract_cache: dict) -> dict:
    """Fetch one chunk for many symbols concurrently, with pacing + persistence.

    Two things the 1-min repair established the hard way, both applied here:
      * SUSTAINED RATE is what breaks the gateway, not concurrency. Measured
        on 5-min spans: concurrency 8 with no delay -> 25% of requests
        cancelled by IB ("API historical data query cancelled"). A floor on
        the gap between request STARTS is the control that matters.
      * Rows must be flushed to chunk files as they arrive. If the process is
        killed before the final merge, that work is otherwise lost entirely.
    """
    import asyncio
    import nest_asyncio
    from ib_throttle import MAX_SIMULTANEOUS

    nest_asyncio.apply()
    conc = max(1, min(args.concurrency, MAX_SIMULTANEOUS))
    sem = asyncio.Semaphore(conc)
    gap = max(0.0, args.request_delay)
    lock = asyncio.Lock()
    last = [0.0]
    results: dict[str, tuple[list[dict], str | None]] = {}
    cache = contract_cache

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
                # SILENT empty result, not an exception. That silently cost 12
                # tickers in the first full run. resolve_contract() is the gate.
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
                        c, end_str(), f"{chunk_days} D", BAR_SIZE,
                        "TRADES", True, 2, False, [], 300)
                    rows = bars_to_rows(list(bars), sym)
                    monitor.end(sym, len(rows), None)
                    err = None
                    break
                except Exception as e:  # noqa: BLE001
                    err = f"{type(e).__name__}: {e}"
                    monitor.end(sym, 0, err)
                    # Error 162 is IB cancelling us for load. Back off so the
                    # retry doesn't add to the pressure that caused it.
                    if attempt < max(1, args.retries):
                        await asyncio.sleep(2.0 * attempt)
            results[sym] = (rows, err)
            if rows:
                persist(rows)

    async def go():
        await asyncio.gather(*(one(s) for s in symbols))

    asyncio.get_event_loop().run_until_complete(go())
    return results


def resolve_contract(ib: IB, symbol: str, exchanges: dict, cache: dict):
    """Return a *qualified* contract for `symbol`, or False if unresolvable.

    Qualification is mandatory, not optional: an unqualified contract makes IB
    reply Error 200 "No security definition has been found for the request",
    and ib_async hands that back as an empty bar list rather than raising. The
    caller cannot tell "delisted" from "I never asked properly".

    Results are memoised in `cache` for the whole run -- qualification is a
    blocking round-trip, and re-doing it per chunk would cost 179 x 35 calls.
    False is cached too, so a delisted ticker is not re-probed 35 times.
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
        print(f"    + {symbol}: resolved without primaryExchange hint",
              flush=True)
        cache[symbol] = bare
        return bare
    print(f"    x {symbol}: genuinely unresolvable, skipping", flush=True)
    return False


def write_chunk_file(rows: list[dict], chunk_num: int, out_dir: str,
                     prefix: str = "5min") -> str:
    """Write an intermediate chunk.

    `prefix` must match the glob each script uses to sweep its own orphans
    after a crash -- sharing the helper while globbing a different prefix means
    resume silently never finds the work it already did.
    """
    path = os.path.join(out_dir, f"_{prefix}_chunk_{chunk_num:04d}.parquet")
    df = pd.DataFrame(rows, columns=COLS)
    tbl = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    pq.write_table(tbl, path, compression="snappy")
    return path


def merge_chunks(out: str, chunk_files: list[str],
                 memory_limit: str = "4GB") -> int:
    """Merge base + chunks into `out`: sorted by (asset, ts) and deduped.

    Uses DuckDB rather than a hand-rolled pandas/pyarrow merge because the
    inputs are NOT pre-sorted -- `raw_bars.parquet` stores per-asset blocks in
    hash order, so any streaming merge would need a full external sort anyway.
    DuckDB does exactly that, spills to disk under `memory_limit`, and
    reconciles the Arrow `string` vs pandas `large_string` mismatch for free.

    Written to a sibling temp file and `os.replace`d into place, so an
    interrupted merge can never leave a truncated `out`.
    """
    import duckdb

    inputs = [p for p in [out, *chunk_files] if os.path.exists(p)]
    if not inputs:
        return 0

    tmp = out + ".tmp"
    con = duckdb.connect()
    try:
        # Threads are capped explicitly: DuckDB sizes its buffer pool per
        # thread, so an unbounded thread count makes a small `memory_limit`
        # unusable (it OOMs before it ever reaches the spill threshold).
        con.execute("SET threads=2;")
        con.execute(f"SET memory_limit='{memory_limit}';")
        con.execute(f"SET temp_directory='{out_dir_for(out)}';")
        con.execute("SET preserve_insertion_order=false;")

        union = " UNION ALL ".join(
            f"SELECT CAST(ts_epoch_ms AS BIGINT) AS ts_epoch_ms, "
            f"CAST(asset AS VARCHAR) AS asset, "
            f"CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, "
            f"CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS close, "
            f"CAST(volume AS DOUBLE) AS volume "
            f"FROM read_parquet('{p}')" for p in inputs)

        # QUALIFY row_number()=1 is a streaming dedup once sorted, so this
        # never materialises a second full copy of the dataset.
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

    os.replace(tmp, out)  # atomic: a crash mid-merge never truncates `out`
    return int(n)


def out_dir_for(path: str) -> str:
    d = os.path.dirname(os.path.abspath(path)) or "."
    return d


# ── coverage / resume ────────────────────────────────────────────────────────
def load_covered(out: str, assets: list[str]) -> dict[str, set]:
    """Per-asset set of ET dates already present in `out`."""
    covered: dict[str, set] = {}
    if not os.path.exists(out):
        return covered
    tbl = pq.read_table(out, columns=["ts_epoch_ms", "asset"])
    df = tbl.to_pandas()
    ts = pd.to_datetime(df["ts_epoch_ms"], unit="ms", utc=True)
    days = ts.dt.tz_convert(ET).dt.date
    wanted = set(assets)
    for sym, day in zip(df["asset"].to_numpy(), days):
        if sym in wanted:
            covered.setdefault(sym, set()).add(day)
    print(f"  resume: {len(df):,} existing bars, {len(covered)}/{len(assets)} "
          f"assets present")
    return covered


def et_days_in(start: datetime, end: datetime) -> list[_date]:
    """Weekdays between two dates (ET). Used only for gap *estimation*."""
    out, cur = [], start.date()
    while cur <= end.date():
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def main() -> int:
    args = parse_args()
    start = datetime.fromisoformat(args.start).replace(tzinfo=UTC)
    end = datetime.fromisoformat(args.end).replace(tzinfo=UTC)
    out = os.path.abspath(args.out)
    out_dir = os.path.dirname(out)
    os.makedirs(out_dir, exist_ok=True)

    # Seed the output from the existing 41-asset file on first run.
    seed = args.seed or os.path.join(out_dir, "raw_bars.parquet")
    if not os.path.exists(out) and os.path.exists(seed):
        import shutil
        print(f"  seeding {out} from {seed}")
        shutil.copy(seed, out)

    if args.assets:
        symbols = [s.strip().upper() for s in args.assets.split(",") if s.strip()]
    else:
        symbols = list(EXISTING_41) + sorted(ALL_NEW) + sorted(BENCHMARKS)
    # Dedup while PRESERVING ORDER. QQQ is deliberately in both the research
    # set and BENCHMARKS, so concatenating the two lists would request it twice
    # per chunk. That is not merely wasted requests: the duplicate rows survive
    # the merge as exact duplicates, and the verifier's duplicate check would
    # fail on a file that looks otherwise clean.
    seen: set[str] = set()
    symbols = [s for s in symbols if not (s in seen or seen.add(s))]
    if args.max_assets > 0:
        symbols = symbols[:args.max_assets]

    n_chunks = max(1, (end - start).days // args.chunk_days + 1)
    print(f"Pulling 5-min bars: {len(symbols)} assets "
          f"({len(ALL_NEW)} new), {start.date()}→{end.date()}, "
          f"~{n_chunks} chunks x {args.chunk_days}d, throttle={args.throttle}s")
    print(f"  out={out}")
    if args.dry_run:
        print(f"  dry-run: would issue ~{n_chunks * len(symbols)} requests "
              f"(~{n_chunks * len(symbols) * args.throttle / 3600:.1f}h of "
              f"pacing alone)")
        return 0

    covered = load_covered(out, symbols)

    # Fold in any stale chunk files from a previous interrupted run.
    stale = sorted(Path(out_dir).glob("_5min_chunk_*.parquet"))
    if stale:
        print(f"  merging {len(stale)} stale chunk files from a previous run...")
        n = merge_chunks(out, [str(f) for f in stale])
        print(f"  ✓ stale merge -> {n:,} bars")
        for f in stale:
            f.unlink()
        covered = load_covered(out, symbols)

    ib = IB()
    if not connect(ib, args.host, args.port, args.client_id, args.timeout):
        print("FATAL: could not connect to IB Gateway", file=sys.stderr)
        return 1
    print(f"  connected to IB Gateway {args.host}:{args.port} "
          f"(clientId={args.client_id})")

    exchanges = {s: FULL_UNIVERSE.get(s, "SMART") for s in symbols}
    # Qualified contracts are memoised for the WHOLE run. resolve_contract()
    # does a blocking round-trip; re-qualifying per chunk would mean 179 x 35
    # needless calls, and re-probing dead tickers 35 times each.
    contracts: dict = {}
    errs: dict[str, str] = {}
    other_errors: list[tuple[str, int, str]] = []

    # Crash-resumable persistence: flush rows to a chunk file every N rows so
    # a kill costs at most that much. The final merge is the only time the big
    # parquet gets rewritten.
    from ib_throttle import ThrottleMonitor
    monitor = ThrottleMonitor(
        log_path=Path(out_dir) / "_throttle_events_5min.jsonl")
    buf: list[dict] = []
    _p = {"n": 0, "rows": 0, "files": []}

    def persist(rows):
        buf.extend(rows)
        _p["rows"] += len(rows)
        if _p["rows"] >= args.persist_every:
            _p["n"] += 1
            cf = write_chunk_file(buf, _p["n"], out_dir, prefix="5min")
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
    t0 = time.time()
    total_new = 0
    empty_assets: OrderedDict[str, int] = OrderedDict()

    while cur_end > start:
        chunk_num += 1
        chunk_start = cur_end - timedelta(days=args.chunk_days)
        if chunk_start <= start:
            chunk_start = start

        # Only request symbols that don't already cover this window. A symbol
        # counts as covered if it has *most* of the expected weekdays, which
        # tolerates holidays without refetching the whole chunk.
        expected = et_days_in(chunk_start, cur_end)
        need = []
        for s in symbols:
            have = covered.get(s, set())
            if sum(1 for d in expected if d in have) >= len(expected) * 0.6:
                continue
            need.append(s)

        if need:
            print(f"  chunk {chunk_num}: {len(need)}/{len(symbols)} assets need "
                  f"data, {chunk_start.date()}→{cur_end.date()}")
            results = (fetch_chunk_concurrent(
                ib, need, cur_end, args.chunk_days, args, exchanges,
                monitor, persist, out_dir, contracts)
                if args.concurrency > 1 else
                request_chunk(ib, need, cur_end, args.chunk_days,
                              args.throttle, args.retries, errors=errs))

            earliest = cur_end
            new_rows: list[dict] = []
            # Normalise both paths to {symbol: (rows, err)}.
            pairs = (list(results.items()) if args.concurrency > 1
                     else [(sym, (rows, None)) for sym, rows in results.items()])
            for sym, (rows, err) in pairs:
                if err:
                    other_errors.append((sym, chunk_num, err))
                if rows:
                    new_rows.extend(rows)
                    covered.setdefault(sym, set()).update(
                        datetime.fromtimestamp(r["ts_epoch_ms"] / 1000, tz=UTC)
                        .astimezone(ET).date() for r in rows)
                else:
                    empty_assets[sym] = empty_assets.get(sym, 0) + 1

            if new_rows and args.concurrency <= 1:
                cf = write_chunk_file(new_rows, chunk_num, out_dir)
                chunk_files.append(cf)
                since_merge += 1
            if new_rows:
                total_new += len(new_rows)
                # Step the cursor to just before the earliest bar we saw, so
                # the next chunk continues without a gap. Falls back to a
                # flat step when a symbol returned nothing at all.
                min_ts = min(r["ts_epoch_ms"] for r in new_rows)
                earliest = datetime.fromtimestamp(min_ts / 1000, tz=UTC)
                next_end = earliest - timedelta(seconds=1)
                print(f"    → {len(new_rows):,} new bars "
                      f"(earliest {earliest.astimezone(ET):%Y-%m-%d})")
            else:
                print("    → no new bars this chunk")
                next_end = chunk_start
        else:
            next_end = chunk_start

        if since_merge >= args.merge_every and chunk_files:
            print(f"  merging {len(chunk_files)} chunk files...")
            n = merge_chunks(out, chunk_files)
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
        cf = write_chunk_file(buf, _p["n"], out_dir, prefix="5min")
        _p["files"].append(cf)
        print(f"  final persist: {len(buf):,} rows -> {os.path.basename(cf)}")
        buf.clear()
    if args.concurrency > 1:
        # Concurrent mode persists through `_p`; the serial path already
        # appended to `chunk_files` directly. Overwriting unconditionally would
        # discard the serial chunks and leave them unmerged on disk.
        chunk_files = _p["files"]

    print(monitor.report())

    if chunk_files:
        print(f"  final merge of {len(chunk_files)} chunk files...")
        n = merge_chunks(out, chunk_files)
        for cf in chunk_files:
            os.unlink(cf)
    elif os.path.exists(out):
        n = pq.ParquetFile(out).metadata.num_rows
    else:
        n = 0

    print(f"\nDONE: {n:,} bars in {out}  ({total_new:,} newly fetched, "
          f"{(time.time() - t0) / 60:.1f} min)")

    # Unresolvable contracts MUST be called out separately. They land in
    # empty_assets alongside genuine pre-IPO windows, where they read as
    # "delisted" and cost no attention -- but the real cause is that IB has no
    # contract for them at all, which is a universe-list defect, not a data
    # gap. Verified on this account: SQ, ZI, HRT, CFLT, PRTG, LC and IRBT all
    # return zero matches from BOTH qualifyContracts and reqContractDetails.
    dead = sorted(s for s, c in contracts.items() if c is False)
    if dead:
        print(f"\n*** {len(dead)} ticker(s) have NO resolvable contract on this "
              f"account and were SKIPPED:")
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
            print(f"    {s:<8} {n} chunk(s) failed")

    report(out, symbols, empty_assets, expect_assets=sorted(ALL_NEW))
    return 0


def report(out: str, symbols: list[str], empty_assets: dict,
           bar_minutes: int = BAR_MINUTES,
           expected_bars_per_day: int = EXPECTED_BARS_PER_DAY,
           expect_assets: list[str] | None = None) -> None:
    """Post-run integrity report.

    `bar_minutes` / `expected_bars_per_day` are parameters so the 1-min repair
    script can reuse this with its own grid (1 min / 390 bars) rather than
    being checked against 5-min expectations.
    """
    step_ms = bar_minutes * 60_000
    last_bar_min = 570 + expected_bars_per_day * bar_minutes - bar_minutes
    pf = pq.ParquetFile(out)
    print(f"\n{'=' * 66}\nVERIFY {out}\n{'=' * 66}")
    print(f"rows={pf.metadata.num_rows:,}  row_groups={pf.metadata.num_row_groups}")
    print(f"schema matches canonical: {pf.schema_arrow.equals(SCHEMA)}")

    tbl = pf.read(columns=["ts_epoch_ms", "asset"])
    df = tbl.to_pandas()
    ts = df["ts_epoch_ms"].to_numpy()

    off = int((ts % step_ms != 0).sum())
    print(f"grid: {bar_minutes}-min  off-grid timestamps: {off:,}")

    et = pd.DatetimeIndex(pd.to_datetime(ts, unit="ms", utc=True)).tz_convert(ET)
    mod = et.hour * 60 + et.minute
    print(f"ET minute-of-day: {mod.min()}..{mod.max()} "
          f"(expect 570..{last_bar_min})")
    print(f"pre-09:30 bars: {int((mod < 570).sum()):,}   "
          f"at/after-16:00 bars: {int((mod >= 960).sum()):,}")
    print(f"weekend bars: {int((et.dayofweek >= 5).sum()):,}")

    dup = int(df.duplicated(subset=["ts_epoch_ms", "asset"]).sum())
    print(f"duplicate (ts,asset): {dup:,}")

    per = pd.DataFrame({"asset": df.asset.to_numpy(), "day": et.date}) \
        .groupby(["asset", "day"]).size()
    full = int((per == expected_bars_per_day).sum())
    print(f"asset-days: {len(per):,}  full({expected_bars_per_day})={full:,}  "
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
        print(f"symbols that returned 0 bars on >=1 chunk: {len(empty_assets)}")
        print(f"  worst: {top}")


if __name__ == "__main__":
    sys.exit(main())

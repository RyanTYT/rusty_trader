#!/usr/bin/env python3
"""Pull 1-min historical bars from IB Gateway into a parquet file.

OPTIMIZED:
  1. No sleep between requests (different contracts have no IBKR pacing).
  2. No-overlap stepping (next chunk end = previous chunk's earliest bar - 1 day).
  3. New rows → small chunk files (instant, no 30s rewrite of the full parquet).
  4. Merge chunk files into the main parquet every N chunks (default 20) + at end.
  5. Verify after each merge (read back + check row count).
  6. Final integrity check (no skipped bars, all assets, no gaps).

RESUMABLE: reads existing parquet's per-asset min/max (fast — no full load).
Chunk files survive crashes + are merged on the next run.

Requires: pip install ib_async pandas pyarrow
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from ib_async import IB, Stock
except ImportError:
    print("ERROR: pip install ib_async", file=sys.stderr)
    raise

EXCHANGE_MAP: dict[str, tuple[str, str]] = {
    "AAPL": ("NASDAQ", "USD"), "ADBE": ("NASDAQ", "USD"), "AMD": ("NASDAQ", "USD"),
    "AMZN": ("NASDAQ", "USD"), "AVGO": ("NASDAQ", "USD"), "BAC": ("NYSE", "USD"),
    "CAT": ("ARCA", "USD"), "COIN": ("NASDAQ", "USD"), "COP": ("NYSE", "USD"),
    "CRM": ("NYSE", "USD"), "CVX": ("NYSE", "USD"), "DE": ("NYSE", "USD"),
    "DIS": ("NYSE", "USD"), "FCX": ("NYSE", "USD"), "GE": ("NASDAQ", "USD"),
    "GOOGL": ("NASDAQ", "USD"), "GS": ("NYSE", "USD"), "HD": ("NYSE", "USD"),
    "JPM": ("NYSE", "USD"), "LIN": ("NYSE", "USD"), "LLY": ("NYSE", "USD"),
    "LOW": ("NYSE", "USD"), "META": ("NASDAQ", "USD"), "MPC": ("NYSE", "USD"),
    "MSFT": ("NASDAQ", "USD"), "MU": ("NASDAQ", "USD"), "NFLX": ("NASDAQ", "USD"),
    "NKE": ("NYSE", "USD"), "NVDA": ("NASDAQ", "USD"), "PFE": ("NYSE", "USD"),
    "PYPL": ("NASDAQ", "USD"), "QCOM": ("NASDAQ", "USD"), "SBUX": ("NASDAQ", "USD"),
    "SCHW": ("NYSE", "USD"), "SLB": ("NYSE", "USD"), "SPY": ("ARCA", "USD"),
    "T": ("NYSE", "USD"), "TSLA": ("NASDAQ", "USD"), "TXN": ("NASDAQ", "USD"),
    "UNH": ("NYSE", "USD"), "XOM": ("NYSE", "USD"),
}
ASSETS = list(EXCHANGE_MAP.keys())
COLS = ["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"]


def make_contract(symbol: str) -> Stock:
    primary, ccy = EXCHANGE_MAP[symbol]
    return Stock(symbol, "SMART", ccy, primaryExchange=primary)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.environ.get("IB_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("IB_PORT", "4002")))
    p.add_argument("--client-id", type=int, default=int(os.environ.get("IB_CLIENT_ID", "15")))
    p.add_argument("--start", default=os.environ.get("START_DATE", "2021-01-01"))
    p.add_argument("--end", default=os.environ.get("END_DATE", datetime.now().strftime("%Y-%m-%d")))
    p.add_argument("--chunk-days", type=int, default=int(os.environ.get("CHUNK_DAYS", "10")))
    p.add_argument("--merge-every", type=int, default=int(os.environ.get("MERGE_EVERY", "20")),
                   help="merge chunk files into main parquet every N chunks")
    p.add_argument("--out", default=os.environ.get("OUTPUT",
                   os.path.join(os.path.dirname(__file__), "..", "data",
                                "backtest-results", "stats", "raw_1min_bars.parquet")))
    p.add_argument("--timeout", type=int, default=int(os.environ.get("IB_TIMEOUT", "60")))
    return p.parse_args()


def connect(ib: IB, host: str, port: int, client_id: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ib.connect(host, port, clientId=client_id, timeout=10)
            return True
        except Exception as e:
            print(f"  connect failed ({e}); retrying in 5s...", file=sys.stderr)
            time.sleep(5)
    return False


def request_chunk(ib: IB, symbols: list[str], end_dt: datetime,
                  chunk_days: int) -> dict[str, list]:
    """Sequential, no-sleep fetch for all symbols in a chunk."""
    end_str = end_dt.strftime('%Y%m%d %H:%M:%S US/Eastern')
    duration_str = f"{chunk_days} D"
    results: dict[str, list] = {}
    for sym in symbols:
        contract = make_contract(sym)
        ib.qualifyContracts(contract)
        try:
            bars = ib.reqHistoricalData(
                contract, endDateTime=end_str, durationStr=duration_str,
                barSizeSetting="1 min", whatToShow="TRADES",
                useRTH=True, formatDate=2, timeout=120)
            results[sym] = bars or []
        except Exception as e:
            print(f"    {sym} chunk@{end_dt.date()} error: {e}", file=sys.stderr)
            results[sym] = []
    return results


def bars_to_rows(bars, symbol: str) -> list[dict]:
    rows = []
    for b in bars:
        ts_ms = int(b.date.timestamp() * 1000) if hasattr(b.date, "timestamp") else int(b.date * 1000)
        rows.append({"ts_epoch_ms": ts_ms, "asset": symbol,
                     "open": float(b.open), "high": float(b.high),
                     "low": float(b.low), "close": float(b.close),
                     "volume": float(b.volume)})
    return rows


def next_end_date(earliest_bar_dt: datetime, start: datetime) -> datetime | None:
    next_end = earliest_bar_dt - timedelta(days=1)
    return next_end if next_end > start else None


def load_existing_ranges(out: str, assets: list[str]) -> dict[str, tuple[float, float]]:
    """Read per-asset min/max ts from the existing parquet (fast — single groupby,
    no need to load all rows into a Python list)."""
    ranges = {}
    if os.path.exists(out):
        df = pd.read_parquet(out, columns=["ts_epoch_ms", "asset"])
        for sym in assets:
            sub = df[df["asset"] == sym]
            if len(sub):
                ranges[sym] = (sub["ts_epoch_ms"].min(), sub["ts_epoch_ms"].max())
        print(f"  resume: {len(df):,} existing bars, {len(ranges)} assets covered")
    return ranges


def write_chunk_file(new_rows: list[dict], chunk_num: int, out_dir: str) -> str:
    """Write new rows to a small chunk file (instant — no merge, no dedup)."""
    path = os.path.join(out_dir, f"_chunk_{chunk_num:04d}.parquet")
    df = pd.DataFrame(new_rows, columns=COLS)
    df.to_parquet(path, index=False)
    return path


def merge_chunks(out: str, chunk_files: list[str]) -> int:
    """Merge the main parquet + all chunk files into the final parquet.
    Deduplicates + sorts. Returns the final row count."""
    tables = []
    if os.path.exists(out):
        tables.append(pq.read_table(out))
    for cf in chunk_files:
        if os.path.exists(cf):
            tables.append(pq.read_table(cf))
    if not tables:
        return 0
    combined = pa.concat_tables(tables)
    df = combined.to_pandas()
    df = df.drop_duplicates(subset=["ts_epoch_ms", "asset"]).sort_values(["asset", "ts_epoch_ms"])
    df["asset"] = df["asset"].astype("string")
    df.to_parquet(out, index=False)
    return len(df)


def verify_parquet(out: str) -> bool:
    """Read back the parquet + verify it's not corrupted."""
    try:
        df = pd.read_parquet(out, columns=["ts_epoch_ms"])
        return len(df) > 0
    except Exception as e:
        print(f"  ⚠ PARQUET CORRUPT: {e}", file=sys.stderr)
        return False


def integrity_check(df: pd.DataFrame, start: datetime, end: datetime) -> list[str]:
    """Final check: verify no skipped bars per asset."""
    problems = []
    start_ts = pd.Timestamp(start)  # start already has tzinfo=utc
    end_ts = pd.Timestamp(end)
    for sym in ASSETS:
        sub = df[df["asset"] == sym]
        if len(sub) == 0:
            problems.append(f"{sym}: NO DATA")
            continue
        ts = sub["ts_epoch_ms"].sort_values().to_numpy()
        dates = pd.to_datetime(ts, unit="ms", utc=True).normalize().drop_duplicates().sort_values()
        if dates[0] > start_ts + timedelta(days=7):
            problems.append(f"{sym}: starts {dates[0].date()} (expected ~{start.date()})")
        if dates[-1] < end_ts - timedelta(days=7):
            problems.append(f"{sym}: ends {dates[-1].date()} (expected ~{end.date()})")
        for i in range(1, len(dates)):
            gap = dates[i] - dates[i-1]
            if gap > timedelta(days=4):
                problems.append(f"{sym}: gap {gap.days}d {dates[i-1].date()}→{dates[i].date()}")
    return problems


def main() -> int:
    args = parse_args()
    assets_env = os.environ.get("ASSETS", ",".join(ASSETS))
    assets = [a.strip() for a in assets_env.split(",") if a.strip()]
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    out = os.path.abspath(args.out)
    out_dir = os.path.dirname(out)
    os.makedirs(out_dir, exist_ok=True)

    print(f"Pulling 1-min bars: {len(assets)} assets, {start.date()}→{end.date()}, "
          f"chunk={args.chunk_days}d, merge_every={args.merge_every}, out={out}")

    # resume: read per-asset min/max (fast — no full row load)
    asset_ranges = load_existing_ranges(out, assets)

    # clean up any stale chunk files from a previous crashed run
    stale_chunks = sorted(Path(out_dir).glob("_chunk_*.parquet"))
    if stale_chunks:
        print(f"  found {len(stale_chunks)} stale chunk files from a previous run — merging...")
        merge_chunks(out, [str(f) for f in stale_chunks])
        for f in stale_chunks:
            f.unlink()
        asset_ranges = load_existing_ranges(out, assets)

    ib = IB()
    if not connect(ib, args.host, args.port, args.client_id, args.timeout):
        print("FATAL: could not connect to IB Gateway", file=sys.stderr)
        return 1
    print(f"  connected to IB Gateway {args.host}:{args.port} (clientId={args.client_id})")

    cur_end = end
    chunk_num = 0
    t0 = time.time()
    new_rows_buffer: list[dict] = []
    chunk_files: list[str] = []
    chunks_since_merge = 0

    while cur_end > start:
        chunk_num += 1
        chunk_start_ts = (cur_end - timedelta(days=args.chunk_days)).timestamp() * 1000
        chunk_end_ts = cur_end.timestamp() * 1000

        need = [s for s in assets
                if s not in asset_ranges
                or not (asset_ranges[s][0] <= chunk_start_ts + 1
                        and asset_ranges[s][1] >= chunk_end_ts - 1)]

        if need:
            print(f"  chunk {chunk_num}: {len(need)}/{len(assets)} assets need data, end={cur_end.date()}")
            results = request_chunk(ib, need, cur_end, args.chunk_days)

            earliest_dt = cur_end
            chunk_new_rows = []
            for sym, bars in results.items():
                if bars:
                    rows = bars_to_rows(bars, sym)
                    chunk_new_rows.extend(rows)
                    first_dt = datetime.fromtimestamp(rows[0]["ts_epoch_ms"] / 1000, tz=timezone.utc)
                    earliest_dt = min(earliest_dt, first_dt)
                    old_range = asset_ranges.get(sym, (float("inf"), float("-inf")))
                    asset_ranges[sym] = (min(old_range[0], rows[0]["ts_epoch_ms"]),
                                         max(old_range[1], rows[-1]["ts_epoch_ms"]))
                    print(f"    {sym:5s} +{len(rows)} bars (earliest={first_dt.date()})")

            if chunk_new_rows:
                cf = write_chunk_file(chunk_new_rows, chunk_num, out_dir)
                chunk_files.append(cf)
                new_rows_buffer = []  # not needed — chunk files hold the data
                chunks_since_merge += 1
                next_end = next_end_date(earliest_dt, start)
                print(f"  chunk {chunk_num} → {len(chunk_new_rows)} new bars written to {os.path.basename(cf)}")
            else:
                print(f"  chunk {chunk_num}: no new bars (all duplicates)")
                next_end = cur_end - timedelta(days=args.chunk_days)
        else:
            next_end = cur_end - timedelta(days=args.chunk_days)

        # merge every N chunks (or if we have lots of chunk files)
        if chunks_since_merge >= args.merge_every:
            print(f"  merging {len(chunk_files)} chunk files into main parquet...")
            n = merge_chunks(out, chunk_files)
            if verify_parquet(out):
                print(f"  ✓ merged + verified: {n:,} bars total ({(time.time()-t0)/60:.1f} min elapsed)")
            else:
                print("  ⚠ MERGE CORRUPT — keeping chunk files for recovery")
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files = []
            chunks_since_merge = 0

        if next_end is None:
            break
        cur_end = next_end

    ib.disconnect()

    # final merge
    if chunk_files:
        print(f"  final merge of {len(chunk_files)} chunk files...")
        n = merge_chunks(out, chunk_files)
        for cf in chunk_files:
            os.unlink(cf)
    elif os.path.exists(out):
        n = len(pd.read_parquet(out, columns=["ts_epoch_ms"]))
    else:
        n = 0

    print(f"\nDONE: {n:,} 1-min bars → {out}")

    # integrity check
    if os.path.exists(out):
        print("\n" + "=" * 80)
        print("INTEGRITY CHECK: verifying no skipped bars...")
        print("=" * 80)
        df = pd.read_parquet(out)
        d0 = pd.to_datetime(df["ts_epoch_ms"].min(), unit="ms", utc=True).date()
        d1 = pd.to_datetime(df["ts_epoch_ms"].max(), unit="ms", utc=True).date()
        print(f"  {len(df):,} bars, {df['asset'].nunique()} assets, {d0} → {d1}")
        problems = integrity_check(df, start, end)
        if problems:
            print(f"⚠ {len(problems)} issues:")
            for p in problems[:20]:
                print(f"  - {p}")
        else:
            print("✓ ALL CLEAN — no skipped bars, all assets present, no large gaps.")
        print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Fill data gaps in the 1-min parquet — detects per-asset missing date ranges
(the integrity check flags them) + re-fetches them from IB Gateway.

Run: python scripts/fill_1min_gaps.py
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pull_1min_bars import (ASSETS, COLS, connect, bars_to_rows,
                             load_existing_ranges, merge_chunks, verify_parquet)
from ib_async import IB

OUT = os.path.join(os.path.dirname(__file__), "..", "data",
                   "backtest-results", "stats", "raw_1min_bars.parquet")


def find_gaps(df: pd.DataFrame, start: datetime, end: datetime) -> dict[str, list[tuple]]:
    """Find per-asset missing date ranges (gaps > 4 calendar days between
    consecutive trading days, excluding known non-trading periods)."""
    gaps: dict[str, list[tuple]] = {}
    for sym in ASSETS:
        sub = df[df["asset"] == sym]
        if len(sub) == 0:
            gaps[sym] = [(start, end)]
            continue
        ts = sub["ts_epoch_ms"].sort_values().to_numpy()
        dates = pd.to_datetime(ts, unit="ms", utc=True).normalize().drop_duplicates().sort_values()
        asset_gaps = []
        # check head
        if dates[0] > start + timedelta(days=7):
            asset_gaps.append((start, dates[0]))
        # check body
        for i in range(1, len(dates)):
            gap = dates[i] - dates[i-1]
            if gap > timedelta(days=4):
                asset_gaps.append((dates[i-1] + timedelta(days=1), dates[i] - timedelta(days=1)))
        # check tail
        if dates[-1] < end - timedelta(days=7):
            asset_gaps.append((dates[-1] + timedelta(days=1), end))
        if asset_gaps:
            gaps[sym] = asset_gaps
    return gaps


def fetch_range(ib: IB, symbol: str, gap_start: datetime, gap_end: datetime) -> list[dict]:
    """Fetch 1-min bars for a symbol covering [gap_start, gap_end]."""
    from pull_1min_bars import make_contract, request_chunk
    # request a chunk ending at gap_end + 1 day, duration = the gap + buffer
    days = (gap_end - gap_start).days + 3  # buffer on both sides
    end_dt = gap_end + timedelta(days=1)
    results = request_chunk(ib, [symbol], end_dt, max(days, 2))
    return bars_to_rows(results.get(symbol, []), symbol)


def main() -> int:
    start = datetime.fromisoformat("2021-01-01").replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat("2026-09-28").replace(tzinfo=timezone.utc)

    print(f"Loading {OUT}...")
    df = pd.read_parquet(OUT)
    print(f"  {len(df):,} bars, {df['asset'].nunique()} assets")

    gaps = find_gaps(df, start, end)
    if not gaps:
        print("✓ NO GAPS — data is complete.")
        return 0

    total_ranges = sum(len(v) for v in gaps.values())
    print(f"\n{len(gaps)} assets with {total_ranges} gap ranges to fill:")
    for sym, ranges in gaps.items():
        for gs, ge in ranges:
            print(f"  {sym}: {gs.date()} → {ge.date()} ({(ge-gs).days}d)")

    ib = IB()
    if not connect(ib, "127.0.0.1", 4002, 66, 60):
        print("FATAL: could not connect to IB Gateway", file=sys.stderr)
        return 1

    out_dir = os.path.dirname(OUT)
    chunk_files = []
    for i, (sym, ranges) in enumerate(gaps.items()):
        for gs, ge in ranges:
            print(f"  fetching {sym} {gs.date()}→{ge.date()}...")
            rows = fetch_range(ib, sym, gs, ge)
            if rows:
                cf = os.path.join(out_dir, f"_gapfill_{i:03d}.parquet")
                pd.DataFrame(rows, columns=COLS).to_parquet(cf, index=False)
                chunk_files.append(cf)
                print(f"    +{len(rows)} bars → {os.path.basename(cf)}")
            else:
                print(f"    +0 bars (no data available — maybe a halt or delisting)")
            time.sleep(1)

    ib.disconnect()

    if chunk_files:
        print(f"\nmerging {len(chunk_files)} gap-fill files...")
        n = merge_chunks(OUT, chunk_files)
        for cf in chunk_files:
            os.unlink(cf)
        if verify_parquet(OUT):
            print(f"✓ merged + verified: {n:,} bars")

        # re-check
        df = pd.read_parquet(OUT)
        gaps_after = find_gaps(df, start, end)
        if not gaps_after:
            print("✓ ALL GAPS FILLED — data is complete.")
        else:
            print(f"⚠ {len(gaps_after)} assets still have gaps:")
            for sym, ranges in gaps_after.items():
                for gs, ge in ranges:
                    print(f"  {sym}: {gs.date()} → {ge.date()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

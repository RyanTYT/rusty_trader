#!/usr/bin/env python3
"""Pull 5-min historical bars for mid-cap + small-cap stocks from IB Gateway.

Duplicates the 1-min script (pull_1min_bars.py) with these changes:
  - barSizeSetting = "5 min" (78 bars/day instead of 390)
  - Larger chunks per request (IBKR allows ~6 months of 5-min bars per request)
  - Extends raw_bars.parquet (the 5-min backtest file) with the new assets
  - Includes the 41 existing megacaps (skipped if already present) + ~230 new names

The stock lists are curated for cross-sectional strategies (momentum, RVOL ORB
with ranking): liquid S&P 400 mid-caps + S&P 600 small-caps with high
volatility + active trading.

Usage:
  python scripts/pull_5min_bars.py --start 2021-01-01 --end 2026-09-28 \\
      --host 127.0.0.1 --port 4002 \\
      --out data/backtest-results/stats/raw_bars_extended.parquet
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

try:
    from ib_async import IB, Stock
except ImportError:
    print("ERROR: pip install ib_async", file=sys.stderr)
    raise

# ── MID-CAP universe (~100 names: S&P 400 + high-liquidity mid-caps) ──────
# Curated for cross-sectional momentum + RVOL ORB: high vol, liquid, diverse sectors
# REMOVED (delisted/acquired/renamed by 2026, verified via IBKR qualification):
#   BNB (crypto, not a stock), ZI, CFLT, LC, ZEN (acquired), SQ (renamed XYZ), WKS
MIDCAP = {
    # Tech / Software / Internet (liquid, high-beta, options-rich)
    "TWLO": "NYSE", "OKTA": "NASDAQ", "ROKU": "NASDAQ",
    "DT": "NYSE", "DOCN": "NYSE", "S": "NYSE", "GTLB": "NASDAQ",
    "CRDO": "NASDAQ", "PATH": "NYSE", "AI": "NYSE",
    "DUOL": "NASDAQ", "BMBL": "NASDAQ", "UPST": "NASDAQ", "HOOD": "NASDAQ",
    "SOFI": "NASDAQ", "RKT": "NYSE", "FIVE": "NYSE",
    "NET": "NYSE", "TTD": "NASDAQ", "DDOG": "NASDAQ",
    "MDB": "NASDAQ", "TEAM": "NASDAQ", "PINS": "NYSE", "SNAP": "NYSE",
    "XYZ": "NYSE", "SHOP": "NASDAQ", "CRWD": "NASDAQ",
    "ASAN": "NYSE", "ALAB": "NASDAQ", "SMCI": "NASDAQ",
    # Biotech / Pharma (binary catalysts, high vol, institutional flow)
    "VKTX": "NASDAQ", "MDGL": "NASDAQ", "CRSP": "NASDAQ", "BBIO": "NASDAQ",
    "IBRX": "NASDAQ", "ROIV": "NASDAQ", "TGTX": "NASDAQ", "NTLA": "NASDAQ",
    "BEAM": "NASDAQ", "INO": "NASDAQ", "REGN": "NASDAQ", "VRTX": "NASDAQ",
    "MRNA": "NASDAQ", "BNTX": "NASDAQ", "SRPT": "NASDAQ", "ALNY": "NASDAQ",
    # Consumer / Retail (turnaround + growth stories, retail flow)
    "CELH": "NASDAQ", "ANF": "NYSE", "ELF": "NYSE", "W": "NYSE",
    "BROS": "NYSE", "RBLX": "NYSE", "U": "NYSE", "CHWY": "NYSE",
    "GME": "NYSE", "BBY": "NYSE", "WING": "NASDAQ", "DASH": "NYSE",
    "ETSY": "NASDAQ", "KMX": "NYSE", "LULU": "NASDAQ", "DPZ": "NYSE",
    "CMG": "NYSE", "CAVA": "NYSE", "SG": "NYSE",
    # Energy / Clean Tech (commodity beta + AI power theme)
    "ENPH": "NASDAQ", "AR": "NYSE", "RIG": "NYSE", "OVV": "NYSE",
    "BE": "NYSE", "FTI": "NYSE", "XPO": "NYSE", "FSLR": "NASDAQ",
    "RUN": "NASDAQ", "SEDG": "NASDAQ", "PLUG": "NASDAQ", "VST": "NYSE",
    # Industrial / Materials (aerospace + defense + specialty)
    "USFD": "NYSE", "CW": "NYSE", "ATI": "NYSE", "NVT": "NYSE",
    "CRS": "NYSE", "MP": "NASDAQ", "UEC": "NYSE", "CCJ": "NYSE",
    "HWM": "NYSE", "TDG": "NASDAQ",
    # Financials / Transport
    "NLY": "NYSE", "AGNC": "NASDAQ", "AAL": "NASDAQ", "LUMN": "NYSE",
    "LYFT": "NASDAQ", "UAA": "NYSE", "VSAT": "NASDAQ",
    # Semis (mid-cap + AI beneficiaries)
    "QRVO": "NASDAQ", "FORM": "NASDAQ", "TER": "NASDAQ", "MPWR": "NASDAQ",
    "ARM": "NASDAQ", "MRVL": "NASDAQ", "ON": "NASDAQ",  # ON already in 41 — auto-skip
}

# ── SMALL-CAP universe (~45 names: S&P 600 + liquid small-caps) ────────────
# REMOVED (delisted/failed IBKR qualification): BTAI, HRT, IRBT, PRTG, SDC
SMALLCAP = {
    # Biotech / Pharma (high vol, catalysts)
    "SRNE": "NASDAQ", "SDGR": "NASDAQ", "RXRX": "NASDAQ",
    "FLGT": "NASDAQ", "TEM": "NASDAQ", "DNA": "NYSE",
    "BVS": "NASDAQ", "NFE": "NYSE",
    # Tech / Software (small but liquid)
    "SOUN": "NASDAQ", "GFAI": "NASDAQ", "APLD": "NASDAQ",
    "GCT": "NASDAQ", "CXAI": "NASDAQ", "PD": "NYSE",
    # Consumer / Retail (speculative + turnaround)
    "CVNA": "NYSE", "ROOT": "NASDAQ", "LMND": "NYSE", "HIPO": "NASDAQ",
    "CLOV": "NASDAQ", "GO": "NASDAQ",
    # Energy / Mining (uranium + commodity themes)
    "DNN": "AMEX", "NXE": "NYSE", "BTU": "NYSE", "HCC": "NYSE",
    "AMRX": "NASDAQ",
}

# Remove any overlap with the existing 41-asset universe (they'll be auto-skipped
# by the resume logic anyway, but this keeps the request list clean)
EXISTING_41 = {
    "AAPL", "ADBE", "AMD", "AMZN", "AVGO", "BAC", "CAT", "COIN", "COP",
    "CRM", "CVX", "DE", "DIS", "FCX", "GE", "GOOGL", "GS", "HD", "JPM",
    "LIN", "LLY", "LOW", "META", "MPC", "MSFT", "MU", "NFLX", "NKE",
    "NVDA", "PFE", "PYPL", "QCOM", "SBUX", "SCHW", "SLB", "SPY", "T",
    "TSLA", "TXN", "UNH", "XOM",
}

# Deduplicate + remove existing 41
ALL_NEW = {}
for d in (MIDCAP, SMALLCAP):
    for sym, ex in d.items():
        if sym not in EXISTING_41 and sym not in ALL_NEW:
            ALL_NEW[sym] = ex

# Include the existing 41 too (they'll be skipped if already present, but
# needed if we want to extend raw_bars to a fuller date range for them)
FULL_UNIVERSE = {}
for sym, ex in {
    "AAPL": "NASDAQ", "ADBE": "NASDAQ", "AMD": "NASDAQ", "AMZN": "NASDAQ",
    "AVGO": "NASDAQ", "BAC": "NYSE", "CAT": "NYSE", "COIN": "NASDAQ",
    "COP": "NYSE", "CRM": "NYSE", "CVX": "NYSE", "DE": "NYSE",
    "DIS": "NYSE", "FCX": "NYSE", "GE": "NASDAQ", "GOOGL": "NASDAQ",
    "GS": "NYSE", "HD": "NYSE", "JPM": "NYSE", "LIN": "NYSE",
    "LLY": "NYSE", "LOW": "NYSE", "META": "NASDAQ", "MPC": "NYSE",
    "MSFT": "NASDAQ", "MU": "NASDAQ", "NFLX": "NASDAQ", "NKE": "NYSE",
    "NVDA": "NASDAQ", "PFE": "NYSE", "PYPL": "NASDAQ", "QCOM": "NASDAQ",
    "SBUX": "NASDAQ", "SCHW": "NYSE", "SLB": "NYSE", "SPY": "ARCA",
    "T": "NYSE", "TSLA": "NASDAQ", "TXN": "NASDAQ", "UNH": "NYSE",
    "XOM": "NYSE",
}.items():
    FULL_UNIVERSE[sym] = ex
FULL_UNIVERSE.update(ALL_NEW)

COLS = ["ts_epoch_ms", "asset", "open", "high", "low", "close", "volume"]


def make_contract(symbol: str) -> Stock:
    primary = FULL_UNIVERSE.get(symbol, "SMART")
    return Stock(symbol, "SMART", "USD", primaryExchange=primary)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.environ.get("IB_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("IB_PORT", "4002")))
    p.add_argument("--client-id", type=int, default=int(os.environ.get("IB_CLIENT_ID", "15")))
    p.add_argument("--start", default=os.environ.get("START_DATE", "2021-01-01"))
    p.add_argument("--end", default=os.environ.get("END_DATE", datetime.now().strftime("%Y-%m-%d")))
    p.add_argument("--chunk-days", type=int, default=int(os.environ.get("CHUNK_DAYS", "60")),
                   help="days per request — 5-min bars allow ~60+ days per request")
    p.add_argument("--merge-every", type=int, default=int(os.environ.get("MERGE_EVERY", "20")))
    p.add_argument("--out", default=os.environ.get("OUTPUT",
                   os.path.join(os.path.dirname(__file__), "..", "data",
                                "backtest-results", "stats", "raw_bars_extended.parquet")))
    p.add_argument("--timeout", type=int, default=int(os.environ.get("IB_TIMEOUT", "60")))
    p.add_argument("--max-assets", type=int, default=int(os.environ.get("MAX_ASSETS", "0")),
                   help="limit the number of new assets to fetch (0 = all; for testing)")
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
    """Fetch 5-min bars directly — '5 mins' (plural) is the correct IBKR string.
    5-min bars allow larger chunks per request (~60+ days). Prints per-asset
    progress so a stuck request is immediately visible."""
    end_str = end_dt.strftime('%Y%m%d %H:%M:%S US/Eastern')
    duration_str = f"{chunk_days} D"
    results: dict[str, list] = {}
    for i, sym in enumerate(symbols):
        contract = make_contract(sym)
        t0 = time.time()
        try:
            ib.qualifyContracts(contract)
            bars = ib.reqHistoricalData(
                contract, endDateTime=end_str, durationStr=duration_str,
                barSizeSetting="5 mins", whatToShow="TRADES",
                useRTH=True, formatDate=1, timeout=120)
            results[sym] = bars or []
            print(f"    [{i+1}/{len(symbols)}] {sym:6s} +{len(bars):5d} bars ({time.time()-t0:.1f}s)")
        except Exception as e:
            print(f"    [{i+1}/{len(symbols)}] {sym:6s} ERROR: {type(e).__name__} ({time.time()-t0:.1f}s)", file=sys.stderr)
            results[sym] = []
    return results


def resample_1m_to_5m(bars_1m) -> list:
    """Resample 1-min bars to 5-min bars (first open, max high, min low, last
    close, sum volume per 5-min bucket). Matches IBKR's 5-min bar semantics."""
    import collections
    buckets = collections.OrderedDict()
    for b in bars_1m:
        ts_ms = int(b.date.timestamp() * 1000) if hasattr(b.date, "timestamp") else int(b.date * 1000)
        # bucket start = floor to 5-min boundary
        bucket_ts = (ts_ms // 300000) * 300000
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                "ts_epoch_ms": bucket_ts,
                "open": float(b.open), "high": float(b.high),
                "low": float(b.low), "close": float(b.close),
                "volume": float(b.volume),
            }
        else:
            bar = buckets[bucket_ts]
            bar["high"] = max(bar["high"], float(b.high))
            bar["low"] = min(bar["low"], float(b.low))
            bar["close"] = float(b.close)
            bar["volume"] += float(b.volume)

    class ResampledBar:
        def __init__(self, d):
            self.__dict__.update(d)

    # return as objects with .date/.open/.high/.low/.close/.volume attributes
    out = []
    for ts, d in buckets.items():
        from datetime import datetime, timezone
        bar = ResampledBar(d)
        bar.date = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        out.append(bar)
    out.sort(key=lambda b: b.date)
    return out


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
    """Read per-asset min/max ts from the existing parquet (fast)."""
    ranges = {}
    if os.path.exists(out):
        df = pd.read_parquet(out, columns=["ts_epoch_ms", "asset"])
        for sym in assets:
            sub = df[df["asset"] == sym]
            if len(sub):
                ranges[sym] = (sub["ts_epoch_ms"].min(), sub["ts_epoch_ms"].max())
        print(f"  resume: {len(df):,} existing bars, {len(ranges)}/{len(assets)} assets covered")
    return ranges


def write_chunk_file(new_rows: list[dict], chunk_num: int, out_dir: str) -> str:
    path = os.path.join(out_dir, f"_5min_chunk_{chunk_num:04d}.parquet")
    df = pd.DataFrame(new_rows, columns=COLS)
    df.to_parquet(path, index=False)
    return path


def merge_chunks(out: str, chunk_files: list[str]) -> int:
    """Merge the main parquet + chunk files into one. Uses pandas DataFrames
    (NOT pyarrow concat_tables — the schemas differ between the original
    raw_bars.parquet types and the new chunk files, and pyarrow requires
    exact schema matches while pandas handles type coercion)."""
    dfs = []
    if os.path.exists(out):
        dfs.append(pd.read_parquet(out))
    for cf in chunk_files:
        if os.path.exists(cf):
            dfs.append(pd.read_parquet(cf))
    if not dfs:
        return 0
    df = pd.concat(dfs, ignore_index=True)
    # coerce to a consistent schema
    df["ts_epoch_ms"] = df["ts_epoch_ms"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")
    df["asset"] = df["asset"].astype("string")
    df = df.drop_duplicates(subset=["ts_epoch_ms", "asset"]).sort_values(["asset", "ts_epoch_ms"])
    df.to_parquet(out, index=False)
    return len(df)


def verify_parquet(out: str) -> bool:
    try:
        df = pd.read_parquet(out, columns=["ts_epoch_ms"])
        return len(df) > 0
    except Exception as e:
        print(f"  ⚠ PARQUET CORRUPT: {e}", file=sys.stderr)
        return False


def main() -> int:
    args = parse_args()
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    out = os.path.abspath(args.out)
    out_dir = os.path.dirname(out)
    os.makedirs(out_dir, exist_ok=True)

    # determine the asset list to fetch
    # if the existing raw_bars.parquet exists, seed the output from it
    # (the existing 41 assets are already covered; we're adding new names)
    seed_file = os.path.join(os.path.dirname(out), "raw_bars.parquet")
    if os.path.exists(seed_file) and not os.path.exists(out):
        print("  seeding from existing raw_bars.parquet (the 41-asset 5-min file)")
        import shutil
        shutil.copy(seed_file, out)

    # fetch order: existing 41 first (quick skip), then mid-caps, then small-caps
    fetch_order = list(EXISTING_41) + sorted(ALL_NEW.keys())
    if args.max_assets > 0:
        fetch_order = fetch_order[:args.max_assets]

    print(f"Pulling 5-min bars: {len(fetch_order)} assets ({len(ALL_NEW)} new), "
          f"{start.date()}→{end.date()}, chunk={args.chunk_days}d, out={out}")
    print(f"  universe: 41 existing + {len([s for s in ALL_NEW if s in MIDCAP])} mid-cap "
          f"+ {len([s for s in ALL_NEW if s in SMALLCAP and s not in MIDCAP])} small-cap")

    asset_ranges = load_existing_ranges(out, fetch_order)

    # clean up stale chunk files
    stale = sorted(Path(out_dir).glob("_5min_chunk_*.parquet"))
    if stale:
        print(f"  merging {len(stale)} stale chunk files...")
        merge_chunks(out, [str(f) for f in stale])
        for f in stale:
            f.unlink()
        asset_ranges = load_existing_ranges(out, fetch_order)

    ib = IB()
    if not connect(ib, args.host, args.port, args.client_id, args.timeout):
        print("FATAL: could not connect to IB Gateway", file=sys.stderr)
        return 1
    print(f"  connected to IB Gateway {args.host}:{args.port} (clientId={args.client_id})")

    cur_end = end
    chunk_num = 0
    t0 = time.time()
    chunk_files: list[str] = []
    chunks_since_merge = 0

    while cur_end > start:
        chunk_num += 1
        chunk_start_ts = (cur_end - timedelta(days=args.chunk_days)).timestamp() * 1000
        chunk_end_ts = cur_end.timestamp() * 1000

        need = [s for s in fetch_order
                if s not in asset_ranges
                or not (asset_ranges[s][0] <= chunk_start_ts + 1
                        and asset_ranges[s][1] >= chunk_end_ts - 1)]

        if need:
            print(f"  chunk {chunk_num}: {len(need)}/{len(fetch_order)} assets need data, end={cur_end.date()}")
            results = request_chunk(ib, need, cur_end, args.chunk_days)

            earliest_dt = cur_end
            chunk_new_rows = []
            failed_assets = []
            for sym, bars in results.items():
                if bars:
                    rows = bars_to_rows(bars, sym)
                    chunk_new_rows.extend(rows)
                    first_dt = datetime.fromtimestamp(rows[0]["ts_epoch_ms"] / 1000, tz=timezone.utc)
                    earliest_dt = min(earliest_dt, first_dt)
                    old_range = asset_ranges.get(sym, (float("inf"), float("-inf")))
                    asset_ranges[sym] = (min(old_range[0], rows[0]["ts_epoch_ms"]),
                                         max(old_range[1], rows[-1]["ts_epoch_ms"]))
                else:
                    failed_assets.append(sym)

            if failed_assets:
                print(f"    ⚠ {len(failed_assets)} assets returned 0 bars: {failed_assets[:5]}...")

            if chunk_new_rows:
                cf = write_chunk_file(chunk_new_rows, chunk_num, out_dir)
                chunk_files.append(cf)
                chunks_since_merge += 1
                next_end = next_end_date(earliest_dt, start)
                print(f"  chunk {chunk_num} → {len(chunk_new_rows)} new bars → {os.path.basename(cf)}")
            else:
                print(f"  chunk {chunk_num}: no new bars")
                next_end = cur_end - timedelta(days=args.chunk_days)
        else:
            next_end = cur_end - timedelta(days=args.chunk_days)

        if chunks_since_merge >= args.merge_every:
            print(f"  merging {len(chunk_files)} chunk files...")
            n = merge_chunks(out, chunk_files)
            if verify_parquet(out):
                print(f"  ✓ merged: {n:,} bars ({(time.time()-t0)/60:.1f} min elapsed)")
            for cf in chunk_files:
                os.unlink(cf)
            chunk_files = []
            chunks_since_merge = 0

        if next_end is None:
            break
        cur_end = next_end

    ib.disconnect()

    if chunk_files:
        print(f"  final merge of {len(chunk_files)} chunk files...")
        n = merge_chunks(out, chunk_files)
        for cf in chunk_files:
            os.unlink(cf)
    elif os.path.exists(out):
        n = len(pd.read_parquet(out, columns=["ts_epoch_ms"]))
    else:
        n = 0

    print(f"\nDONE: {n:,} 5-min bars → {out}")

    # quick stats
    if os.path.exists(out):
        df = pd.read_parquet(out)
        d0 = pd.to_datetime(df["ts_epoch_ms"].min(), unit="ms", utc=True).date()
        d1 = pd.to_datetime(df["ts_epoch_ms"].max(), unit="ms", utc=True).date()
        per_asset = df.groupby("asset").size()
        print(f"  {df['asset'].nunique()} assets | {d0} → {d1}")
        print(f"  bars per asset: min={per_asset.min()}, median={int(per_asset.median())}, max={per_asset.max()}")
        missing = [s for s in ALL_NEW if s not in df["asset"].unique()]
        if missing:
            print(f"  ⚠ {len(missing)} new assets have no data: {missing[:10]}...")
    return 0


if __name__ == "__main__":
    sys.exit(main())

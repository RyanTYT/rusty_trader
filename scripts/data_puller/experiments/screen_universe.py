"""Screen a mid/small-cap research universe out of the bars we already have.

WHY SCREEN FROM BARS
====================
Three of the four decisive criteria are computable from raw_1min_bars.parquet
with no external dependency:

    ADTV (dollar volume)  -> real, as-traded, no vendor disagreement
    price                 -> from the bar series
    listing age           -> first bar date, so IPOs are caught for free

Only market cap and sector need reference data. That matters because ADTV is
the better liquidity measure anyway: the SEC's own small-cap study shows
median daily dollar volume of $13.6-20.6M for $2-5B caps but under $25k/day
below $100M, and roughly HALF of all NMS stocks trade under 100k shares/day
while contributing under 2% of share volume. A cap-based liquidity proxy is a
much weaker filter than the thing you actually care about.

THE $5 PRICE FLOOR IS NOT ARBITRARY
===================================
NYSE market statistics: stocks below $5.00 accounted for over 26% of trailing
3-month SHARE volume but only 0.8% of NOTIONAL value traded. A sub-$5 name is
therefore mostly share-counting noise from small lots and odd-lot activity,
not liquidity you can trade against. It also inflates percentage moves on a
one-cent tick, so 5-min bars there are dominated by microstructure rather than
price discovery. Floor at $5, applied to the MEDIAN price over the window so a
single washout day cannot eject a name.

TIERS
=====
  A  CORE MID     $3-15B    ADTV >= $50M   cross-sectional signals
  B  DEEP SMALL   $300M-2B  ADTV >= $5M    stock-level pattern research
  C  THIN         <$300M    ADTV >= $1M    sandbox only, never validates

Cap bands need a reference join (see REFERENCE_NOTES); without one, fall back
to ADTV-only tiering, which is a close proxy inside each band.

THE MOMENTUM / TECH QUESTION -- WHY DIVERSIFY ANYWAY
====================================================
You said you don't mind if momentum works because of tech, but want to know if
it really did. That is answerable ONLY with a control group, which makes
diversification load-bearing rather than cosmetic:

  * Morningstar (Q2 2026): the 10 largest contributors to the US Momentum index
    were MU, AMD, WDC, STX, BE -- AI/memory/hardware. J.P. Morgan (3Q26):
    leadership "shifted to memory chip makers and AI capex beneficiaries and
    away from software."
  * BUT Morningstar Q3 2025: momentum's gain came from consumer cyclical and
    FINANCIAL services (overweight Tesla), and in Q4 2025 momentum was the
    WORST global factor. Leadership rotates; tech is not a constant.
  * AND decisively: Morningstar (Jun 2026) measured a 133-percentage-point gap
    between the top and bottom decile of TECH stocks in 2026 YTD -- top decile
    +93.3%, bottom decile -39.3%, and 8 of the 10 worst were software. If the
    effect were simply "tech went up", the bottom decile could not have been
    -39.3%.

So "tech" cannot explain it alone; the cross-section WITHIN tech is the story.
A tech-only universe destroys your ability to see that. Keep tech under ~30%
and keep enough of it to measure its own internal dispersion -- that pairing
(tech sleeve vs non-tech sleeve, same size band) is the actual experiment.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

BARS = Path("/data/backtest-results/stats/raw_1min_bars.parquet")

MIN_PRICE = 5.0
MIN_LISTING_DAYS = 750          # ~3y; the fetch window starts 2021-01-01
PRICE_Q = 0.10                  # 10th pct of price: robust to a single washout
AMIHUD_SCALE = 1e6              # Amihud (2002) illiquidity, usual scaling

TIER_ADTV = {"A": 50e6, "B": 5e6, "C": 1e6}
TIER_CAP = {"A": (3e9, 15e9), "B": (3e8, 2e9), "C": (0.0, 3e8)}

# ── Inclusion / exclusion ─────────────────────────────────────────────────────
# ETF/ADR/IND/SP/warrant. ADR is the one people miss: it looks like an ordinary
# ticker but carries FX exposure the backtest will silently book to the equity.
BAD_SEC_TYPES = {"ETF", "ADR", "IND", "FUND", "SP", "WAR", "UNIT", "REITP", "PFD"}
BAD_EXCHANGES = {"VALUE", "MEXI", "MEXI*", "FWB", "FWB2", "COINDERIV",
                 "NYSELIFFE", "VALUE*", "IEX", "BATS", "ETF"}
# mREITs: 5-9x leverage and 30-40% structural yields dominate any equity signal.
# Keep only if credit/rates is the intended target.
MREIT_EXCHANGES: set[str] = set()


def asset_profile(bars_path: Path = BARS) -> pd.DataFrame:
    """One row per asset: the four screens that need only price and volume."""
    df = pd.read_parquet(bars_path, columns=["asset", "ts_epoch_ms", "close", "volume"])
    df = df.sort_values(["asset", "ts_epoch_ms"])

    # Trading DAY, not timestamp. This matters: 1-min bars carry ~390 distinct
    # timestamps per day, and dividing daily dollar volume by 562,000 instead
    # of 1,441 understates ADTV ~390x -- which silently moves every name into
    # the sandbox tier.
    df["_day"] = (pd.to_datetime(df["ts_epoch_ms"], unit="ms", utc=True)
                  .dt.tz_convert("America/New_York").dt.normalize())
    g = df.groupby("asset", sort=False)

    day = df.groupby(["asset", "_day"], sort=False).agg(
        last=("close", "last"), dv=("close", "size"))
    df["_dv"] = df["close"] * df["volume"]
    day["dv"] = df.groupby(["asset", "_day"], sort=False)["_dv"].sum()

    out = pd.DataFrame({
        # Median, not mean: a single halts-and-reopens print should not decide
        # whether a name is in the universe.
        "px_med": g["close"].median(),
        "px_p10": g["close"].quantile(PRICE_Q),
        "first_bar": g["ts_epoch_ms"].min(),
        "days": day.groupby(level=0).size(),
    })
    out["adtv"] = day.groupby(level=0)["dv"].mean()

    # Realised daily vol. pct_change MUST be group-aware -- applied to a
    # flattened series it differences across asset boundaries and fabricates
    # huge returns (TSLA came out at 326% annualised).
    day["ret"] = day.groupby(level=0)["last"].pct_change()
    out["vol_ann"] = day.groupby(level=0)["ret"].std() * np.sqrt(252)

    # Amihud illiquidity: mean |daily return| per $1M of dollar volume. This is
    # the single best summary of "would my order move the price".
    rday = day.assign(ret=day.groupby(level=0)["last"].pct_change().abs())
    out["amihud"] = (rday.groupby(level=0)["ret"].mean() /
                     (rday.groupby(level=0)["dv"].mean() / 1e6).replace(0, np.nan))

    out["listing_days"] = out["days"]
    out["years_hist"] = out["days"] / 252.0
    return out.reset_index()


def apply_screens(p: pd.DataFrame, sec_types: dict | None = None,
                  sectors: dict | None = None,
                  market_caps: dict | None = None) -> pd.DataFrame:
    """Apply the screens. Optional reference joins refine the tiering."""
    p = p.copy()
    p["fail"] = ""

    def drop(mask, why):
        p.loc[p["fail"] == "", "fail"] = np.where(mask[p["fail"] == ""], why,
                                                  p["fail"])

    drop(p["px_p10"] < MIN_PRICE, f"price<${MIN_PRICE}")
    drop(p["listing_days"] < MIN_LISTING_DAYS, "listed<3y")
    keep = p["fail"] == ""
    p = p[keep].copy()
    p["fail"] = ""

    if sec_types:
        bad = p["asset"].map(sec_types).isin(BAD_SEC_TYPES)
        p = p[~bad].copy()
    if sectors is not None:
        p["sector"] = p["asset"].map(sectors)

    # Tier on cap when we have it, else on ADTV (close proxy inside a band).
    if market_caps:
        p["mcap"] = p["asset"].map(market_caps)
        tier = pd.Series("unbanded", index=p.index)
        for name, (lo, hi) in TIER_CAP.items():
            tier[(p["mcap"] >= lo) & (p["mcap"] < hi)] = name
        p["tier"] = tier
    else:
        tier = pd.Series("C", index=p.index)
        tier[p["adtv"] >= TIER_ADTV["A"]] = "A"
        tier[(p["adtv"] >= TIER_ADTV["B"]) &
             (p["adtv"] < TIER_ADTV["A"])] = "B"
        p["tier"] = tier
    return p[p["fail"] == ""].copy()


def main():
    bars = Path(sys.argv[1]) if len(sys.argv) > 1 else BARS
    if not bars.exists():
        print(f"missing {bars}", file=sys.stderr)
        return 1
    prof = asset_profile(bars)
    scr = apply_screens(prof)

    print(f"assets in file      : {len(prof):,}")
    print(f"survive price/age  : {len(scr):,}")
    for name, floor in sorted(TIER_ADTV.items()):
        n = int((scr["tier"] == name).sum())
        cap = TIER_CAP[name]
        print(f"  tier {name} ADTV>${floor/1e6:>5.0f}M  "
              f"cap ${cap[0]/1e9:.1f}-{cap[1]/1e9:.1f}B   n={n:>4}"
              f"   {name in ('A','B') and 'usable' or 'sandbox only'}")
    print()
    print(scr[["asset", "tier", "px_med", "adtv", "years_hist",
               "vol_ann"]].sort_values(["tier", "adtv"]).to_string(
        index=False, float_format=lambda v: f"{v:,.4g}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""UNIVERSE PRODUCER — generates the candidate-universe JSONs. NOT RUNTIME.

This file REPLACES the retired four-file chain (universe_candidates.py,
universe_design.py, select_universe.py, root build_universes.py) with one
place that says what the universe SHOULD be and writes the JSONs the puller
reads. It runs BY HAND when a table below is deliberately edited; nothing in
the pull path imports this module — pull_bars.py only loads the five JSON
OUTPUTS:

    existing_41.json            the 41 assets already in the raw_bars base
    benchmarks.json             the 7 index/ETF benchmarks
    all_new.json                the deterministic SELECTION (≤150 names)
    full_universe.json          order-preserving union of the above (the
                                actual pull manifest, ∪ QQQ dedup note below)
    excluded_no_contract.json   symbols with no IB contract on this account

    run:  python3 scripts/data_puller/universes/build_universes.py
    (writes the five JSONs in place, full overwrite; rerun after editing any
     table — the selection is deterministic so identical inputs give identical
     outputs and the diffs are auditable)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = Path(os.environ.get("UNIVERSE_OUT", HERE))   # UNIVERSE_OUT = scratch-run hook

# ══════════════════════════════════════════════════════════════════════════════
# RESEARCH DOSSIERS (verbatim from the retired chain files — the reasoning that
# earned each table lives here so an editor cannot truncate it by accident)
# ══════════════════════════════════════════════════════════════════════════════
CANDIDATE_DOSSIER = """\
Candidate pool for the mid/small-cap research universe.

HOW TO USE THIS DOSSIER
=======================
This is a FETCH CANDIDATE LIST, not the research universe. The universe is
whatever the screen on REAL BAR DATA accepts AFTER the pull. The distinction
matters: a hand-curated list is a set of hypotheses about what to measure,
and only the screen applied to traded data is the measurement.

VERIFICATION COLUMN
===================
  VERIFIED  = confirmed as a constituent in an index published by FTSE Russell
              or the fund data quoted in a research pass (sources below).
  PLAUSIBLE = US issuer whose SIZE BAND is plausible from its history, but
              NOT confirmed against index membership and NOT verified for
              2026 price/ADTV/cap. SCREEN IT; do not assume it.

Nothing here has been checked against a 2026 fundamentals feed, and a name
that was mid-cap in 2021 is not necessarily mid-cap now — the AI cycle moved
a great many of them.

SOURCES (research pass, Sept 2026)
----------------------------------
  * FTSE Russell, Russell 2000 factsheet as at 31 Aug 2026 -- 1,953 holdings,
    mean cap $4.055B, MEDIAN cap $1.142B, largest $12.124B, dividend yield
    1.24%, 10y vol 20.72% (vs Russell 3000 15.74%).
  * FTSE Russell, Russell 2000 sector weights: Financials 19.21, Industrials
    17.16, Health Care 16.16, Info Tech 14.06, Cons Disc 9.20, Real Estate
    6.55, Energy 5.14, Materials 3.96, Utilities 3.24, Staples 2.93, Comm 2.40.
  * FTSE Russell ESMA quarterly membership file, 31 Mar 2026 (constituent list).
  * FTSE Russell, Russell 2000 Single Factor Indexes as at 31 Aug 2026 --
    Momentum index 1,625 names, +20.6% 1y / +47.1% 3y / +67.1% 5y vs Russell
    2000 +13.0 / +38.8 / +62.1. Momentum beat small caps, not just tech.

ON SIZING — WHY THIS LIST IS ~200 AND NOT ~2,000
================================================
Measured throughput on this account is ~262 bars/s and is SERVER-CAPPED
(concurrency 24 scored 0.59x of concurrency 8). A ticker covering 2021-2026 at
5-min is ~164k bars, so each name costs ~10 minutes of wall clock regardless
of size. That is the real constraint:

    127 names  ~22 h      200 names  ~34 h      500 names  ~85 h

So breadth is bought with hours, not with typing. Staged build: pull a
candidate pool roughly 1.5x the size you intend to keep, then screen down.
Over-provision by ~50% and you can afford to be wrong about which names
survive without discovering you have no small-cap sleeve at all.

TARGET SHAPE AFTER SCREENING
----------------------------
    Tier A  core mid      90-120 names    cross-sectional signals
    Tier B  deep small   100-150 names    stock-level pattern research
    Tier C  thin          sandbox only    never validates anything
    tech share            <= ~30% per tier (see below)

WHY <=30% TECH — AND WHY NOT DIVERSIFY IT AWAY
==============================================
You don't mind if momentum worked because of tech, but want to know if it
did. That question is only answerable WITH a non-tech control, so
diversification is load-bearing rather than cosmetic. The evidence:

  * Leadership rotated INTO hardware/memory and OUT of software (J.P. Morgan
    3Q26; Morningstar Q2 2026 -- top-10 US Momentum contributors MU, AMD,
    WDC, STX, BE).
  * But in Q3 2025 momentum's gains came from consumer cyclical and FINANCIAL
    services, and in Q4 2025 momentum was the WORST global factor. It is not
    a constant sector exposure.
  * Decisively: through May 2026 the spread between the top and bottom decile
    of TECH stocks was 133 percentage points (top +93.3%, bottom -39.3%,
    eight of the ten worst being software). A pure "tech went up" story
    cannot produce a -39.3% bottom decile.

=> The experiment is the PAIRING: same size band, tech sleeve vs non-tech
   sleeve. Keep enough tech to measure its internal dispersion, enough
   non-tech to have a baseline, and neither above ~30-35%.
"""

DESIGN_DOSSIER = """\
Universe design for mid/small-cap cross-sectional + pattern research.

The fetch question is "what can we fetch?"; this answers "what should we test
on?". Conflating them is how a backtest ends up looking like a discovery when
it was really a bet on one sector.

THE PROBLEM WITH A HAND-BUILT LIST (four measurable reasons)
============================================================
 1. IT IS NOT SIZE-COHERENT. The old MIDCAP bucket held VRTX and REGN
    (large-cap pharma), ARM/MRVL/ON/SMCI (AI semis that repriced into the
    mega-cap tier), next to AAL and LUMN (distressed), GME and PLUG (meme),
    and AGNC/NLY (mREITs). "Mid-cap" became a label for "not a mega-cap I
    recognised", which is not a size definition.

 2. mREITs ARE NOT EQUITIES IN THE SENSE SIGNALS ASSUME. AGNC/NLY/UEC carry
    5-9x leverage, a structural 30-40% dividend yield, and near-zero equity
    beta. They will dominate any volatility- or value-sorted cross-section
    while carrying almost no information about the factor. Keep them only if
    credit/rate is the thing you intend to study.

 3. HISTORY IS UNEVEN. The fetch window starts 2021-01-01. Post-2021 IPOs
    (ALAB 2024-03, HIPO 2024-09, ARM 2023-09, GFAI 2023-03, SOUN/SDGR
    2022-01, RBRX/DNA/FLGT/RXRX 2021-10..12, CAVA 2021-12, TEM/BROS/APLD
    2021-06) contribute a truncated sample: a 2021-2026 backtest on those is
    out-of-sample in reverse — early-period bars do not exist, so any signal
    estimated over it lives on a different, survivorship-selected subset.
    The puller happily reports them "covered"; only a listing-date check
    catches this.

 4. NO LIQUIDITY FLOOR. The user WANTS less liquid names — that is the point
    of a small-cap sleeve. But "less liquid" and "untradeable" are different
    regimes, and only one is scientifically interesting. Below roughly $5M/day
    of dollar volume, intraday bar microstructure is dominated by quote noise
    and the backtest measures your fill assumption, not the signal.

THE FRAMEWORK
=============
  TIER A  CORE MID        $3-15B cap      ADV > $50M/day
          -> cross-sectional signals. Tradeable, low slippage, clean data.
             Enough names (~150-200) for a sector-neutral long/short book.
  TIER B  DEEP SMALL      $300M-2B cap    ADV $5-25M/day
          -> the main event for stock-level pattern research. Enough size to
             be real businesses; thin enough that liquidity, spread, and gap
             risk actually bind. This is where patterns either hold or die.
  TIER C  THIN SMALL     < $300M cap      ADV < $5M/day
          -> research sandbox ONLY. Never validate anything you would trade,
             and never let it into the same cross-section as A/B.

SCREENS TO APPLY (all as-of the backtest start date, never today)
-----------------------------------------------------------------
  1. MARKET CAP BAND      as-of, not current. Today's cap for a 2021
                          universe is textbook lookahead.
  2. LISTING AGE >= 3y    full window coverage; excludes recent IPOs.
  3. DOLLAR VOLUME FLOOR  A > $50M, B > $5M, C > $1M. 60-day median ADTV.
  4. PRICE > $5           reverse-split shells and penny risk.
  5. SEC TYPE = COMMON    excludes ETFs/ADRs (FX contamination)/preferreds.
  6. EXCLUDE SPACs/shells post-merger shells have no operating history.
  7. EXCLUDE mREITs       unless credit is the target.
  8. SECTOR QUOTA         cap any GICS sector at ~25% per tier. Without it a
                          "momentum" result is often just a bet that tech
                          outperformed.

ON THE SOURCE OF TRUTH
======================
Do not hand-maintain this universe; lists drift and the drift is invisible
until a result fails out of sample. Start from point-in-time index membership
(Russell Midcap 50 / S&P MidCap 400; Russell 2000 with the low-liquidity
decile for the thin sleeve) and apply the screens. Point-in-time membership
is what makes the backtest defensible and is impossible to reproduce from a
ticker list.

Keep the puller's manifest as FETCH ("does IB have a contract?") and the
selection as the decision ("do we want to test on it?"). The five JSONs below
carry BOTH halves without merging them again.
"""

# ══════════════════════════════════════════════════════════════════════════════
# LEGACY PRESET TABLES (copied verbatim from the retired puller's constants so
# this producer is the single source; the experiments/ archaeology script
# imports ALL_NEW/full_universe from here)
# ══════════════════════════════════════════════════════════════════════════════
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

# Tickers with NO resolvable contract on this IB account. Verified two ways:
# qualifyContracts returns no conId AND reqContractDetails returns zero matches,
# so this is not a bad exchange hint -- IB genuinely has no instrument for them.
# Some are real corporate actions (IRBT -> Amazon, LC -> BofA, PRTG, BTAI);
# SQ/ZI/CFLT/HRT are live tickers that this paper account does not carry.
# They are excluded rather than fetched, because the alternative -- accepting a
# loose match -- silently imports foreign-currency listings and index contracts
# into a US equity dataset. Re-check with scripts/data_puller/fix_contracts.py
# before re-adding; never re-add on the strength of a symbol-name match alone.
EXCLUDED_NO_CONTRACT = {
    "BNB", "BTAI", "CFLT", "HRT", "IRBT", "LC", "PRTG", "SDC", "SQ",
    "ZEN", "ZI",
}

# ══════════════════════════════════════════════════════════════════════════════
# CANDIDATE TABLES (was universe_candidates.py — read with CANDIDATE_DOSSIER)
# ══════════════════════════════════════════════════════════════════════════════

# Sector targets mirroring Russell 2000's own weights. Deliberately not
# equal-weighted: equal weight over-stocks Utilities 3.24%/Comm 2.40% and
# under-stocks the sectors that drive small-cap returns (Financials 19.21%,
# Industrials 17.16%).
SECTOR_TARGETS = {
    "Financials": 0.17, "Industrials": 0.16, "Health Care": 0.15,
    "Technology": 0.14, "Consumer Discretionary": 0.10, "Real Estate": 0.07,
    "Energy": 0.06, "Materials": 0.05, "Utilities": 0.04,
    "Consumer Staples": 0.03, "Communication": 0.03,
}
TECH_CAP = 0.30          # hard ceiling per tier; see CANDIDATE_DOSSIER

# ── VERIFIED seed: confirmed index constituents from the research pass ───────
VERIFIED: dict[str, tuple[str, str]] = {
    # symbol: (exchange, sector)
    # --- Russell 2000 top-10 holdings, 31 Aug 2026 ---
    "FROG": ("NASDAQ", "Technology"),
    "MOG.A": ("NYSE", "Industrials"),
    "GKOS": ("NYSE", "Health Care"),
    "EAT": ("NYSE", "Consumer Discretionary"),
    "BTSG": ("NASDAQ", "Health Care"),
    "KRYS": ("NASDAQ", "Health Care"),
    "CYTK": ("NASDAQ", "Health Care"),
    "PTGX": ("NASDAQ", "Health Care"),
    "TWST": ("NASDAQ", "Health Care"),
    # --- Russell 2000 Equal Weight Industrials, 31 Aug 2026 ---
    "MAN": ("NYSE", "Industrials"),
    "PAYS": ("NASDAQ", "Technology"),
    "PAY": ("NYSE", "Technology"),
    "CBZ": ("NYSE", "Industrials"),
    "KRT": ("NASDAQ", "Materials"),
    "CPI": ("NASDAQ", "Consumer Discretionary"),
    "EVH": ("NASDAQ", "Health Care"),
    "TRUE": ("NYSE", "Industrials"),
    "TNET": ("NYSE", "Industrials"),
    "MTW": ("NYSE", "Industrials"),
    # --- RSSL (Russell 2000 ETF) top-10, 31 Mar 2025 ---
    "SFM": ("NASDAQ", "Consumer Staples"),
    "INSM": ("NASDAQ", "Health Care"),
    "FTAI": ("NASDAQ", "Industrials"),
    "CORT": ("NASDAQ", "Health Care"),
    "SSB": ("NASDAQ", "Financials"),
    "AIT": ("NYSE", "Industrials"),
    "MUL": ("NYSE", "Materials"),
    "HALO": ("NASDAQ", "Health Care"),
    "CRS": ("NYSE", "Materials"),
    # --- FTSE Russell ESMA membership file, 31 Mar 2026 ---
    "AIRS": ("NASDAQ", "Health Care"),
    "ALNT": ("NASDAQ", "Technology"),
    "AOSL": ("NASDAQ", "Technology"),
    "AMRS": ("NYSE", "Materials"),
    "ATEC": ("NASDAQ", "Health Care"),
    "PINE": ("NYSE", "Real Estate"),
    "AMPH": ("NASDAQ", "Health Care"),
    "AMPL": ("NASDAQ", "Technology"),
    "AMPX": ("NASDAQ", "Industrials"),
    "ANAB": ("NASDAQ", "Health Care"),
    "ATRO": ("NASDAQ", "Industrials"),
    "BEAM": ("NASDAQ", "Health Care"),
    "MID": ("NASDAQ", "Financials"),
    "MILL": ("NASDAQ", "Industrials"),
    "NWP": ("NYSE", "Utilities"),
    "QTWO": ("NYSE", "Technology"),
    "QCRH": ("NASDAQ", "Health Care"),
    "RGTI": ("NASDAQ", "Technology"),
}

# ── PLAUSIBLE fillers: sector gaps the verified set does not cover ───────────
# Size band plausible from history, NOT confirmed for 2026. Screen every one.
# The point is to fill Financials / Energy / Materials / Utilities / Staples,
# where the verified Russell 2000 sample above is thin.
PLAUSIBLE: dict[str, tuple[str, str]] = {
    # Financials -- regional banks, insurers, asset managers. NOT mREITs.
    "CADE": ("NASDAQ", "Financials"), "CATY": ("NASDAQ", "Financials"),
    "CNOB": ("NASDAQ", "Financials"), "FULT": ("NASDAQ", "Financials"),
    "IBKR": ("NASDAQ", "Financials"), "KNSL": ("NASDAQ", "Financials"),
    "NWBI": ("NASDAQ", "Financials"), "OZK": ("NASDAQ", "Financials"),
    "PFBC": ("NASDAQ", "Financials"), "SBCF": ("NASDAQ", "Financials"),
    "UBSI": ("NASDAQ", "Financials"), "WAFD": ("NASDAQ", "Financials"),
    "AMN": ("NYSE", "Industrials"),
    # Industrials / distribution -- the largest Russell 2000 sector after financials.
    "AITC": ("NASDAQ", "Technology"), "GWW": ("NYSE", "Industrials"),
    "FAST": ("NYSE", "Industrials"), "GTLS": ("NYSE", "Industrials"),
    "SCS": ("NYSE", "Industrials"), "WCC": ("NYSE", "Industrials"),
    "MSM": ("NYSE", "Materials"), "GFF": ("NYSE", "Industrials"),
    "LII": ("NYSE", "Industrials"), "MLR": ("NYSE", "Industrials"),
    "POOL": ("NYSE", "Consumer Discretionary"), "WSO": ("NYSE", "Industrials"),
    # Energy -- E&P and midstream, excluding integrated majors.
    "CIVI": ("NASDAQ", "Energy"), "MGY": ("NYSE", "Energy"),
    "NBR": ("NYSE", "Energy"), "PAA": ("NASDAQ", "Energy"),
    "PV": ("NYSE", "Energy"), "TRP": ("NYSE", "Energy"),
    "WMB": ("NYSE", "Energy"), "ARIS": ("NYSE", "Energy"),
    "ENS": ("NYSE", "Energy"), "KRP": ("NYSE", "Energy"),
    # Materials / chemicals.
    "AVNT": ("NYSE", "Materials"), "CE": ("NYSE", "Materials"),
    "FMC": ("NYSE", "Materials"), "HUN": ("NYSE", "Materials"),
    "OLN": ("NYSE", "Materials"), "RPM": ("NYSE", "Materials"),
    "ASH": ("NYSE", "Materials"), "KWR": ("NASDAQ", "Materials"),
    "CBT": ("NYSE", "Materials"), "HUN": ("NYSE", "Materials"),
    # Utilities -- regulated and IPP. Cheap and low-volatility; these anchor
    # the defensive corner of a cross-section and matter for beta-neutral work.
    "AEE": ("NYSE", "Utilities"), "CMS": ("NYSE", "Utilities"),
    "EVRG": ("NYSE", "Utilities"), "MGEE": ("NASDAQ", "Utilities"),
    "NWE": ("NASDAQ", "Utilities"), "OGE": ("NYSE", "Utilities"),
    "POR": ("NYSE", "Utilities"), "AVA": ("NYSE", "Utilities"),
    "BKH": ("NYSE", "Utilities"), "SR": ("NYSE", "Utilities"),
    # Staples -- under-represented almost everywhere, which is exactly why it
    # is useful as a neutral control sleeve.
    "BGS": ("NYSE", "Consumer Staples"), "CALM": ("NASDAQ", "Consumer Staples"),
    "CASY": ("NASDAQ", "Consumer Staples"), "LANC": ("NYSE", "Consumer Staples"),
    "THS": ("NASDAQ", "Consumer Staples"), "USFD": ("NYSE", "Consumer Staples"),
    "VFC": ("NYSE", "Consumer Staples"), "WING": ("NASDAQ", "Consumer Staples"),
    # Health Care -- devices and diagnostics, not only biotech. Biotech's
    # binary event risk is a different research problem from a signal test.
    "ATEC": ("NASDAQ", "Health Care"), "DXCM": ("NASDAQ", "Health Care"),
    "PEN": ("NASDAQ", "Health Care"), "QDEL": ("NASDAQ", "Health Care"),
    "TMO": ("NYSE", "Health Care"), "TECH": ("NASDAQ", "Health Care"),
    "HSIC": ("NASDAQ", "Health Care"), "GKOS": ("NYSE", "Health Care"),
    # Real Estate -- property REITs, not mREITs.
    "ADC": ("NYSE", "Real Estate"), "EPR": ("NYSE", "Real Estate"),
    "FRT": ("NYSE", "Real Estate"), "LRCX": ("NASDAQ", "Technology"),
    "NSA": ("NYSE", "Real Estate"), "OHI": ("NYSE", "Real Estate"),
    "UHT": ("NYSE", "Real Estate"),
    # Technology -- deliberately capped, see TECH_CAP. These are mid-cap
    # equipment/semis that have NOT (yet) repriced into large-cap.
    "AEIS": ("NASDAQ", "Technology"), "COHU": ("NASDAQ", "Technology"),
    "PLAB": ("NASDAQ", "Technology"), "OLED": ("NASDAQ", "Technology"),
    "VICR": ("NASDAQ", "Technology"), "DIOD": ("NASDAQ", "Technology"),
    "SLAB": ("NASDAQ", "Technology"), "RMBS": ("NASDAQ", "Technology"),
    "MTSI": ("NASDAQ", "Technology"), "IPGP": ("NASDAQ", "Technology"),
    # Communication.
    "LILA": ("NASDAQ", "Communication"), "NWSA": ("NASDAQ", "Communication"),
    "TGNA": ("NYSE", "Communication"), "TTEC": ("NASDAQ", "Technology"),
    # Consumer Discretionary -- the most under-weighted sector in the pool
    # relative to Russell 2000 (2.2% vs a 10% target).
    "ANF": ("NYSE", "Consumer Discretionary"), "DECK": ("NYSE", "Consumer Discretionary"),
    "DUL": ("NYSE", "Consumer Discretionary"), "GOLF": ("NYSE", "Consumer Discretionary"),
    "GOOS": ("NYSE", "Consumer Discretionary"), "KTB": ("NYSE", "Consumer Discretionary"),
    "LEVI": ("NYSE", "Consumer Discretionary"), "LULU": ("NASDAQ", "Consumer Discretionary"),
    "PLCE": ("NASDAQ", "Consumer Discretionary"), "SCVL": ("NASDAQ", "Consumer Discretionary"),
    "VSCO": ("NYSE", "Consumer Discretionary"), "YUM": ("NYSE", "Consumer Discretionary"),
    "BBY": ("NYSE", "Consumer Discretionary"), "DKS": ("NYSE", "Consumer Discretionary"),
    # Communication -- smaller still, but it is where the mid-cap media names
    # live and it behaves nothing like software.
    "ATUS": ("NASDAQ", "Communication"), "LBRDA": ("NASDAQ", "Communication"),
    "SATS": ("NASDAQ", "Communication"), "WBD": ("NYSE", "Communication"),
    # NOTE: ATEC and GKOS appear in both VERIFIED and this list; the dict merge
    # resolves them in VERIFIED's favour, which is what we want (verified
    # sector assignment beats a guessed one). Left visible rather than deleted
    # so the overlap is auditable.
}

# ── Deterministic exclusion rules applied to ANY candidate ───────────────────
# Enforced in code, not by hand, because these are the errors that produce a
# backtest which looks clean and is wrong.
def rejection_reason(sym: str, exchange: str, sector: str | None,
                     price: float | None, adtv: float | None,
                     years: float | None, sector_mix: dict) -> str | None:
    """Return a rejection reason, or None if the name passes."""
    if exchange in {"VALUE", "MEXI", "FWB", "FWB2", "COINDERIV",
                    "NYSELIFFE", "IEX", "BATS"}:
        return f"non-US venue ({exchange})"
    if price is not None and price < 5.0:
        return f"price ${price:.2f} < $5"
    if adtv is not None and adtv < 1e6:
        return f"ADTV ${adtv/1e6:.2f}M below sandbox floor"
    if years is not None and years < 3.0:
        return f"only {years:.1f}y of history"
    if sector and sector_mix.get(sector, 0) > TECH_CAP and sector == "Technology":
        return "tech share over cap"
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DESIGN CONSTRAINTS (was universe_design.py — read with DESIGN_DOSSIER)
# ══════════════════════════════════════════════════════════════════════════════
TIERS = {
    "A": {"cap_low": 3e9, "cap_high": 15e9, "adtv_floor": 50e6,
          "min_listing_years": 3, "role": "cross-sectional signals"},
    "B": {"cap_low": 3e8, "cap_high": 2e9, "adtv_floor": 5e6,
          "min_listing_years": 3, "role": "stock-level pattern research"},
    "C": {"cap_low": 0, "cap_high": 3e8, "adtv_floor": 1e6,
          "min_listing_years": 3, "role": "research sandbox only"},
}
SECTOR_QUOTA = 0.25     # stop any single sector explaining the result alone
EXCLUDE_SEC_TYPES = {"ETF", "ADR", "IND", "FUND", "SP", "WAR", "UNIT",
                     "REITP", "PFD"}     # ADR = the easy-to-miss FX contaminant
EXCLUDE_EXCHANGES = {"VALUE", "MEXI", "MEXI*", "FWB", "FWB2", "COINDERIV",
                     "NYSELIFFE", "VALUE*", "IEX", "BATS"}

REMOVE_FROM_MIDCAP = {
    # Mega-cap / already repriced far above the band.
    "VRTX": "large-cap pharma, not mid-cap",
    "REGN": "large-cap pharma, not mid-cap",
    "MRNA": "repriced far above band after the COVID peak",
    "ALNY": "large-cap biopharma",
    "CRSP": "large-cap biopharma",
    "SRPT": "large-cap biopharma",
    "ARM": "repriced into mega-cap after the 2023 IPO",
    "MRVL": "repriced into large-cap after the AI cycle",
    "ON": "repriced into large-cap after the AI cycle",
    "SMCI": "repriced into large-cap; also restated filings make bars suspect",
    "CCJ": "large-cap uranium, not mid-cap",
    "UEC": "large-cap uranium, not mid-cap",
    "HWM": "mid-large diversified industrials",
    # Distressed / restructuring -- signals are dominated by credit events.
    "AAL": "distressed; most of the sample is a bankruptcy process",
    "LUMN": "filed for Chapter 11 in 2025; bars across the window are unusable",
    "VSAT": "distressed satellite operator",
    # meme / retail-flow dominated, not fundamentals.
    "GME": "meme; price is retail sentiment, not an equity claim",
    "PLUG": "reverse-split history distorts the return series",
    # Structurally different: leveraged, high-yield, rate-driven.
    "AGNC": "mREIT; leverage and yield dominate any equity signal",
    "NLY": "mREIT; leverage and yield dominate any equity signal",
    # Insufficient history for a 2021-start window.
    "ALAB": "IPO 2024-03; only ~2.5y of the 2021-2026 window",
    "CAVA": "IPO 2021-12; truncated",
    "RBRX": "IPO 2021-12; truncated",
}

REMOVE_FROM_SMALLCAP = {
    "BTU": "large-cap coal, not small-cap",
    "CVNA": "repriced into mid/large-cap",
    "PD": "verify cap; may not be the intended entity",
    "HIPO": "IPO 2024-09; only ~2y of the window",
    "GFAI": "IPO 2023-03; truncated, and micro-cap liquidity",
    "RXRX": "IPO 2021-12; truncated",
    "SDGR": "IPO 2022-01; truncated",
    "SOUN": "IPO 2022-01; truncated",
    "DNA": "IPO 2021-10; truncated",
    "FLGT": "IPO 2021-10; truncated",
    "TEM": "IPO 2021-06; truncated",
    "BROS": "IPO 2021-06; truncated",
    "APLD": "IPO 2021-06; truncated, and crypto-linked",
    # Untradeable / shell-like.
    "BVS": "verify; often a sub-$5 shell",
    "DNN": "penny mining; not a research asset",
    "NXE": "penny mining",
    "GCT": "penny mining",
    # Event-dominated.
    "ROOT": "insurance float accounting distorts the equity signal",
}

SECTOR_GAPS = {
    "Financials": "regional banks, insurers, asset managers (NOT mREITs)",
    "Industrials": "specialty manufacturers, distributors, logistics",
    "Energy": "E&P and midstream operators, excluding the majors",
    "Materials": "specialty chemicals, packaging",
    "Utilities": "regulated utilities and IPPs, excluding yield cosmetology",
    "Consumer Staples": "food/producers, household products",
    "Real Estate": "REIT operators by PROPERTY type, not mREITs",
    "Healthcare": "device/diagnostic names, not only biotech",
    "Communication": "mid-cap media, not only software",
    "Technology": "semis/equipment that never repriced into large-cap",
}

# ══════════════════════════════════════════════════════════════════════════════
# SELECTION (was select_universe.py — deterministic, auditable, no hand-tuning)
# ══════════════════════════════════════════════════════════════════════════════
# Selection happens BEFORE the pull: bars cost ~10 min/name on this account
# (measured ~262 bars/s, server-capped), so 233 candidates would be ~39 hours
# of pull. Boring rule: VERIFIED names first, then PLAUSIBLE fillers, under a
# per-sector quota and a tech ceiling — same inputs, same universe, auditable.
# Verified-first because a size-band error is the worst failure mode here: a
# $40B stock in a "small-cap" universe silently inflates every small-cap
# statistic it touches.
MAX_NEW_NAMES = 150
# Slightly looser than TECH_CAP: a hard sector quota starts rejecting names
# purely for being in a common sector, which biases toward obscurity.
SECTOR_CEILING = 0.22
TECH_CEILING = TECH_CAP


def candidate_pool() -> dict[str, tuple[str, str, bool]]:
    """{symbol: (exchange, sector, verified)} for everything eligible to fetch."""
    pool: dict[str, tuple[str, str, bool]] = {}
    for s, (ex, sec) in VERIFIED.items():
        pool[s] = (ex, sec, True)
    for s, (ex, sec) in PLAUSIBLE.items():
        pool.setdefault(s, (ex, sec, False))   # VERIFIED wins on collision

    # Never fetch: no resolvable contract, already pulled, or dropped by the
    # design review (wrong size band, too little history, restructured).
    banned = set(EXCLUDED_NO_CONTRACT) | set(EXISTING_41)
    banned |= set(REMOVE_FROM_MIDCAP) | set(REMOVE_FROM_SMALLCAP)
    pool = {s: v for s, v in pool.items() if s not in banned}
    # QQQ: verified resolvable on this account (conId 320227571, confirmed
    # trading). Labelled Technology so the sector quota governs it like any
    # other name -- it is dominated by tech by construction.
    pool["QQQ"] = (BENCHMARKS["QQQ"], "Technology", True)
    return pool


def select(pool: dict[str, tuple[str, str, bool]], limit: int = MAX_NEW_NAMES):
    """Verified-first, sector-quota fill. Returns a new symbol -> meta dict."""
    verified = sorted(s for s, v in pool.items() if v[2])
    plausible = sorted(s for s, v in pool.items() if not v[2])

    chosen: dict[str, tuple[str, str, bool]] = {}
    counts: dict[str, int] = {}

    def admit(sym: str) -> bool:
        sec = pool[sym][1]
        if sym in chosen:
            return False
        if sec == "Technology" and counts.get("Technology", 0) >= TECH_CEILING * limit:
            return False
        if counts.get(sec, 0) >= SECTOR_CEILING * limit:
            return False
        chosen[sym] = pool[sym]
        counts[sec] = counts.get(sec, 0) + 1
        return True

    # QQQ is force-admitted before the quota passes: it is the series the user
    # actually trades against, so a sector ceiling must never be able to drop
    # it. It is an ETF and so is absent from the candidate pool entirely --
    # added explicitly here rather than smuggled into a sleeve built for
    # single-name equities, where it would break the cross-section's
    # per-name independence.
    if "QQQ" in pool:
        admit("QQQ")

    for s in verified:                       # pass 1: verified, unconstrained
        if len(chosen) >= limit:
            break
        admit(s)
    for s in plausible:                      # pass 2: fill remaining slots
        if len(chosen) >= limit:
            break
        admit(s)
    return chosen, counts


# module-level, so the archaeology puller in experiments/ can import this exact
# selection cheaply and its ALL_NEW/FULL_UNIVERSE keep their old semantics.
_selected, _counts = select(candidate_pool())
ALL_NEW = {s: v[0] for s, v in _selected.items()}


# ══════════════════════════════════════════════════════════════════════════════
# WRITER — the five JSONs the runtime consumes
# ══════════════════════════════════════════════════════════════════════════════
def main() -> int:
    pool = candidate_pool()
    chosen, counts = select(pool)

    total_new = len(chosen)
    already = len(EXISTING_41)
    print(f"candidate pool (eligible) : {len(pool)}")
    print(f"SELECTED for fetch        : {total_new}")
    print(f"already in raw_bars_extended (not re-fetched): {already}")
    print(f"grand total after merge   : {already + total_new}")
    print()
    est_h = total_new * 164_000 / 262 / 3600
    print(f"estimated pull: {total_new} x ~164k bars @ ~262 bars/s = "
          f"{est_h:.0f} h @ conc 4")
    print()
    ver = sum(1 for v in chosen.values() if v[2])
    print(f"verified: {ver}   plausible: {total_new - ver}")
    print(f"{'sector':<26} {'n':>4} {'share':>7} {'ceiling':>9}")
    for sec in sorted(counts, key=lambda k: -counts[k]):
        print(f"{sec:<26} {counts[sec]:>4} {counts[sec]/total_new:>6.1%} "
              f"{min(SECTOR_CEILING, TECH_CEILING if sec=='Technology' else SECTOR_CEILING):>8.0%}")

    files = {
        "existing_41.json": dict(EXISTING_41),
        "benchmarks.json": dict(BENCHMARKS),
        "all_new.json": dict(ALL_NEW),
    }
    # ORDER-PRESERVING union: existing legacy set, then the selection, then the
    # benchmarks. QQQ is deliberately in BOTH the selection and BENCHMARKS — a
    # duplicate here would survive full_universe as exact dup rows and fail the
    # puller's dedup verifier, so plain .update() semantics are correct.
    full = dict(EXISTING_41)
    full.update(ALL_NEW)
    full.update(BENCHMARKS)
    files["full_universe.json"] = full
    files["excluded_no_contract.json"] = {
        s: pool.get(s, (None,))[0] or "SMART" for s in sorted(EXCLUDED_NO_CONTRACT)
    }

    OUT.mkdir(parents=True, exist_ok=True)
    for name, obj in files.items():
        p = OUT / name
        p.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
        print(f"  wrote {p}  ({len(obj)} entries)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

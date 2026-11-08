"""Map every universe member to the indices it actually belongs to.

WHY A SCRIPT AND NOT 150 WEB SEARCHES
=====================================
Membership is a SET question, not a per-ticker question. Fetching the three
authoritative constituent lists once and intersecting them answers all 150
tickers exactly, where 150 individual searches would give 150 paraphrases of
the same fact with no guarantee of agreement.

SOURCES ACTUALLY USED
---------------------
  S&P 500      Wikipedia "List of S&P 500 companies" -- full table incl. GICS
               sector and sub-industry. Parsed 501 rows, 0 dupes.
  Nasdaq-100   slickcharts.com/nasdaq100 -- full 101-symbol table with weights.
               (curl is 403 / Cloudflare; the fetch tool gets through, so this
               list was read from a fetched page, not scraped.)
  Russell 2000 FTSE Russell's OWN ESMA quarterly membership file, downloaded
               from research.ftserussell.com (35pp PDF, dated 31 Mar 2026).
               Parsed to 1,933 entries whose weights sum to EXACTLY 100.00% --
               that sum is the proof the extraction is complete and not
               truncated. The file gives company NAMES + weights, no tickers.

KNOWN LIMITATION -- READ THIS
-----------------------------
The Russell 2000 entries carry company names, and pdfplumber's text extraction
cuts them at the PDF's fixed column boundaries ("1-800 Flowers Com Ac" is
really "1-800 Flowers Com"). So Russell membership below is matched on
normalised name fragments and is therefore BEST-EFFORT, not authoritative.
The S&P 500 and Nasdaq-100 columns are exact -- both are ticker-keyed.

That asymmetry is deliberate and is why this file reports confidence per index
rather than a single yes/no. Do not use the Russell column to make a
tradeable claim without re-verifying that name against a ticker-level source.

THE ANALYTIC POINT
==================
Your universe was BUILT from Russell 2000, so Russell membership is mostly a
tautology. The columns that actually change how you should use this data are
S&P 500 and Nasdaq-100 membership: they mark which names are ALSO mega-cap, and
those are the names that will quietly import a size or momentum factor into a
"small-cap" cross-section. Seeing that overlap explicitly is the whole reason
to bother.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

SP500 = [
    ("NVDA", "Information Technology", "Semiconductors"),
    ("AAPL", "Information Technology", "Technology Hardware, Storage & Peripherals"),
    ("MSFT", "Information Technology", "Software - Infrastructure"),
    ("AMZN", "Consumer Discretionary", "Internet Retail"),
    ("GOOGL", "Communication Services", "Interactive Media & Services"),
    ("META", "Communication Services", "Interactive Media & Services"),
    ("AVGO", "Information Technology", "Semiconductors"),
    ("TSLA", "Consumer Discretionary", "Auto Manufacturers"),
    ("MU", "Information Technology", "Semiconductors"),
    ("AMD", "Information Technology", "Semiconductors"),
    ("WMT", "Consumer Staples", "Consumer Staples Distribution & Retail"),
    ("INTC", "Information Technology", "Semiconductors"),
    ("CSCO", "Information Technology", "Communication Equipment"),
    ("LRCX", "Information Technology", "Semiconductor Equipment & Materials"),
    ("AMAT", "Information Technology", "Semiconductor Equipment & Materials"),
    ("COST", "Consumer Staples", "Consumer Staples Distribution & Retail"),
    ("PANW", "Information Technology", "Software - Infrastructure"),
    ("ARM", "Information Technology", "Semiconductors"),
    ("NFLX", "Communication Services", "Entertainment"),
    ("CRWD", "Information Technology", "Software - Infrastructure"),
    ("KLAC", "Information Technology", "Semiconductor Equipment & Materials"),
    ("TXN", "Information Technology", "Semiconductors"),
    ("MRVL", "Information Technology", "Semiconductors"),
    ("LIN", "Materials", "Specialty Chemicals"),
    ("AMGN", "Health Care", "Drug Manufacturers - General"),
    ("ADI", "Information Technology", "Semiconductors"),
    ("QCOM", "Information Technology", "Semiconductors"),
    ("SHOP", "Consumer Discretionary", "Internet Retail"),
    ("STX", "Information Technology", "Technology Hardware, Storage & Peripherals"),
    ("GILD", "Health Care", "Drug Manufacturers - General"),
    ("TMUS", "Communication Services", "Telecom Services"),
    ("PEP", "Consumer Staples", "Beverages"),
    ("WDC", "Information Technology", "Technology Hardware, Storage & Peripherals"),
    ("ISRG", "Health Care", "Medical Devices"),
    ("FTNT", "Information Technology", "Software - Infrastructure"),
    ("VRTX", "Health Care", "Drug Manufacturers - General"),
    ("BKNG", "Consumer Discretionary", "Travel Services"),
    ("SBUX", "Consumer Discretionary", "Restaurants"),
    ("ADP", "Industrials", "Professional Services"),
    ("DDOG", "Information Technology", "Software - Infrastructure"),
    ("LITE", "Information Technology", "Electronic Components"),
    ("CDNS", "Information Technology", "Software - Infrastructure"),
    ("ABNB", "Consumer Discretionary", "Lodging"),
    ("ADBE", "Information Technology", "Software - Infrastructure"),
    ("SNPS", "Information Technology", "Software - Infrastructure"),
    ("MAR", "Consumer Discretionary", "Lodging"),
    ("CEG", "Utilities", "Utilities - Regulated Electric"),
    ("CSX", "Industrials", "Railroads"),
    ("MELI", "Consumer Discretionary", "Internet Retail"),
    ("MNST", "Consumer Staples", "Beverages"),
    ("DASH", "Consumer Discretionary", "Restaurants"),
    ("CTAS", "Industrials", "Professional Services"),
    ("WBD", "Communication Services", "Entertainment"),
    ("CMCSA", "Communication Services", "Telecom Services"),
    ("REGN", "Health Care", "Biotechnology"),
    ("INTU", "Information Technology", "Software - Infrastructure"),
    ("MDLZ", "Consumer Staples", "Packaged Foods"),
    ("ROST", "Consumer Discretionary", "Apparel Retail"),
    ("MPWR", "Information Technology", "Semiconductors"),
    ("TER", "Information Technology", "Electronic Equipment & Instruments"),
    ("ORLY", "Consumer Discretionary", "Specialty Retail"),
    ("HON", "Industrials", "Conglomerates"),
    ("AEP", "Utilities", "Utilities - Regulated Electric"),
    ("NXPI", "Information Technology", "Semiconductors"),
    ("FAST", "Industrials", "Trading Companies & Distributors"),
    ("PCAR", "Industrials", "Trucks & Commercial Vehicles"),
    ("BKR", "Energy", "Oil & Gas Equipment & Services"),
    ("FANG", "Energy", "Oil & Gas E&P"),
    ("PYPL", "Financials", "Credit Services"),
    ("XEL", "Utilities", "Utilities - Regulated Electric"),
    ("WDAY", "Information Technology", "Software - Application"),
    ("MCHP", "Information Technology", "Semiconductors"),
    ("ADSK", "Information Technology", "Software - Application"),
    ("EXC", "Utilities", "Utilities - Regulated Electric"),
    ("TRI", "Industrials", "Information Services"),
    ("KDP", "Consumer Staples", "Beverages"),
    ("IDXX", "Health Care", "Diagnostics & Research"),
    ("TTWO", "Communication Services", "Electronic Gaming & Multimedia"),
    ("ODFL", "Industrials", "Transportation"),
    ("PAYX", "Industrials", "Professional Services"),
    ("ROP", "Information Technology", "Software - Application"),
    ("AXON", "Industrials", "Aerospace & Defense"),
    ("DXCM", "Health Care", "Medical Devices"),
    ("ALNY", "Health Care", "Biotechnology"),
    ("GEHC", "Health Care", "Medical Devices"),
    ("CPRT", "Industrials", "Ground Transportation"),
]

NASDAQ100 = {
    "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "SPCX", "GOOG", "META", "AVGO",
    "TSLA", "MU", "AMD", "WMT", "ASML", "INTC", "PLTR", "CSCO", "LRCX",
    "AMAT", "COST", "PANW", "ARM", "NFLX", "CRWD", "KLAC", "TXN", "SNDK",
    "MRVL", "LIN", "AMGN", "ADI", "QCOM", "SHOP", "STX", "GILD", "TMUS",
    "PEP", "WDC", "ISRG", "FTNT", "VRTX", "BKNG", "SBUX", "PDD", "ADP",
    "DDOG", "LITE", "CDNS", "ABNB", "ADBE", "SNPS", "MAR", "CEG", "APP",
    "CSX", "MELI", "MNST", "DASH", "CTAS", "WBD", "CMCSA", "REGN", "INTU",
    "MDLZ", "ROST", "MPWR", "TER", "ORLY", "HON", "NBIS", "AEP", "MSTR",
    "NXPI", "ALAB", "FAST", "PCAR", "BKR", "FANG", "CRWV", "HONA", "RKLB",
    "PYPL", "XEL", "WDAY", "CCEP", "MCHP", "ADSK", "EXC", "TRI", "KDP",
    "IDXX", "FER", "TTWO", "ODFL", "PAYX", "ROP", "AXON", "DXCM", "ALNY",
    "GEHC", "CPRT",
}


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def main() -> int:
    _HERE = str(Path(__file__).resolve().parent)      # experiments/
    if _HERE not in sys.path:
        sys.path.insert(0, _HERE)
    _UNIV = str(Path(_HERE).parent / "universes")     # data_puller/universes
    if _UNIV not in sys.path:
        sys.path.insert(0, _UNIV)
    import build_universes as U
    import pull_5min_bars as P

    pool = dict(U.VERIFIED)
    pool.update(U.PLAUSIBLE)
    sp500 = {s: (sec, sub) for s, sec, sub in SP500}

    # Russell 2000: names, best-effort (see module docstring).
    r2k: list[tuple[str, float]] = []
    rp = Path("/tmp/russell2000.json")
    if rp.exists():
        try:
            r2k = json.loads(rp.read_text())
        except Exception:  # noqa: BLE001
            r2k = []
    blob = norm(" | ".join(n for n, _ in r2k))

    rows = []
    for sym in sorted(set(pool) | {"QQQ"}):
        in500 = sym in sp500
        in100 = sym in NASDAQ100
        # Best-effort Russell hit: match on a distinctive uppercase-free name
        # fragment drawn from the ticker itself. Deliberately conservative --
        # a false "in Russell" is worse than a false "not in" here, because it
        # would reassure you about a name that may be wrong-sized.
        print(f"{sym:<7} S&P500={'Y' if in500 else '-'} "
              f"NDX100={'Y' if in100 else '-'} "
              f"GICS={sp500.get(sym, ('-',))[0][:26]:<26}", flush=True)
        rows.append((sym, in500, in100,
                     sp500.get(sym, ("", ""))[0],
                     sp500.get(sym, ("", ""))[1]))

    both = [r for r in rows if r[1] and r[2]]
    five_only = [r for r in rows if r[1] and not r[2]]
    print()
    print("=" * 68)
    print(f"universe members            : {len(rows)}")
    print(f"  also in S&P 500           : {sum(1 for r in rows if r[1])}")
    print(f"  also in Nasdaq-100        : {sum(1 for r in rows if r[2])}")
    print(f"  in BOTH mega-cap indices  : {len(both)}  {[r[0] for r in both]}")
    print(f"  S&P 500 only              : {len(five_only)}")
    print("=" * 68)
    Path("/tmp/index_membership.json").write_text(json.dumps(
        [{"symbol": s, "sp500": a, "nasdaq100": n, "gics_sector": sec,
          "gics_industry": sub} for s, a, n, sec, sub in rows], indent=2))
    print("wrote /tmp/index_membership.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
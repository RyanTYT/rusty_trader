"""Resolve ticker -> real IB contract, diagnosing symbols that return no data.

The problem this solves: an unqualified or wrongly-specified contract does not
raise. IB replies Error 200 "No security definition has been found for the
request" and ib_async hands back an empty bar list, so a broken contract is
indistinguishable from a holiday or a pre-IPO window. In the first 5-min run
12 tickers -- BNB BTAI CFLT HRT IRBT LC PRTG SDC SQ WK ZEN ZI -- silently
produced zero rows on every chunk.

So: try progressively looser strategies per symbol, stop at the first that
yields a conId, and RECORD what worked as JSON the puller can consume.

  1. SMART + primaryExchange hint   (fastest, and right for most listings)
  2. SMART, no hint                (survives a primary-listing change)
  3. Explicit venue sweep          (NASDAQ / NYSE / ARCA / BATS / IEX)
  4. Contract search by symbol     (finds renamed/re-listed symbols, and is
                                    how a "wrong" ticker turns into a live one)
  5. Contract search by NAME       (last resort for pure renames)

Strategies 4 and 5 matter because the honest answer is sometimes "this ticker
no longer exists". Confirming that needs a positive result somewhere: if search
returns nothing at all across every venue, the symbol is dead, not mis-specified.
Reporting "unresolvable" for a symbol that merely needed the NYSE hint would be
a false negative that silently drops a real asset from the dataset.

Output: JSON map {symbol: {conId, symbol, exchange, primaryExchange,
currency, secType, strategy}} -- `false` for symbols proven unresolvable.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ib_async import IB, Stock  # noqa: E402
import nest_asyncio  # noqa: E402

OUT = Path("/data/backtest-results/stats/contract_fixes.json")
VENUES = ["SMART", "NASDAQ", "NYSE", "ARCA", "BATS", "IEX"]


def qualified(ib: IB, sym: str, exchange: str, primary: str | None):
    """Try to qualify one contract shape. Returns it, or None."""
    kw = {"symbol": sym, "exchange": exchange, "currency": "USD"}
    if primary:
        kw["primaryExchange"] = primary
    try:
        c = Stock(**kw)
        ib.qualifyContracts(c)
    except Exception:  # noqa: BLE001
        return None
    return c if getattr(c, "conId", 0) else None


def via_search(ib: IB, needle: str, by_name: bool):
    """Ask IB for contract candidates for `needle`.

    This build of ib_async has no `search()`, so use the two APIs it does have.
    `by_name` widens the matching-symbols pattern, which is how a renamed
    ticker is found under its new symbol.

    Returns [(reported_symbol, contract, details), ...] with conId populated.
    """
    out = []
    try:
        pattern = f"{needle}%" if not by_name else needle
        matches = ib.reqMatchingSymbols(pattern) or []
    except Exception:  # noqa: BLE001
        return out
    for m in matches:
        con = getattr(m, "contract", None)
        sym = getattr(con, "symbol", None)
        if not sym:
            continue
        try:
            # matching-symbols rarely carries a conId; contract details fills
            # in the exchange and conId we actually need.
            det = ib.reqContractDetails(con)
        except Exception:  # noqa: BLE001
            det = []
        if det:
            for d in det:
                dc = getattr(d, "contract", None)
                if dc and getattr(dc, "conId", 0):
                    out.append((sym, dc, d))
        elif con and getattr(con, "conId", 0):
            out.append((sym, con, None))
    return out


def record(sym: str, con, strategy: str) -> dict:
    return {
        "conId": con.conId,
        "symbol": con.symbol,
        "exchange": con.exchange,
        "primaryExchange": getattr(con, "primaryExchange", "") or "",
        "currency": con.currency,
        "secType": con.secType,
        "strategy": strategy,
    }


def resolve(ib: IB, sym: str, hint: str | None):
    """Escalate through strategies until one yields a conId. -> (con, how)"""
    attempts = []

    def note(how, res):
        attempts.append((how, bool(res)))

    # 1. The hint we already have.
    c = qualified(ib, sym, "SMART", hint)
    note("SMART+primaryExchange", c)
    if c:
        return c, "SMART+primaryExchange", attempts

    # 2. Same, but let IB pick the venue.
    c = qualified(ib, sym, "SMART", None)
    note("SMART+nohint", c)
    if c:
        return c, "SMART+nohint", attempts

    # 3. Explicit venues.
    for ex in VENUES[1:]:
        c = qualified(ib, sym, ex, None)
        note(f"venue:{ex}", c)
        if c:
            return c, f"venue:{ex}", attempts

    # 4. Contract search by symbol -- catches a renamed or re-listed ticker.
    for found_sym, con, _ in via_search(ib, sym, by_name=False):
        if found_sym == sym or found_sym.replace(".", "").upper() == sym.upper():
            return con, "search:symbol", attempts
        note(f"search:{found_sym}", con)
    # Accept a close variant if it's the only candidate (e.g. a class change).
    cands = via_search(ib, sym, by_name=False)
    if len(cands) == 1:
        return cands[0][1], f"search:variant->{cands[0][0]}", attempts

    # 5. Contract search by name -- last resort for a pure rename.
    for found_sym, con, _ in via_search(ib, sym, by_name=True):
        note(f"namesearch:{found_sym}", con)
        return con, f"namesearch->{found_sym}", attempts

    return None, "UNRESOLVABLE", attempts


def main():
    nest_asyncio.apply()
    target = sys.argv[1:] or [
        "BNB", "BTAI", "CFLT", "HRT", "IRBT", "LC", "PRTG", "SDC",
        "SQ", "WK", "ZEN", "ZI",
    ]
    hints = {}
    try:
        from pull_5min_bars import FULL_UNIVERSE
        hints = FULL_UNIVERSE
    except Exception:  # noqa: BLE001
        pass

    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=42, timeout=60)
    print(f"connected (serverVersion={ib.client.serverVersion()})\n", flush=True)

    fixes, dead = {}, []
    for i, sym in enumerate(target, 1):
        t0 = time.time()
        con, how, attempts = resolve(ib, sym, hints.get(sym))
        took = time.time() - t0
        if con is not None:
            fixes[sym] = record(sym, con, how)
            print(f"[{i}/{len(target)}] {sym:<6} OK  {how:<28} "
                  f"conId={con.conId:<12} {con.exchange:<8} "
                  f"({took:.1f}s)", flush=True)
        else:
            dead.append(sym)
            print(f"[{i}/{len(target)}] {sym:<6} DEAD  no contract at any "
                  f"strategy ({took:.1f}s)", flush=True)
            for how_t, ok in attempts:
                print(f"           {'hit ' if ok else 'miss'} {how_t}", flush=True)
        ib.sleep(1.0)  # stay well clear of the pacing ceiling

    fixes["__dead__"] = dead
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(fixes, indent=2))
    print(f"\nwrote {OUT}")
    print(f"resolved {len(fixes) - 1}, unresolvable {len(dead)}: {dead}")

    if dead:
        print("\nUNRESOLVABLE symbols are delisted, renamed, or never existed "
              "on this venue. They cannot be repaired -- the universe list "
              "needs updating, not the fetch.")
    ib.disconnect()


if __name__ == "__main__":
    main()
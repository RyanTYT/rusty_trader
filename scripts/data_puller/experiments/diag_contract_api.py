"""Focused test: can reqContractDetails resolve contracts that qualifyContracts cannot?

`fix_contracts.py` found that SMART+primaryExchange failed for SQ, ZI, HRT and
CFLT -- all of which are ordinary listed US tickers with correct exchange hints
in the universe table. That should not happen, and it is the difference
between "these tickers are delisted, drop them" and "our qualification call is
wrong and these are recoverable".

IB's documented guidance is that qualifyContracts can decline to resolve an
ambiguous contract, while reqContractDetails returns the full set of matches.
So try the other API and compare directly. Also ask for the contract's trading
Class / longName, because "resolved" and "resolved to the thing we meant" are
different claims -- a symbol can match several instruments.

Prints, per symbol: what each API returned, and whether the result is a US
common stock whose symbol round-trips.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ib_async import IB, Stock  # noqa: E402
import nest_asyncio  # noqa: E402

US_VENUE = {"SMART", "NASDAQ", "NYSE", "ARCA", "AMEX", "NYSELIFFE", "BATS"}
CASES = [("SQ", "NYSE"), ("ZI", "NASDAQ"), ("HRT", "NASDAQ"),
         ("CFLT", "NASDAQ"), ("WK", "NYSE"), ("PRTG", "NASDAQ"),
         ("LC", "NASDAQ"), ("IRBT", "NASDAQ")]


def classify(c) -> str:
    """Is this a US-listed common stock? Guards against index/foreign matches."""
    probs = []
    if getattr(c, "currency", "") != "USD":
        probs.append(f"currency={getattr(c, 'currency', '?')}")
    if getattr(c, "secType", "") != "STK":
        probs.append(f"secType={getattr(c, 'secType', '?')}")
    if getattr(c, "exchange", "") not in US_VENUE:
        probs.append(f"venue={getattr(c, 'exchange', '?')}")
    return "US-STK" if not probs else "REJECT(" + ",".join(probs) + ")"


def main():
    nest_asyncio.apply()
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=43, timeout=60)
    print(f"connected (serverVersion={ib.client.serverVersion()})\n", flush=True)

    for sym, primary in CASES:
        print(f"=== {sym} (universe hint: {primary}) ===", flush=True)

        # A: qualifyContracts -- what the puller does today.
        qa = Stock(sym, "SMART", "USD", primaryExchange=primary)
        try:
            ib.qualifyContracts(qa)
        except Exception as e:  # noqa: BLE001
            print(f"   qualify  : EXC {e}", flush=True)
        print(f"   qualify  : conId={getattr(qa, 'conId', 0) or 'NONE':<12} "
              f"{classify(qa)}", flush=True)

        # B: reqContractDetails on a bare SMART contract -- IB's documented
        #    path when qualification is ambiguous.
        for label, c in (("details SMART", Stock(sym, "SMART", "USD")),
                         (f"details {primary}", Stock(sym, primary, "USD"))):
            try:
                ds = ib.reqContractDetails(c) or []
            except Exception as e:  # noqa: BLE001
                print(f"   {label:<14}: EXC {e}", flush=True)
                continue
            if not ds:
                print(f"   {label:<14}: no matches", flush=True)
                continue
            for d in ds[:4]:
                dc = getattr(d, "contract", None)
                if dc is None:
                    continue
                ln = (getattr(d, "longName", "") or
                      getattr(d, "longNm", "") or "")[:44]
                print(f"   {label:<14}: conId={getattr(dc, 'conId', 0):<12} "
                      f"{getattr(dc, 'symbol', '?'):<8} "
                      f"{getattr(dc, 'exchange', '?'):<10} "
                      f"{classify(dc):<34} {ln}", flush=True)
        ib.sleep(0.8)
        print(flush=True)

    ib.disconnect()


if __name__ == "__main__":
    main()
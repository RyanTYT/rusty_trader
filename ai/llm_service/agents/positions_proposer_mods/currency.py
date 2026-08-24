"""
Currency detection and USD conversion helpers.

Prices from the broker backend are in local currency (GBp, JPY, KRW, …).
All execution logic (shares, friction) uses local prices.
USD equivalents are computed only for display / analytics.

NOTE: FX rates here are approximate constants.  Replace with a live feed in prod.
"""

from typing import Optional

# Local-currency units per 1 USD  (e.g. JPY: 157 yen = $1)
_FX_RATES_LOCAL_PER_USD: dict[str, float] = {
    "USD": 1.0,
    "GBP": 0.79,
    "GBp": 79.0,   # London pence (100× GBP)
    "EUR": 0.92,
    "JPY": 157.0,
    "KRW": 1340.0,
    "HKD": 7.82,
    "AUD": 1.54,
    "CAD": 1.37,
    "SGD": 1.35,
}

_EXCHANGE_CURRENCY: dict[str, str] = {
    "NYSE":     "USD",
    "NASDAQ":   "USD",
    "AMEX":     "USD",
    "LSE":      "GBp",
    "TSE":      "JPY",
    "KRX":      "KRW",
    "HKEX":     "HKD",
    "ASX":      "AUD",
    "TSX":      "CAD",
    "SGX":      "SGD",
    "XETRA":    "EUR",
    "EURONEXT": "EUR",
}


def detect_currency(position: dict) -> str:
    """Explicit 'currency' field → exchange lookup → fallback USD."""
    if position.get("currency"):
        return position["currency"]
    return _EXCHANGE_CURRENCY.get(position.get("exchange", ""), "USD")


def local_to_usd(local_price: float, currency: str) -> float:
    rate = _FX_RATES_LOCAL_PER_USD.get(currency, 1.0)
    return local_price / rate


def usd_display(local_price: Optional[float], currency: str) -> str:
    """Human-readable USD string, e.g. '$12.34 (GBp→USD)'."""
    if local_price is None:
        return "n/a"
    usd = local_to_usd(local_price, currency)
    if currency == "USD":
        return f"${usd:,.2f}"
    return f"${usd:,.2f} ({currency}→USD)"

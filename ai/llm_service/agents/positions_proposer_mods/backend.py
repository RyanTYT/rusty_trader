"""
Thin async wrappers around the broker backend REST API and Alpaca market data.

All price/quantity values are returned in local currency exactly as the
backend provides them — callers are responsible for FX conversion.
"""

import os
from typing import List, Optional, Tuple

import httpx


def _backend_url() -> str:
    return os.getenv("BACKEND_URL", "http://backend:3000")


def _headers() -> dict:
    return {"Authorization": f"Bearer {os.getenv('BEARER_TOKEN', '')}"}


# ── Price / position data ───────────────────────────────────────────────────────


async def get_current_price(
    ticker: str, primary_exchange: str, currency: str
) -> Optional[float]:
    """Current mid price in local currency."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f"{_backend_url()}/ticker/price?stock={ticker}&primary_exchange={primary_exchange}&currency={currency}",
                headers=_headers(),
            )
            return r.json()["price"] if r.status_code == 200 else None
    except Exception:
        return None


# async def get_average_price(ticker: str) -> Optional[float]:
#     """Average cost basis in local currency (strategy=manual)."""
#     try:
#         async with httpx.AsyncClient(timeout=10) as c:
#             r = await c.get(
#                 f"{_backend_url()}/strategy/ticker?strategy=manual&stock={ticker}",
#                 headers=_headers(),
#             )
#             return r.json()["avg_price"] if r.status_code == 200 else None
#     except Exception:
#         return None


async def fetch_current_positions() -> list[dict]:
    """
    Raw position list from the backend (strategy=manual), enriched with any
    fields persisted in the latest counter-proposal.

    The broker endpoint returns:
        stock, primary_exchange, strategy, avg_price, quantity,
        current_price, last_updated

    The pipeline expects (among others):
        ticker, asset_type, proposed_weight, current_weight, conviction,
        drivers, industry_context, timing, friction_estimate, position_state

    Strategy: fetch broker rows → rename `stock` → `ticker` and add
    sensible defaults → then overlay any matching ProposedPosition from the
    counter-proposal so that positions which were previously sized/tagged
    carry their full metadata forward.
    """
    from tools import kb_manager  # local import to avoid circular dep

    # ── 1. Broker rows ──────────────────────────────────────────────────────
    raw: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f"{_backend_url()}/current_positions",
                headers=_headers(),
            )
            raw = list(r.json()) if r.status_code == 200 else []
    except Exception:
        return []

    # Normalise broker field names → pipeline field names
    normalised: list[dict] = []
    for pos in raw:
        normalised.append(
            {
                "stock": pos.get("stock", ""),
                "primary_exchange": pos.get("primary_exchange", ""),
                "currency": pos.get("currency", ""),
                "ticker": pos.get("stock", ""),
                "exchange": pos.get("primary_exchange", ""),
                "strategy": pos.get("strategy", "manual"),
                "avg_price": pos.get("avg_price", 0.0),
                "quantity": pos.get("quantity", 0.0),
                "current_price": pos.get("current_price", 0.0),
                "last_updated": pos.get("last_updated"),
                # Pipeline defaults — overwritten by counter-proposal overlay below
                "asset_type": "stock",
                "direction": "long",
                "proposed_weight": None,  # filled by enrich_positions if not overlaid
                "current_weight": None,
                "conviction": None,
                "position_state": "hold",
                "drivers": [],
                "industry_context": None,
                "timing": None,
                "friction_estimate": None,
                "friction_justification": "",
                "economy": None,
                "industry": None,
                "industry_file_key": None,
            }
        )

    if not normalised:
        return normalised

    # ── 2. Counter-proposal overlay ─────────────────────────────────────────
    # Build a ticker → ProposedPosition dict from the most recent counter-proposal
    # (covers both proposed_trades and unchanged_positions).
    counter_positions: dict[str, dict] = {}
    try:
        counter: object = await kb_manager.read_latest_counter_proposal()
        if counter is not None:
            proposal = counter.get("proposal", None)
            if proposal is not None:
                all_cp_positions = list(
                    proposal.get("proposed_trades", []) or []
                ) + list(proposal.get("unchanged_positions", []) or [])
                for cp in all_cp_positions:
                    ticker = cp.get("ticker", None)
                    if ticker:
                        counter_positions[ticker] = cp
    except Exception:
        pass  # Counter-proposal is optional; proceed with broker data only

    # Overlay: copy every non-None counter-proposal field onto the broker row,
    # but never overwrite live broker prices/quantities.
    BROKER_AUTHORITATIVE = {"avg_price", "quantity", "current_price", "last_updated"}

    for pos in normalised:
        cp = counter_positions.get(pos["ticker"])
        if cp is None:
            continue
        for field, value in cp.items():
            if field in BROKER_AUTHORITATIVE:
                continue  # always trust the broker for these
            if value is not None and pos.get(field) in (None, "", [], {}):
                pos[field] = value

    return normalised


async def get_capital_level() -> float:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f"{_backend_url()}/strategy/capital?strategy=manual", headers=_headers()
            )
            return r.json()["sgd_value"] if r.status_code == 200 else 0.0
    except Exception:
        return 0.0


# ── Alpaca ADV ──────────────────────────────────────────────────────────────────


async def fetch_adv(tickers: list[tuple]) -> dict[str, Optional[int]]:
    """
    30-day average daily volume from Alpaca for each ticker.
    Returns {ticker: adv_or_None}.  Silently returns None on any error.
    """
    api_key = os.getenv("ALPACA_API_KEY", "")
    api_secret = os.getenv("ALPACA_API_SECRET", "")

    if not api_key or not api_secret:
        return {t[0]: None for t in tickers}

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }
    base = "https://data.alpaca.markets/v2"
    result: dict[str, Optional[int]] = {}

    async with httpx.AsyncClient(timeout=15) as c:
        for ticker in tickers:
            try:
                r = await c.get(
                    f"{base}/stocks/{ticker[0]}/bars",
                    headers=headers,
                    params={"timeframe": "1Day", "limit": 30, "adjustment": "split"},
                )
                bars = r.json().get("bars", []) if r.status_code == 200 else []
                result[ticker[0]] = (
                    int(sum(b["v"] for b in bars) / len(bars)) if bars else None
                )
            except Exception:
                result[ticker[0]] = None

    return result


async def fetch_prices_for_tickers(
    tickers: List[tuple],
) -> dict[tuple, Optional[float]]:
    """Fetch current prices for multiple tickers concurrently."""
    import asyncio

    tasks = {t: get_current_price(t[0], t[1], t[2]) for t in tickers}
    fetched = await asyncio.gather(*tasks.values(), return_exceptions=True)
    return {
        ticker: (
            None
            if isinstance(result, Exception) or isinstance(result, BaseException)
            else result
        )
        for ticker, result in zip(tasks.keys(), fetched)
    }

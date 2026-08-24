"""
Position enrichment pipeline.

Takes raw backend position dicts and augments them with:
  - live price and average cost basis (local currency)
  - USD equivalents for display
  - unrealised P&L
  - days held, distance to validation / invalidation
  - portfolio weight (fraction of total cost basis)
  - computed TradingFriction

FX positions (ticker starts with "FX:") are silently filtered out —
they are FX hedges, not equity proposals.
"""

from datetime import datetime, timezone
from typing import Optional

from .currency import local_to_usd, usd_display
from .friction import compute_trading_friction


# ── Public entry point ──────────────────────────────────────────────────────────


async def enrich_positions(
    raw_positions: list[dict],
    capital: float,
) -> tuple[list[dict], dict[str, float], dict[str, float]]:
    """
    Enrich a list of raw backend positions.

    Returns:
        enriched   — list of augmented position dicts
        price_map  — {ticker: local_currency_price}
        size_map   — {ticker: position_size_usd}
    """
    # Filter out FX positions — not equity, breaks weight/price logic
    equity_positions = [
        p
        for p in raw_positions
        if not (
            str(p.get("stock", "")).startswith("FX:")
            or str(p.get("stock", "")).startswith("CASH:")
        )
    ]

    total_cost = sum(
        (p.get("quantity") or 0) * (p.get("avg_price") or 0) for p in equity_positions
    )

    enriched: list[dict] = []
    price_map: dict[str, float] = {}
    size_map: dict[str, float] = {}
    now = datetime.now(timezone.utc)

    for pos in equity_positions:
        result = await _enrich_one(pos, total_cost, now)
        enriched.append(result)
        ticker = result.get("ticker", "")
        if result.get("current_price") is not None:
            price_map[ticker] = result["current_price"]
        if result.get("position_size_usd") is not None:
            size_map[ticker] = result["position_size_usd"]

    return enriched, price_map, size_map


# ── Internal helpers ────────────────────────────────────────────────────────────


async def _enrich_one(pos: dict, total_cost: float, now: datetime) -> dict:
    stock = pos.get("stock", "")
    primary_exchange = pos.get("primary_exchange", "")
    currency = pos.get("currency", "")
    quantity = pos.get("quantity") or 0.0
    current_price = pos.get("current_price", 0.0)
    direction = "long" if quantity >= 0.0 else "short"

    # Weight = fraction of total portfolio cost basis
    weight = (
        (quantity * (pos.get("avg_price") or 0)) / total_cost if total_cost > 0 else 0.0
    )

    # currency = detect_currency({**pos, "exchange": pos.get("primary_exchange", "")})

    # current_price = await get_current_price(stock, primary_exchange, currency)
    avg_price = pos.get("avg_price", 0)

    days_held, opened_iso = _days_held(pos, now)
    pos_size_local = (quantity * current_price) if current_price is not None else None
    pos_size_usd = (
        local_to_usd(pos_size_local, currency) if pos_size_local is not None else None
    )

    pnl_pct, pnl_usd = _pnl(current_price, avg_price, direction, pos_size_usd)

    timing = pos.get("timing") or {}
    val_level = (timing.get("validation_condition") or {}).get("level")
    inv_level = (timing.get("invalidation_condition") or {}).get("level")
    dist_val = _dist(val_level, current_price, direction="to_val")
    dist_inv = _dist(inv_level, current_price, direction="to_inv")

    return {
        # Identity
        "ticker": stock,
        "stock": stock,  # kept for backward compat
        "exchange": pos.get("primary_exchange", ""),
        "primary_exchange": pos.get("primary_exchange", ""),
        "strategy": pos.get("strategy", "manual"),
        "direction": direction,
        "quantity": quantity,
        "currency": currency,
        # Weight — fraction of total cost basis
        "weight": round(weight, 6),
        "proposed_weight": round(weight, 6),
        # Prices (local currency)
        "current_price": current_price,
        "avg_cost_basis": avg_price,
        "avg_price": avg_price,  # kept for backward compat
        # USD display strings
        "current_price_usd_display": usd_display(current_price, currency),
        "avg_cost_basis_usd_display": usd_display(avg_price, currency),
        "position_size_local": pos_size_local,
        "position_size_usd": pos_size_usd,
        # P&L
        "unrealized_pnl_pct": round(pnl_pct * 100, 2) if pnl_pct is not None else None,
        "unrealized_pnl_usd": round(pnl_usd, 0) if pnl_usd is not None else None,
        # Timing
        "position_opened_date": opened_iso,
        "days_held": days_held,
        "last_updated": pos.get("last_updated"),
        "timing": timing,
        "dist_to_validation_pct": round(dist_val * 100, 2)
        if dist_val is not None
        else None,
        "dist_to_invalidation_pct": round(dist_inv * 100, 2)
        if dist_inv is not None
        else None,
        # Tags (filled by idea_generator; may be absent for legacy positions)
        "economy": pos.get("economy", ""),
        "industry": pos.get("industry", ""),
        "industry_file_key": pos.get("industry_file_key", ""),
        "conviction": pos.get("conviction"),
        "asset_type": pos.get("asset_type", "stock"),
        "drivers": pos.get("drivers", []),
        "industry_context": pos.get("industry_context"),
    }


def _days_held(pos: dict, now: datetime) -> tuple[Optional[int], Optional[str]]:
    raw = pos.get("last_updated", "")
    try:
        if isinstance(raw, str):
            opened = datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        else:
            opened = raw
        return (now - opened).days, opened.isoformat()
    except Exception:
        return None, None


def _pnl(
    current_price: Optional[float],
    avg_price: Optional[float],
    direction: str,
    pos_size_usd: Optional[float],
) -> tuple[Optional[float], Optional[float]]:
    if not (current_price and avg_price and avg_price > 0):
        return None, None
    raw = (current_price - avg_price) / avg_price
    pnl_pct = raw if direction == "long" else -raw
    pnl_usd = pnl_pct * pos_size_usd if pos_size_usd else None
    return pnl_pct, pnl_usd


def _dist(
    level: Optional[float],
    current_price: Optional[float],
    direction: str,
) -> Optional[float]:
    """
    Returns signed distance as a fraction of current price.
    direction="to_val":  positive = price hasn't reached level yet
    direction="to_inv":  positive = price is above invalidation (safe)
    """
    if not (level and current_price):
        return None
    if direction == "to_val":
        return (level - current_price) / current_price
    else:  # to_inv
        return (current_price - level) / current_price


# ── Convert enriched dict → ProposedPosition-compatible dict ───────────────────


def enriched_to_proposed(
    pos: dict,
    adv_map: dict[str, Optional[int]],
    price_map: dict[str, float],
) -> Optional[dict]:
    """
    Map an enriched position dict to a ProposedPosition-compatible dict.
    Used for unchanged_positions and trimmed_positions in the final proposal
    so that PositionsProposal.model_validate() succeeds.

    Fields that have no equivalent in an enriched position (drivers, timing
    validation/invalidation levels, industry_context) are reconstructed from
    stored data or filled with safe defaults.
    """
    ticker = pos.get("ticker") or pos.get("stock", "")
    if not ticker:
        return None

    current_price = pos.get("current_price")
    pos_size_usd = pos.get("position_size_usd") or 0.0
    adv = adv_map.get(ticker)
    asset_type = pos.get("asset_type", "stock")
    direction = pos.get("direction", "long")
    weight = float(pos.get("proposed_weight") or pos.get("weight") or 0.0)
    economy = pos.get("economy") or "us"
    industry = pos.get("industry") or "unknown"
    industry_file_key = pos.get("industry_file_key") or f"{economy}_{industry}"

    # Friction — use real adv now that it's available
    if current_price and pos_size_usd > 0:
        friction = compute_trading_friction(
            ticker, asset_type, pos_size_usd, current_price, adv
        )
        friction_dict = friction.model_dump()
    else:
        from models.types import TradingFriction

        friction_dict = TradingFriction(
            estimated_shares_or_contracts=0,
            commission_usd=0.0,
            estimated_slippage_usd=0.0,
            total_friction_usd=0.0,
            friction_as_pct_of_position=0.0,
            round_trip_friction_usd=0.0,
            round_trip_friction_pct=0.0,
        ).model_dump()

    # Timing — reconstruct from stored timing or use ±10% defaults
    timing_raw = pos.get("timing") or {}
    val_raw = timing_raw.get("validation_condition") or {}
    inv_raw = timing_raw.get("invalidation_condition") or {}

    timing_dict = {
        "horizon_days": timing_raw.get("horizon_days", 30),
        "catalyst_date": timing_raw.get("catalyst_date"),
        "validation_condition": {
            "level": val_raw.get("level")
            or (current_price * 1.10 if current_price else 0.0),
            "rationale": val_raw.get("rationale", "Original thesis target"),
            "action": val_raw.get("action", "trim"),
            "signal_type": val_raw.get("signal_type", "technical"),
        },
        "invalidation_condition": {
            "level": inv_raw.get("level")
            or (current_price * 0.90 if current_price else 0.0),
            "rationale": inv_raw.get("rationale", "Original thesis stop"),
            "action": inv_raw.get("action", "close"),
            "signal_type": inv_raw.get("signal_type", "technical"),
        },
        "price_corridor_rationale": timing_raw.get(
            "price_corridor_rationale", "Carried from original position"
        ),
        "monitoring_checklist": timing_raw.get("monitoring_checklist", []),
    }

    drivers = pos.get("drivers") or [
        {
            "title": "Carried position",
            "description": "Position carried from prior proposal — no new driver assessed.",
            "type": "fundamental",
            "overlooked_reason": None,
        }
    ]

    industry_context = pos.get("industry_context") or {
        "economy": economy,
        "industry": industry,
        "industry_file_key": industry_file_key,
        "headwind": None,
        "tailwind": None,
        "macro_linkage": "Carried from prior position — macro context not re-evaluated.",
    }

    return {
        "ticker": ticker,
        "exchange": pos.get("exchange") or pos.get("primary_exchange", ""),
        "primary_exchange": pos.get("primary_exchange", ""),
        "currency": pos.get("currency", ""),
        "direction": direction,
        "asset_type": asset_type,
        "proposed_weight": weight,
        "current_weight": weight,
        "conviction": pos.get("conviction") or 2,
        "drivers": drivers,
        "economy": economy,
        "industry": industry,
        "industry_file_key": industry_file_key,
        "industry_context": industry_context,
        "timing": timing_dict,
        "position_state": "hold",
        "displaced_ticker": None,
        "why_better_than_displaced": None,
        "friction_estimate": friction_dict,
        "friction_justification": (
            f"Carried position — no new transaction. "
            f"Existing RT friction ~${friction_dict.get('round_trip_friction_usd', 0):.2f} if exited."
        ),
        "option_expiry": pos.get("option_expiry"),
        "option_strike": pos.get("option_strike"),
        "option_vs_stock_rationale": None,
        "option_greeks_context": None,
        "option_monitoring": None,
        # Pass-through enrichment metadata (not on schema — kept for display)
        "_enrichment": {
            "current_price": current_price,
            "avg_cost_basis": pos.get("avg_cost_basis"),
            "position_size_usd": pos_size_usd,
            "unrealized_pnl_pct": pos.get("unrealized_pnl_pct"),
            "unrealized_pnl_usd": pos.get("unrealized_pnl_usd"),
            "days_held": pos.get("days_held"),
            "current_price_usd_display": pos.get("current_price_usd_display"),
        },
        "_assembly_weight_rationale": pos.get("_assembly_weight_rationale", ""),
    }

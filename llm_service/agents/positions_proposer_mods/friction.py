"""
IBKR Pro Fixed Tier — transaction cost model.

Computes one-way and round-trip friction for equity and options trades:
  commission  = IBKR Pro fixed schedule
  slippage    = half-spread × market-impact multiplier × position size
"""

from models.types import TradingFriction

# ── Spread lookup by liquidity tier ────────────────────────────────────────────
SPREAD_BPS = {"large_cap": 2, "mid_cap": 8, "small_cap": 20}

MARKET_IMPACT_MULTIPLIER = {"minimal": 0.5, "moderate": 1.5, "significant": 3.0}


def _spread_tier(adv: int | None) -> str:
    if adv is None:
        return "mid_cap"
    if adv > 5_000_000:
        return "large_cap"
    if adv > 500_000:
        return "mid_cap"
    return "small_cap"


def _impact_mult(shares: int, adv: int | None) -> float:
    if not adv:
        return MARKET_IMPACT_MULTIPLIER["moderate"]
    ratio = shares / adv
    if ratio < 0.01:
        return MARKET_IMPACT_MULTIPLIER["minimal"]
    if ratio < 0.05:
        return MARKET_IMPACT_MULTIPLIER["moderate"]
    return MARKET_IMPACT_MULTIPLIER["significant"]


def _equity_commission(shares: int, price: float) -> float:
    """IBKR Pro fixed: $0.005/share, min $1, max 1% of trade value."""
    return max(1.0, min(shares * 0.005, shares * price * 0.01))


def _options_commission(contracts: int) -> float:
    """IBKR Pro fixed: $0.65/contract, min $1."""
    return max(1.0, contracts * 0.65)


def compute_trading_friction(
    ticker: str,
    asset_type: str,
    position_size_usd: float,
    current_price: float,
    adv: int | None,
    contracts: int | None = None,
) -> TradingFriction:
    """Return a TradingFriction for one side; round-trip = 2×."""
    tier = _spread_tier(adv)
    spread_bps = SPREAD_BPS[tier]

    if asset_type == "stock":
        shares = int(position_size_usd / current_price) if current_price > 0 else 0
        commission = _equity_commission(shares, current_price)
        slippage = (
            (spread_bps / 10_000) * 0.5 * _impact_mult(shares, adv) * position_size_usd
        )
        n = shares
    else:
        n = contracts or max(1, int(position_size_usd / (current_price * 100)))
        commission = _options_commission(n)
        slippage = (spread_bps / 10_000) * 3.0 * position_size_usd

    one_way = commission + slippage
    pct_one_way = one_way / position_size_usd if position_size_usd > 0 else 0.0

    if tier not in ['large_cap', 'mid_cap', 'small_cap']:
        print("tier produced not one of ['large_cap', 'mid_cap', 'small_cap']")

    return TradingFriction(
        estimated_shares_or_contracts=n,
        commission_usd=round(commission, 2),
        estimated_slippage_usd=round(slippage, 2),
        total_friction_usd=round(one_way, 2),
        friction_as_pct_of_position=round(pct_one_way, 6),
        round_trip_friction_usd=round(one_way * 2, 2),
        round_trip_friction_pct=round(pct_one_way * 2, 6),
        ibkr_tier="pro_fixed",
        adv_used=adv,
        spread_tier=tier,
    )


def friction_summary_line(
    ticker: str,
    pos_usd: float | None,
    price: float | None,
    adv: int | None,
    asset_type: str = "stock",
) -> str:
    """Single-line friction description for prompt context."""
    if pos_usd and price and pos_usd > 0 and price > 0:
        f = compute_trading_friction(ticker, asset_type, pos_usd, price, adv)
        adv_str = f"{adv:,}" if adv else "unknown"
        return (
            f"  {ticker}: pos ${pos_usd:,.0f} | RT cost ${f.round_trip_friction_usd:,.0f} "
            f"({f.round_trip_friction_pct * 100:.3f}%) | ADV {adv_str} | tier {f.spread_tier}"
        )
    return f"  {ticker}: insufficient data for friction estimate"

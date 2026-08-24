"""
Text formatters that convert data structures into prompt-ready strings.

These are used exclusively to build LLM prompt context — they are not
used for API responses or user-facing output.
"""

from typing import Optional

from tools import kb_manager


# ── Formatting helpers ──────────────────────────────────────────────────────────


def _fmt_price(val: Optional[float]) -> str:
    return f"${val:,.2f}" if val is not None else "n/a"


def _fmt_pct(val: Optional[float], sign: bool = False) -> str:
    if val is None:
        return "n/a"
    return f"{'+' if sign and val >= 0 else ''}{val:.1f}%"


def _fmt_usd(val: Optional[float], sign: bool = False) -> str:
    if val is None:
        return "n/a"
    return f"${'+' if sign and val >= 0 else ''}{val:,.0f}"


# ── Position block ──────────────────────────────────────────────────────────────


def format_positions(positions: list[dict]) -> str:
    """Render enriched positions as a human-readable block for prompt context."""
    if not positions:
        return "(No current positions)"
    return "\n\n".join(_position_block(p) for p in positions)


def _position_block(p: dict) -> str:
    ticker = p.get("ticker", "??")
    direction = p.get("direction", "?")
    weight = p.get("proposed_weight", p.get("weight", 0.0))
    economy = p.get("economy", "unknown")
    industry = p.get("industry", "unknown")
    currency = p.get("currency", "USD")
    current_price = p.get("current_price")
    avg_price = p.get("avg_cost_basis")
    pnl_pct = p.get("unrealized_pnl_pct")
    pnl_usd = p.get("unrealized_pnl_usd")
    days_held = p.get("days_held")
    dist_val = p.get("dist_to_validation_pct")
    dist_inv = p.get("dist_to_invalidation_pct")
    pos_usd = p.get("position_size_usd")
    cp_display = p.get("current_price_usd_display", "n/a")
    avg_display = p.get("avg_cost_basis_usd_display", "n/a")

    timing = p.get("timing") or {}
    validation = (
        (timing.get("validation_condition") or {}) if isinstance(timing, dict) else {}
    )
    invalidation = (
        (timing.get("invalidation_condition") or {}) if isinstance(timing, dict) else {}
    )
    horizon = timing.get("horizon_days") if isinstance(timing, dict) else None
    catalyst = timing.get("catalyst_date") if isinstance(timing, dict) else None
    checklist = (
        timing.get("monitoring_checklist", []) if isinstance(timing, dict) else []
    )[:3]

    alerts = []
    if dist_inv is not None and 0 <= dist_inv <= 5:
        alerts.append("⚠️ NEAR INVALIDATION")
    if dist_val is not None and 0 <= dist_val <= 5:
        alerts.append("🎯 NEAR VALIDATION")
    if dist_inv is not None and dist_inv < 0:
        alerts.append("🔴 INVALIDATION BREACHED")
    if dist_val is not None and dist_val < 0:
        alerts.append("✅ VALIDATION BREACHED")
    if horizon is not None and days_held is not None and days_held >= horizon:
        alerts.append("⏰ HORIZON ELAPSED")

    alert_str = (" | " + ", ".join(alerts)) if alerts else ""

    return (
        f"┌─ {ticker} ({direction.upper()}) — {weight * 100:.1f}% NAV"
        f" ≈ {_fmt_usd(pos_usd)}{alert_str}\n"
        f"│  Economy: {economy} | Industry: {industry} | Currency: {currency}\n"
        f"│  Current: {_fmt_price(current_price)} {currency} ({cp_display} USD equiv)"
        f"  |  Cost: {_fmt_price(avg_price)} {currency} ({avg_display} USD equiv)"
        f"  |  Days: {days_held if days_held is not None else 'n/a'}\n"
        f"│  P&L: {_fmt_pct(pnl_pct, sign=True)} ({_fmt_usd(pnl_usd, sign=True)})\n"
        f"│  Horizon: {horizon if horizon is not None else 'n/a'}d"
        f" | Catalyst: {catalyst or 'n/a'}\n"
        f"│  Validation   → {_fmt_price(validation.get('level'))}"
        f" ({_fmt_pct(dist_val, sign=True)} away)\n"
        f"│  Invalidation → {_fmt_price(invalidation.get('level'))}"
        f" ({_fmt_pct(dist_inv, sign=True)} away)\n"
        f"└─ Monitoring: {', '.join(checklist)}"
    )


# ── Ideas formatting ────────────────────────────────────────────────────────────


def parse_ideas(latest_ideas_raw: str) -> list[dict]:
    """Delegates to the canonical parser in kb_manager."""
    return kb_manager.parse_ideas_output(latest_ideas_raw)


def format_ideas_index(ideas: list[dict]) -> str:
    """Compact one-liner per idea — used in Stage 2 macro prompt."""
    if not ideas:
        return "(No ideas available — run idea_generator first)"
    return "\n".join(
        f"  {i.get('ticker', '?')} | {i.get('economy', '?')} | "
        f"{i.get('industry_file_key', i.get('industry', '?'))} | "
        f"bias={i.get('direction', 'long')} | "
        f"conviction={i.get('conviction_preliminary', '?')} | "
        f"{i.get('one_line_thesis', '')}"
        for i in ideas
    )


def format_ideas_for_economy(ideas: list[dict], economy: str) -> str:
    """Detailed multi-line format for one economy — used in Stage 3 prompt."""
    filtered = [i for i in ideas if i.get("economy") == economy]
    if not filtered:
        return f"(No ideas for economy: {economy})"
    lines = []
    for i in filtered:
        lines.append(
            f"  {i.get('ticker', '?')} [{i.get('industry_file_key', '?')}] "
            f"| {i.get('direction', 'long')} | conviction={i.get('conviction_preliminary', '?')}\n"
            f"    {i.get('one_line_thesis', '')}\n"
            f"    Catalyst: {i.get('key_catalyst', 'n/a')} ({i.get('time_horizon_days', '?')}d)\n"
            f"    Invalidation: {i.get('invalidation', 'n/a')}"
        )
    return "\n\n".join(lines)


def format_idea_detail(idea: dict) -> str:
    """Full idea detail — used in Stage 4 prompt."""
    lines = [
        f"**{idea.get('ticker')}** — {idea.get('name', '')}",
        f"Direction: {idea.get('direction')} | Economy: {idea.get('economy')} | "
        f"Industry: {idea.get('industry_file_key', idea.get('industry'))}",
        f"Conviction: {idea.get('conviction_preliminary')} | "
        f"Heuristic: {idea.get('heuristic_triggered', '')}",
        f"Thesis: {idea.get('one_line_thesis', '')}",
        f"Catalyst: {idea.get('key_catalyst', 'n/a')} | Horizon: {idea.get('time_horizon_days', '?')}d",
        f"Overlooked: {idea.get('overlooked_reason', 'n/a')}",
        f"Invalidation: {idea.get('invalidation', 'n/a')}",
        "Evidence:",
        *[f"  - {ev}" for ev in idea.get("supporting_evidence", [])],
    ]
    related = idea.get("related_tickers", [])
    if related:
        lines.append(f"Related tickers: {', '.join(related)}")
    return "\n".join(lines)


# ── Assembly prompt helpers ─────────────────────────────────────────────────────


def format_candidate_for_assembly(trade: dict, capital: float) -> str:
    ticker = trade.get("ticker", "??")
    direction = trade.get("direction", "long")
    economy = trade.get("economy", "?")
    ind = trade.get("industry_file_key", trade.get("industry", "?"))
    conviction = trade.get("conviction", "?")
    timing = trade.get("timing") or {}
    horizon = timing.get("horizon_days", trade.get("horizon_days", "n/a"))
    catalyst = timing.get("catalyst_date", trade.get("catalyst_date", "n/a"))
    val_level = (timing.get("validation_condition") or {}).get("level")
    inv_level = (timing.get("invalidation_condition") or {}).get("level")
    price = trade.get("current_price")
    fe = trade.get("friction_estimate") or {}
    rt_usd = fe.get("round_trip_friction_usd") or 0.0
    rt_pct = fe.get("round_trip_friction_pct") or 0.0
    one_line = trade.get("one_line_thesis", "")

    upside_str = "upside n/a"
    if val_level and price and price > 0:
        upside_str = f"{(val_level - price) / price * 100:+.1f}% to validation (${val_level:,.2f})"

    inv_str = "invalidation n/a"
    if inv_level and price and price > 0:
        inv_str = f"{(price - inv_level) / price * 100:.1f}% to invalidation (${inv_level:,.2f})"

    cap_weight = trade.get("proposed_weight") or 0.10
    cap_pos_usd = capital * cap_weight
    coverage_str = "coverage n/a"
    if rt_usd > 0 and cap_pos_usd > 0 and val_level and price and price > 0:
        gain = (val_level - price) / price * cap_pos_usd
        coverage_str = (
            f"coverage {gain / rt_usd:.1f}× at {cap_weight * 100:.0f}% weight"
        )

    recon = trade.get("_reconciliation") or {}
    recon_note = (
        f"\n    [Reconciled: {recon.get('decision', '')}] {recon.get('winning_rationale', '')[:100]}"
        if recon
        else ""
    )

    return (
        f"  CANDIDATE {ticker} ({direction}) | {economy}/{ind} | conviction={conviction}\n"
        f"    Horizon: {horizon}d | Catalyst: {catalyst}\n"
        f"    {upside_str} | {inv_str}\n"
        f"    RT friction: ${rt_usd:,.0f} ({rt_pct * 100:.3f}%) | {coverage_str}\n"
        f"    Thesis: {one_line[:120] if one_line else 'n/a'}"
        f"{recon_note}"
    )


def format_trimmed_for_assembly(p: dict) -> str:
    ticker = p.get("ticker", "??")
    economy = p.get("economy", "?")
    ind = p.get("industry_file_key", p.get("industry", "?"))
    conviction = p.get("conviction", "?")
    current_w = float(p.get("proposed_weight") or p.get("weight") or 0.0)
    pnl_pct = p.get("unrealized_pnl_pct")
    days_held = p.get("days_held")
    dist_inv = p.get("dist_to_invalidation_pct")
    dist_val = p.get("dist_to_validation_pct")
    one_line = p.get("one_line_thesis", p.get("thesis", "n/a"))

    pnl_str = f" | P&L {pnl_pct:+.1f}%" if pnl_pct is not None else ""
    inv_str = (
        f"{dist_inv:+.1f}% to invalidation"
        if dist_inv is not None
        else "invalidation n/a"
    )
    val_str = (
        f"{dist_val:+.1f}% to validation" if dist_val is not None else "validation n/a"
    )

    return (
        f"  TRIM {ticker} | {economy}/{ind} | conviction={conviction} | "
        f"current_weight={current_w * 100:.1f}%{pnl_str} | days_held={days_held}\n"
        f"    {val_str} | {inv_str}\n"
        f"    Thesis: {one_line[:120] if one_line else 'n/a'}"
    )


def format_alerts_for_assembly(alerts: list[dict]) -> str:
    if not alerts:
        return "(No Stage 1 alerts)"
    return "\n".join(
        f"  ⚠️  {a.get('ticker')} | severity={a.get('severity')} | "
        f"type={a.get('alert_type')} | action={a.get('recommended_action')}\n"
        f"     {a.get('description', '')[:150]}"
        for a in alerts
    )

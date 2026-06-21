"""
positions_proposer.py — Pipeline orchestrator.

Runs the 4-stage hierarchical drill-down, deduplication, and final assembly.
Heavy logic lives in the submodules; this file is intentionally thin.

Pipeline:
  Stage 1  → Portfolio audit (alerts, state summary)
  Stage 2  → Macro exposure decisions
  Stage 3  → Industry exposure decisions (parallel, per flagged economy)
  Stage 4  → Company selection (parallel, per flagged industry)
  Dedup    → Reconcile tickers that appeared in multiple Stage 4 calls
  Assembly → Allocate freed budget to trimmed + new positions
"""

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Optional

from models.settings_manager import load_settings
from models.types import (
    CounterProposal,
    PipelineStages,
    PositionsProposal,
    Stage1AuditSummary,
    Stage4CompanySummary,
)
from tools import kb_manager
from tools.field_corrector import correct_fields

from agents.positions_proposer_mods.backend import (
    fetch_adv,
    fetch_current_positions,
    fetch_prices_for_tickers,
    get_capital_level,
)
from agents.positions_proposer_mods.enrichment import (
    enrich_positions,
    enriched_to_proposed,
)
from agents.positions_proposer_mods.formatters import parse_ideas
from agents.positions_proposer_mods.friction import compute_trading_friction
from agents.positions_proposer_mods.stages import (
    apply_assembly_to_portfolio,
    deduplicate_proposed_trades,
    run_final_assembly,
    run_stage1,
    run_stage2,
    run_stage3_for_economy,
    run_stage4_for_industry,
)

# Cache: skip if run < 1 hour ago and a proposal already exists
_last_run = datetime.now(timezone.utc)

# ── helpers ──────────────────────────────────────────────────────────────────────


def _contract_tuples_from_positions(
    enriched_positions: list[dict],
) -> list[tuple[str, str, str]]:
    """
    Build contract tuples for every position that carries a ticker.
    enriched_positions come from IBKR and already have primary_exchange + currency.
    """
    return [
        (
            p["ticker"],
            p.get("primary_exchange", ""),
            p.get("currency", "USD"),
        )
        for p in enriched_positions
        if p.get("ticker")
    ]


def _contract_tuples_from_idea_ticker_objs(
    objs: list[dict],
    already_in_price_map: set[str],
) -> list[tuple[str, str, str]]:
    """
    Build contract tuples for idea tickers not yet in price_map.
    Each obj is {"ticker": ..., "primary_exchange": ..., "currency": ...}
    as emitted by Stage 3.
    """
    seen: set[str] = set()
    result: list[tuple[str, str, str]] = []
    for obj in objs:
        ticker = obj.get("ticker", "")
        if not ticker or ticker in already_in_price_map or ticker in seen:
            continue
        seen.add(ticker)
        result.append(
            (ticker, obj.get("primary_exchange", ""), obj.get("currency", "USD"))
        )
    return result


# ── Public entry point ───────────────────────────────────────────────────────────


async def run(options_mode_override=None, force=False) -> dict:
    global _last_run
    now = datetime.now(timezone.utc)

    if not force and (now - _last_run).total_seconds() < 3600:
        cached = await kb_manager.read_proposal()
        if cached not in ("", None):
            return _parse_proposal(cached)

    settings = await load_settings()
    options_mode = (
        options_mode_override
        if options_mode_override is not None
        else settings.options_mode
    )

    # ── 1. Fetch capital and raw positions ──────────────────────────────────────
    capital = await get_capital_level()
    raw_positions = await fetch_current_positions()
    print(f"[run] Fetched {len(raw_positions)} current positions")

    # ── 2. Enrich positions (FX positions filtered inside enrich_positions) ─────
    enriched_current, price_map, size_map = await enrich_positions(
        raw_positions, capital
    )
    print(f"[run] Enriched {len(enriched_current)} positions (FX filtered)")

    # ── 3. ADV for current holdings ─────────────────────────────────────────────
    #   enrich_positions already called fetch_prices_for_tickers internally and
    #   returns price_map keyed by bare ticker.  For ADV we build contract tuples
    #   from the enriched positions so the fetcher can route correctly.
    current_contract_tuples = _contract_tuples_from_positions(enriched_current)
    adv_map = await fetch_adv(current_contract_tuples)

    # ── 4. Load ideas ────────────────────────────────────────────────────────────
    ideas = parse_ideas(await kb_manager.read_latest_ideas())

    # ── Stage 1: Portfolio audit ─────────────────────────────────────────────────
    stage1_result = await run_stage1(enriched_current)
    triggered_alerts = stage1_result.get("triggered_alerts", [])
    if not isinstance(triggered_alerts, list):
        triggered_alerts = []

    stage1_audit = Stage1AuditSummary(
        positions_reviewed=[p.get("ticker", "?") for p in enriched_current],
        alerts_generated=len(triggered_alerts),
    )

    # ── Stage 2: Macro profile ───────────────────────────────────────────────────
    macro_decisions = await run_stage2(
        stage1_result=stage1_result,
        ideas=ideas,
        capital=capital,
        enriched_positions=enriched_current,
        price_map=price_map,
        adv_map=adv_map,
    )
    print(
        f"[run] Macro decisions: { {d.economy: d.decision for d in macro_decisions} }"
    )

    active_economies = [
        d for d in macro_decisions if d.decision in ("explore", "reduce")
    ]

    # ── Stage 3: Industry decisions (parallel across active economies) ───────────
    stage3_results = await asyncio.gather(
        *[
            run_stage3_for_economy(
                economy=d.economy,
                macro_rationale=d.rationale,
                ideas=ideas,
                enriched_positions=enriched_current,
            )
            for d in active_economies
        ],
        return_exceptions=True,
    )

    all_industry_decisions = []
    stage3_block: dict = {}
    for i, result in enumerate(stage3_results):
        economy = active_economies[i].economy
        if isinstance(result, Exception) or isinstance(result, BaseException):
            print(f"[run] Stage 3 failed for {economy}: {result}")
            stage3_block[economy] = {"error": str(result)}
            continue
        all_industry_decisions.extend(result)
        stage3_block[economy] = {ind.industry_file_key: ind.decision for ind in result}

    active_industries = [
        d for d in all_industry_decisions if d.decision in ("explore", "reduce")
    ]

    # ── Pre-fetch prices + ADV for all idea tickers before Stage 4 runs ─────────
    #   relevant_idea_tickers is now a list[dict] with ticker/primary_exchange/currency.
    #   Build contract tuples for anything not yet in price_map.
    idea_ticker_objs_needed = [
        obj
        for ind_dec in active_industries
        for obj in (ind_dec.relevant_idea_tickers or [])
        if obj.get("ticker") and obj["ticker"] not in price_map
    ]

    if idea_ticker_objs_needed:
        # De-duplicate by ticker before fetching
        seen_tickers: set[str] = set()
        unique_idea_contract_tuples: list[tuple[str, str, str]] = []
        for obj in idea_ticker_objs_needed:
            ticker = obj["ticker"]
            if ticker not in seen_tickers:
                seen_tickers.add(ticker)
                unique_idea_contract_tuples.append(
                    (
                        ticker,
                        obj.get("primary_exchange", ""),
                        obj.get("currency", "USD"),
                    )
                )

        idea_prices = await fetch_prices_for_tickers(unique_idea_contract_tuples)
        price_map.update(idea_prices)

        idea_adv = await fetch_adv(unique_idea_contract_tuples)
        adv_map.update(idea_adv)

    # ── Stage 4: Company selection (parallel across active industries) ───────────
    stage4_results = await asyncio.gather(
        *[
            run_stage4_for_industry(
                industry_decision=ind_dec,
                ideas=ideas,
                enriched_positions=enriched_current,
                price_map=price_map,
                adv_map=adv_map,
                capital=capital,
                settings=settings,
                options_mode=options_mode,
            )
            for ind_dec in active_industries
        ],
        return_exceptions=True,
    )

    all_proposed_trades_raw: list[dict] = []
    friction_cleared_all: list[str] = []
    friction_failed_all: list[str] = []

    for i, result in enumerate(stage4_results):
        ind_key = active_industries[i].industry_file_key
        if isinstance(result, Exception) or isinstance(result, BaseException):
            print(f"[run] Stage 4 failed for {ind_key}: {result}")
            continue

        for trade in result.get("proposed_trades", []):
            # Recompute friction using live price_map + adv_map so adv_used is never null
            _attach_friction(trade, price_map, adv_map, capital)
            all_proposed_trades_raw.append(trade)

        friction_cleared_all.extend(result.get("friction_cleared", []))
        friction_failed_all.extend(result.get("friction_failed", []))

    # ── Deduplication ────────────────────────────────────────────────────────────
    all_proposed_trades = await deduplicate_proposed_trades(all_proposed_trades_raw)
    # print(f"[ALL PROPOSED TRADES]: {all_proposed_trades}")

    # ── Categorise current positions ─────────────────────────────────────────────
    proposed_tickers = {t.get("ticker") for t in all_proposed_trades}
    close_alert_tickers = {
        a.get("ticker")
        for a in triggered_alerts
        if a.get("recommended_action") == "close"
    }
    trim_alert_tickers = {
        a.get("ticker")
        for a in triggered_alerts
        if a.get("recommended_action") == "trim"
    }

    # Unchanged: no alert, not being replaced
    unchanged_positions = [
        p
        for p in enriched_current
        if p.get("ticker") not in proposed_tickers
        and p.get("ticker") not in close_alert_tickers
        and p.get("ticker") not in trim_alert_tickers
    ]
    # Trimmed: flagged for trim but not displaced by a new trade
    trimmed_positions = [
        p
        for p in enriched_current
        if p.get("ticker") in trim_alert_tickers
        and p.get("ticker") not in proposed_tickers
    ]
    # Closed: urgent close alerts not superseded by a new trade
    effectively_closed_tickers: set[str] = close_alert_tickers - proposed_tickers
    removed_positions = list(effectively_closed_tickers)

    # ── Final Assembly ────────────────────────────────────────────────────────────
    assembly = await run_final_assembly(
        proposed_trades=all_proposed_trades,
        unchanged_positions=unchanged_positions,
        trimmed_positions=trimmed_positions,
        closed_tickers=effectively_closed_tickers,
        triggered_alerts=triggered_alerts,
        capital=capital,
        settings=settings,
    )

    locked_weight = sum(
        float(p.get("proposed_weight") or p.get("weight") or 0.0)
        for p in unchanged_positions
    )
    final_trades, final_unchanged, final_trimmed, total_friction_usd = (
        apply_assembly_to_portfolio(
            assembly=assembly,
            proposed_trades=all_proposed_trades,
            unchanged_positions=unchanged_positions,
            trimmed_positions=trimmed_positions,
            capital=capital,
        )
    )

    # ── Convert unchanged/trimmed to ProposedPosition-compatible dicts ───────────
    #    This is the fix for the schema mismatch: raw enriched dicts have wrong field
    #    names and missing required fields.  enriched_to_proposed() maps them correctly.
    final_unchanged_mapped = [
        m
        for p in final_unchanged
        if (m := enriched_to_proposed(p, adv_map, price_map)) is not None
    ]
    final_trimmed_mapped = [
        m
        for p in final_trimmed
        if (m := enriched_to_proposed(p, adv_map, price_map)) is not None
    ]

    # ── Build pipeline audit trail ────────────────────────────────────────────────
    pipeline_stages = PipelineStages(
        stage1_audit=stage1_audit,
        stage2_macro={
            "economies_considered": [d.economy for d in macro_decisions],
            "decisions": {d.economy: d.decision for d in macro_decisions},
            "macro_decisions": [d.model_dump() for d in macro_decisions],
        },
        stage3_industry=stage3_block,
        stage4_companies=Stage4CompanySummary(
            explored=[
                t
                for ind in active_industries
                for t in (ind.relevant_idea_tickers or [])
            ],
            friction_cleared=friction_cleared_all,
            friction_failed=friction_failed_all,
        ),
    )

    # ── Verify weight sum ─────────────────────────────────────────────────────────
    all_final = final_unchanged_mapped + final_trimmed_mapped + final_trades
    weight_sum = sum(
        float(p.get("proposed_weight") or p.get("weight") or 0.0) for p in all_final
    )
    if abs(weight_sum - 1.0) > 0.001:
        print(f"[run] WARNING: final weight sum = {weight_sum:.6f}")

    total_friction_pct = total_friction_usd / capital if capital > 0 else 0.0

    # ── Assemble proposal ─────────────────────────────────────────────────────────
    proposal = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "capital_at_proposal": capital,
        "triggered_alerts": triggered_alerts,
        "proposed_trades": final_trades,
        "unchanged_positions": final_unchanged_mapped,
        "trimmed_positions": final_trimmed_mapped,
        "removed_positions": removed_positions,
        "weight_sum_check": round(weight_sum, 6),
        "assembly_pool_summary": assembly.get("pool_summary", {}),
        "candidate_comparisons": assembly.get("candidate_comparisons", []),
        "portfolio_thesis": _derive_portfolio_thesis(macro_decisions, final_trades),
        "macro_backdrop": _derive_macro_backdrop(macro_decisions),
        "total_estimated_friction_usd": round(total_friction_usd, 2),
        "total_friction_as_pct_nav": round(total_friction_pct, 6),
        "pipeline_stages": pipeline_stages.model_dump(),
    }

    _last_run = now
    final_proposal = correct_fields(proposal, PositionsProposal) or proposal
    await kb_manager.write_proposal(json.dumps(final_proposal, indent=2))
    return final_proposal


# ── Private helpers ───────────────────────────────────────────────────────────────


def _attach_friction(
    trade: dict,
    price_map: dict,
    adv_map: dict,
    capital: float,
) -> None:
    """
    Recompute and overwrite trade['friction_estimate'] using live price_map + adv_map.
    This ensures adv_used is never null when ADV data is available.
    """
    ticker = trade.get("ticker", "")
    price = price_map.get(ticker)
    adv = adv_map.get(ticker)  # populated for idea tickers before Stage 4
    weight = trade.get("proposed_weight") or 0.12
    pos_usd = capital * weight
    asset_type = trade.get("asset_type", "stock")

    if price and pos_usd > 0:
        friction = compute_trading_friction(ticker, asset_type, pos_usd, price, adv)
        trade["friction_estimate"] = friction.model_dump()


def _derive_portfolio_thesis(macro_decisions, proposed_trades: list[dict]) -> str:
    maintain = [d.economy for d in macro_decisions if d.decision == "maintain"]
    explore = [d.economy for d in macro_decisions if d.decision == "explore"]
    reduce = [d.economy for d in macro_decisions if d.decision == "reduce"]
    parts = []
    if maintain:
        parts.append(f"Maintaining existing exposure in {', '.join(maintain)}.")
    if explore and proposed_trades:
        tickers = [t.get("ticker", "?") for t in proposed_trades[:3]]
        parts.append(
            f"Adding exposure via {', '.join(tickers)} in {', '.join(explore)}."
        )
    if reduce:
        parts.append(
            f"Reducing exposure in {', '.join(reduce)} on thesis deterioration."
        )
    return " ".join(parts) or "Portfolio held unchanged — no macro rotation warranted."


def _derive_macro_backdrop(macro_decisions) -> str:
    if not macro_decisions:
        return "Macro assessment not available."
    return " | ".join(
        f"{d.economy.upper()}: {d.rationale}" for d in macro_decisions[:3]
    )


def _parse_proposal(raw: str, capital: Optional[float] = None) -> dict:
    clean = re.sub(r"```(?:json)?", "", raw).strip()
    try:
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        result = json.loads(match.group() if match else clean)
    except Exception:
        return {}
    if capital is not None:
        result["capital_at_proposal"] = capital
    return result


async def fetch_target_positions() -> Optional[CounterProposal]:
    return await kb_manager.read_latest_counter_proposal()

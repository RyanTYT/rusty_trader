"""
Pipeline stage execution functions.

Each function is a self-contained LLM call + JSON parse step.
They are called in sequence by the orchestrator (positions_proposer.py).

Stage 1 — Portfolio audit (alerts + state summary)
Stage 2 — Macro profile decision (per economy)
Stage 3 — Industry decision (per flagged economy)
Stage 4 — Company selection (per flagged industry)
Dedup    — LLM reconciliation when the same ticker surfaces in 2 Stage 4 calls
Assembly — Freed-budget allocation across trimmed + new trades

Price-fetch contract convention
────────────────────────────────
Wherever tickers are passed to fetch_prices_for_tickers or fetch_adv, they are
3-tuples: (ticker: str, primary_exchange: str, currency: str).

For positions that came from enriched_positions the three fields are already present
on the dict.  For idea tickers the information originates from the ideas KB (each idea
must carry `primary_exchange` and `currency`) and is preserved through Stage 3's
`relevant_idea_tickers` list, which the LLM now emits as objects rather than bare
strings.
"""

import asyncio
import json
import re
from datetime import datetime, timezone

from models.types import IndustryDecision, MacroDecision
from tools import kb_manager, llm_client

from .formatters import (
    format_alerts_for_assembly,
    format_candidate_for_assembly,
    format_idea_detail,
    format_ideas_for_economy,
    format_ideas_index,
    format_positions,
    format_trimmed_for_assembly,
)
from .friction import compute_trading_friction, friction_summary_line
from .prompts import (
    FINAL_ASSEMBLY_SYSTEM_PROMPT,
    OPTIONS_ADDENDUM,
    STAGE1_SYSTEM_PROMPT,
    STAGE2_SYSTEM_PROMPT,
    STAGE3_SYSTEM_PROMPT,
    STAGE4_SYSTEM_PROMPT,
    TICKER_RECONCILIATION_PROMPT,
)


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _parse_json(raw: str, fallback: dict) -> dict:
    try:
        clean = re.sub(r"```(?:json)?", "", raw).strip()
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        return json.loads(match.group() if match else clean)
    except Exception:
        return {**fallback, "parse_error": raw[:300]}


# ── Helpers: contract tuple construction ────────────────────────────────────────


def _contract_tuple_from_position(pos: dict) -> tuple[str, str, str]:
    """Build a (ticker, primary_exchange, currency) tuple from an enriched position."""
    return (
        pos["ticker"],
        pos.get("primary_exchange", ""),
        pos.get("currency", "USD"),
    )


def _contract_tuple_from_idea_ticker(obj) -> tuple[str, str, str] | None:
    """
    Convert a Stage 3 relevant_idea_tickers entry to a contract tuple.

    The LLM now emits each entry as:
        {"ticker": "XOM", "primary_exchange": "NYSE", "currency": "USD"}

    For robustness we also accept a bare string (legacy / fallback), in which case
    primary_exchange and currency are left empty and the price fetcher will handle
    the gap (e.g. by assuming USD / SMART routing).
    """
    if isinstance(obj, dict):
        ticker = obj.get("ticker", "")
        if not ticker:
            return None
        return (ticker, obj.get("primary_exchange", ""), obj.get("currency", "USD"))
    if isinstance(obj, str):
        # Legacy bare-string fallback — no exchange metadata available
        return (obj, "", "USD")
    return None


def _ticker_from_contract_tuple(t: tuple[str, str, str]) -> str:
    return t[0]


# ── Stage 1 ─────────────────────────────────────────────────────────────────────


async def run_stage1(enriched_positions: list[dict]) -> dict:
    """
    Audit current positions against price levels and KB thesis data.
    Returns triggered_alerts + portfolio_state_summary.
    """
    positions_block = format_positions(enriched_positions)
    kb_context = await _build_stage1_kb_context(enriched_positions)

    raw = await llm_client.complete(
        function_name="positions_proposer_stage1",
        system_prompt=STAGE1_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Date: {_today()}\n\n"
                    "CURRENT POSITIONS (audit these):\n\n"
                    f"{positions_block}\n\n"
                    f"{kb_context}\n\n"
                    "Generate triggered_alerts for every position meeting any audit condition. "
                    "Also produce the portfolio_state_summary."
                ),
            }
        ],
        max_tokens=3000,
        task_profile=llm_client.TaskProfile.HIGH_STAKES,
    )
    return _parse_json(
        raw,
        fallback={"triggered_alerts": [], "portfolio_state_summary": {"positions": []}},
    )


async def _build_stage1_kb_context(enriched_positions: list[dict]) -> str:
    """Load macro snippets, industry snippets, and company catalysts for held positions."""
    blocks: list[str] = []

    economies = {p.get("economy") for p in enriched_positions if p.get("economy")}
    industries = {
        (
            p.get("economy"),
            _strip_economy_prefix(p.get("industry_file_key", ""), p.get("economy", "")),
        )
        for p in enriched_positions
        if p.get("economy") and p.get("industry_file_key")
    }

    # Macro key signals (per economy)
    macro_snippets = []
    for economy in sorted(economies):
        for section in kb_manager.MACRO_FILES.get(economy, []):
            content = await kb_manager.read_macro_file(economy, section)
            if not content:
                continue
            m = re.search(r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL)
            if m:
                macro_snippets.append(
                    f"**{economy.upper()} {section} — Key Signals:**\n{m.group(1).strip()[:350]}"
                )
    if macro_snippets:
        blocks.append("═══ MACRO KEY SIGNALS ═══\n" + "\n\n".join(macro_snippets))

    # Industry key signals (per industry held)
    ind_snippets = []
    for economy, ind_stem in sorted(industries):
        content = await kb_manager.read_industry_file(economy, ind_stem)
        if not content:
            continue
        m = re.search(r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL)
        signal_text = m.group(1).strip()[:400] if m else content[:300]
        ind_snippets.append(f"**{economy}_{ind_stem} — Key Signals:**\n{signal_text}")
    if ind_snippets:
        blocks.append("═══ INDUSTRY KEY SIGNALS ═══\n" + "\n\n".join(ind_snippets))

    # Company catalysts (primary thesis-integrity signal)
    company_snippets = []
    for pos in enriched_positions:
        ticker = pos.get("ticker", "")
        if not ticker:
            continue
        catalysts = await kb_manager.read_company_file(ticker, "catalysts")
        if catalysts:
            company_snippets.append(f"**{ticker} — catalysts.md:**\n{catalysts[:800]}")
    if company_snippets:
        blocks.append(
            "═══ COMPANY CATALYSTS ═══\n" + "\n\n---\n\n".join(company_snippets)
        )

    return "\n\n".join(blocks) or "(No KB data available)"


def _strip_economy_prefix(industry_file_key: str, economy: str) -> str:
    prefix = f"{economy}_"
    return (
        industry_file_key[len(prefix) :]
        if industry_file_key.startswith(prefix)
        else industry_file_key
    )


# ── Stage 2 ─────────────────────────────────────────────────────────────────────


async def run_stage2(
    stage1_result: dict,
    ideas: list[dict],
    capital: float,
    enriched_positions: list[dict],
    price_map: dict,
    adv_map: dict,
) -> list[MacroDecision]:
    """Decide per-economy macro exposure: maintain | explore | reduce | skip."""

    portfolio_economies = {
        pos.get("economy")
        for pos in stage1_result.get("portfolio_state_summary", {}).get("positions", [])
        if pos.get("economy")
    }
    ideas_economies = {i.get("economy") for i in ideas if i.get("economy")}
    relevant_economies = portfolio_economies | ideas_economies | {"global"}

    macro_block = await _build_macro_block(relevant_economies)
    friction_block = _build_friction_block(enriched_positions, price_map, adv_map)

    raw = await llm_client.complete(
        function_name="positions_proposer_stage2",
        system_prompt=STAGE2_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Date: {_today()}\nTotal NAV: ${capital:,.0f}\n\n"
                    "═══ CURRENT PORTFOLIO STATE ═══\n"
                    f"{json.dumps(stage1_result.get('portfolio_state_summary', {}), indent=2)}\n\n"
                    "═══ IDEAS INDEX (compact) ═══\n"
                    f"{format_ideas_index(ideas)}\n\n"
                    "═══ MACRO KEY SIGNALS ═══\n"
                    f"{macro_block}\n\n"
                    "═══ TRANSACTION COSTS ═══\n"
                    f"{friction_block}\n\n"
                    "For each economy in the portfolio or ideas index, decide: "
                    "maintain | explore | reduce | skip. Always assess 'global'."
                ),
            }
        ],
        max_tokens=2000,
        task_profile=llm_client.TaskProfile.HIGH_STAKES,
    )

    parsed = _parse_json(raw, fallback={"macro_decisions": []})
    decisions = []
    for item in parsed.get("macro_decisions", []):
        try:
            decisions.append(MacroDecision(**item))
        except Exception as e:
            print(f"[Stage 2] parse error: {e} — {item}")
    return decisions


async def _build_macro_block(economies: set[str]) -> str:
    parts = []
    for economy in sorted(economies):
        for section in kb_manager.MACRO_FILES.get(economy, []):
            content = await kb_manager.read_macro_file(economy, section)
            if not content:
                continue
            m = re.search(r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL)
            if m:
                parts.append(
                    f"**{economy.upper()} {section}**: {m.group(1).strip()[:350]}"
                )
    return "\n\n".join(parts) or "(No macro data available)"


def _build_friction_block(
    enriched_positions: list[dict],
    price_map: dict,
    adv_map: dict,
) -> str:
    lines = ["ROUND-TRIP COSTS PER CURRENT POSITION:"]
    for pos in enriched_positions:
        ticker = pos.get("ticker", "?")
        lines.append(
            friction_summary_line(
                ticker,
                pos.get("position_size_usd"),
                price_map.get(ticker),
                adv_map.get(ticker),
                pos.get("asset_type", "stock"),
            )
        )
    return "\n".join(lines)


# ── Stage 3 ─────────────────────────────────────────────────────────────────────


async def run_stage3_for_economy(
    economy: str,
    macro_rationale: str,
    ideas: list[dict],
    enriched_positions: list[dict],
) -> list[IndustryDecision]:
    """Decide per-industry exposure for one economy: maintain | explore | reduce | skip."""

    industry_block = await _build_industry_block(economy)
    current_block = (
        format_positions([p for p in enriched_positions if p.get("economy") == economy])
        or f"(No current positions in {economy})"
    )

    system = STAGE3_SYSTEM_PROMPT.replace("{economy}", economy).replace(
        "{macro_rationale}", macro_rationale
    )
    raw = await llm_client.complete(
        function_name=f"positions_proposer_stage3_{economy}",
        system_prompt=system,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Date: {_today()}\nEconomy: {economy.upper()}\n"
                    f"Macro decision rationale: {macro_rationale}\n\n"
                    "═══ INDUSTRY KEY SIGNALS ═══\n"
                    f"{industry_block}\n\n"
                    "═══ IDEAS FOR THIS ECONOMY ═══\n"
                    f"{format_ideas_for_economy(ideas, economy)}\n\n"
                    "═══ CURRENT POSITIONS IN THIS ECONOMY ═══\n"
                    f"{current_block}\n\n"
                    "For each industry, decide: maintain | explore | reduce | skip. "
                    "For 'explore', list specific idea tickers as objects with "
                    "ticker, primary_exchange, and currency fields."
                    f"The industry_file_key MUST come from one of ({','.join([i.strip('.md') for i in await kb_manager.list_industry_files()])})"
                ),
            }
        ],
        max_tokens=2000,
        task_profile=llm_client.TaskProfile.HIGH_STAKES,
    )

    parsed = _parse_json(raw, fallback={"industry_decisions": []})
    decisions = []
    for item in parsed.get("industry_decisions", []):
        try:
            # Normalise relevant_idea_tickers: accept both object and legacy string form,
            # storing each as a dict with ticker / primary_exchange / currency keys.
            raw_tickers = item.get("relevant_idea_tickers") or []
            normalised: list[dict] = []
            for entry in raw_tickers:
                if isinstance(entry, dict) and entry.get("ticker"):
                    normalised.append(
                        {
                            "ticker": entry["ticker"],
                            "primary_exchange": entry.get("primary_exchange", ""),
                            "currency": entry.get("currency", "USD"),
                        }
                    )
                elif isinstance(entry, str) and entry:
                    # Legacy bare-string — log a warning so we can track down the gap
                    print(
                        f"[Stage 3 {economy}] WARNING: idea ticker '{entry}' missing "
                        "exchange metadata. Falling back to empty primary_exchange / USD."
                    )
                    normalised.append(
                        {
                            "ticker": entry,
                            "primary_exchange": "",
                            "currency": "USD",
                        }
                    )
            item["relevant_idea_tickers"] = normalised
            decisions.append(IndustryDecision(**item))
        except Exception as e:
            print(f"[Stage 3 {economy}] parse error: {e} — {item}")
    # print(f"[Industry Decisions] {decisions}")
    return decisions


async def _build_industry_block(economy: str) -> str:
    all_files = await kb_manager.list_industry_files()
    parts = []
    for fname in sorted(f for f in all_files if f.startswith(f"{economy}_")):
        parts_split = fname.split("_", 1)
        if len(parts_split) != 2:
            print(
                f"[Stage 3] WARNING: unexpected industry file name '{fname}' — skipping"
            )
            continue
        _, industry = parts_split
        content = await kb_manager.read_industry_file(economy, industry)
        if not content:
            continue
        m = re.search(r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL)
        signal_text = m.group(1).strip()[:400] if m else content[:300]
        parts.append(f"**{fname}**:\n{signal_text}")
    return "\n\n".join(parts) or f"(No industry data for {economy})"


# ── Stage 4 ─────────────────────────────────────────────────────────────────────


async def run_stage4_for_industry(
    industry_decision: IndustryDecision,
    ideas: list[dict],
    enriched_positions: list[dict],
    price_map: dict,
    adv_map: dict,
    capital: float,
    settings,
    options_mode: bool,
) -> dict:
    """Company-level selection for one industry slot."""

    # relevant_idea_tickers is now a list of dicts: {ticker, primary_exchange, currency}
    relevant_ticker_objs: list[dict] = industry_decision.relevant_idea_tickers or []
    relevant_tickers = [
        obj["ticker"] for obj in relevant_ticker_objs if obj.get("ticker")
    ]

    if not relevant_tickers:
        print(f"[Stage 4] No tickers for {industry_decision.industry_file_key}")
        return {"proposed_trades": [], "friction_cleared": [], "friction_failed": []}

    economy = industry_decision.economy
    industry_file_key = industry_decision.industry_file_key

    company_block = await _build_company_block(relevant_tickers)
    idea_map = {i.get("ticker"): i for i in ideas}
    idea_block = "\n\n---\n\n".join(
        format_idea_detail(idea_map[t]) for t in relevant_tickers if t in idea_map
    )

    current_in_industry = [
        p
        for p in enriched_positions
        if p.get("industry_file_key") == industry_file_key
        or p.get("economy") == economy
    ]

    friction_lines = _build_stage4_friction_lines(
        current_in_industry, relevant_ticker_objs, price_map, adv_map, capital
    )

    system = STAGE4_SYSTEM_PROMPT.format(
        industry_file_key=industry_file_key,
        economy=economy,
        max_c1=int(settings.max_conviction_1_weight * 100),
        max_c2=int(settings.max_conviction_2_weight * 100),
        max_c3=int(settings.max_conviction_3_weight * 100),
    )
    assert "{industry_file_key}" not in system and "{economy}" not in system, (
        "STAGE4_SYSTEM_PROMPT format() substitution failed"
    )
    if options_mode:
        system += OPTIONS_ADDENDUM

    raw = await llm_client.complete(
        function_name=f"positions_proposer_stage4_{industry_file_key}",
        system_prompt=system,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Date: {_today()}\nNAV: SGD${capital:,.0f}\n"
                    f"Industry: {industry_file_key} | Economy: {economy}\n"
                    f"Stage 3 rationale: {industry_decision.rationale}\n\n"
                    "═══ FULL COMPANY RESEARCH ═══\n"
                    f"{company_block}\n\n"
                    "═══ FULL IDEA DETAIL ═══\n"
                    f"{idea_block}\n\n"
                    "═══ CURRENT POSITIONS IN THIS INDUSTRY ═══\n"
                    f"{format_positions(current_in_industry) if current_in_industry else '(none)'}\n\n"
                    "═══ TRANSACTION COSTS ═══\n"
                    f"{chr(10).join(friction_lines)}\n\n"
                    "IBKR Pro: $0.005/share (min $1, max 1% trade value) equities; "
                    "$0.65/contract (min $1) options.\n"
                    "A new idea must generate expected gain > round-trip cost. Coverage ≥ 3×."
                ),
            }
        ],
        max_tokens=4000,
        task_profile=llm_client.TaskProfile.HIGH_STAKES,
    )

    return _parse_json(
        raw,
        fallback={"proposed_trades": [], "friction_cleared": [], "friction_failed": []},
    )


async def _build_company_block(tickers: list[str]) -> str:
    sections = []
    for ticker in tickers:
        catalysts = await kb_manager.read_company_file(ticker, "catalysts")
        overview = await kb_manager.read_company_file(ticker, "overview")
        supply_chain = await kb_manager.read_company_file(ticker, "supply_chain")
        if not any([catalysts, overview, supply_chain]):
            continue
        text = f"### {ticker}\n"
        if overview:
            text += f"**Overview:**\n{overview}\n\n"
        if supply_chain:
            text += f"**Supply chain:**\n{supply_chain}\n\n"
        if catalysts:
            text += f"**Catalysts & thesis:**\n{catalysts}\n"
        sections.append(text)
    return "\n\n---\n\n".join(sections) or "(No company research available)"


def _build_stage4_friction_lines(
    current_positions: list[dict],
    idea_ticker_objs: list[dict],
    price_map: dict,
    adv_map: dict,
    capital: float,
) -> list[str]:
    lines = []
    # Current positions being displaced — keyed by bare ticker in price_map
    for pos in current_positions:
        ticker = pos.get("ticker", "?")
        lines.append(
            friction_summary_line(
                ticker,
                pos.get("position_size_usd"),
                price_map.get(ticker),
                adv_map.get(ticker),
                pos.get("asset_type", "stock"),
            )
        )
    # Idea candidates (estimated at 12% NAV) — use bare ticker as price_map key
    for obj in idea_ticker_objs:
        ticker = obj.get("ticker", "")
        if not ticker:
            continue
        price = price_map.get(ticker)
        if price is None:
            lines.append(f"  {ticker}: price unavailable — friction estimate skipped")
            continue
        lines.append(
            friction_summary_line(
                ticker, capital * 0.12, price, adv_map.get(ticker), "stock"
            )
        )
    return lines


# ── Deduplication ────────────────────────────────────────────────────────────────


async def deduplicate_proposed_trades(all_trades: list[dict]) -> list[dict]:
    """
    When the same ticker surfaces from multiple Stage 4 calls, use LLM
    reconciliation to pick or merge the two proposals.
    """
    by_ticker: dict[str, list[dict]] = {}
    for trade in all_trades:
        by_ticker.setdefault(trade.get("ticker", ""), []).append(trade)

    singles = [proposals[0] for proposals in by_ticker.values() if len(proposals) == 1]
    duplicates = [(t, p) for t, p in by_ticker.items() if len(p) >= 2]

    reconciled = await asyncio.gather(
        *[
            _reconcile_ticker(t, proposals[0], proposals[1])
            for t, proposals in duplicates
        ],
        return_exceptions=True,
    )

    result = list(singles)
    for i, outcome in enumerate(reconciled):
        ticker, proposals = duplicates[i]
        if isinstance(outcome, Exception) or isinstance(outcome, BaseException):
            print(
                f"[dedup] Reconciliation failed for {ticker}: {outcome} — using highest conviction"
            )
            result.append(max(proposals, key=lambda p: p.get("conviction", 0)))
        else:
            result.append(outcome)
    return result


async def _reconcile_ticker(ticker: str, proposal_a: dict, proposal_b: dict) -> dict:
    raw = await llm_client.complete(
        function_name=f"positions_proposer_reconcile_{ticker}",
        system_prompt=TICKER_RECONCILIATION_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Ticker: {ticker}\n\n"
                    f"═══ PROPOSAL A ═══\n{json.dumps(proposal_a, indent=2)}\n\n"
                    f"═══ PROPOSAL B ═══\n{json.dumps(proposal_b, indent=2)}\n\n"
                    "Compare and reconcile. Output the reconciliation JSON."
                ),
            }
        ],
        max_tokens=2000,
        task_profile=llm_client.TaskProfile.HIGH_STAKES,
    )
    rec = _parse_json(raw, fallback={})
    decision = rec.get("reconciliation_decision", "keep_proposal_a")

    if decision == "keep_proposal_b":
        winner = proposal_b
    elif decision == "merge" and rec.get("merged_proposal"):
        winner = rec["merged_proposal"]
    else:
        # Default: highest conviction; proposal_a on tie
        winner = proposal_a
        if proposal_b.get("conviction", 0) > proposal_a.get("conviction", 0):
            winner = proposal_b

    winner["_reconciliation"] = {
        "conflict_detected": rec.get("conflict_detected", False),
        "conflict_description": rec.get("conflict_description", ""),
        "interaction_analysis": rec.get("interaction_analysis", ""),
        "decision": decision,
        "winning_rationale": rec.get("winning_rationale", ""),
    }
    return winner


# ── Final Assembly ───────────────────────────────────────────────────────────────


def _pos_weight(p: dict) -> float:
    return float(p.get("proposed_weight") or p.get("weight") or 0.0)


async def run_final_assembly(
    proposed_trades: list[dict],
    unchanged_positions: list[dict],
    trimmed_positions: list[dict],
    closed_tickers: set[str],
    triggered_alerts: list[dict],
    capital: float,
    settings,
) -> dict:
    """
    Distribute the freed budget pool (closes + trim deltas) across:
      (a) trimmed positions' retained weights
      (b) new trade candidates

    Unchanged positions are never resized here — their weights are frozen.
    """
    locked_weight = sum(_pos_weight(p) for p in unchanged_positions)
    freed_from_closes = sum(
        _pos_weight(p)
        for p in unchanged_positions  # these are already removed from active
        if p.get("ticker") in closed_tickers
    )
    # More accurate: closes come from enriched_current that were flagged
    freed_from_closes = sum(
        _pos_weight(p)
        for p in trimmed_positions
        + list(
            # closed positions contributed to locked before being removed
            {"ticker": t}
            for t in closed_tickers
        )
        if False  # will be computed externally and passed in below
    )

    max_trim_freed = sum(max(0.0, _pos_weight(p) - 0.02) for p in trimmed_positions)
    freed_pool = 1.0 - locked_weight

    print(
        f"[assembly] locked={locked_weight:.3f} | "
        f"freed_pool={freed_pool:.3f} | "
        f"max_trim_freed={max_trim_freed:.3f}"
    )

    alert_by_ticker = {a.get("ticker"): a for a in triggered_alerts}
    trimmed_lines = []
    for p in trimmed_positions:
        line = format_trimmed_for_assembly(p)
        a = alert_by_ticker.get(p.get("ticker"), {})
        if a:
            line += (
                f"\n    Stage 1 alert: {a.get('alert_type')} ({a.get('severity')}) "
                f"— {a.get('description', '')[:120]}"
            )
        trimmed_lines.append(line)

    candidate_lines = [
        format_candidate_for_assembly(t, capital) for t in proposed_trades
    ]
    close_list = ", ".join(sorted(closed_tickers)) or "none"

    cap = settings
    system = FINAL_ASSEMBLY_SYSTEM_PROMPT.format(
        max_c1=int(cap.max_conviction_1_weight * 100),
        max_c2=int(cap.max_conviction_2_weight * 100),
        max_c3=int(cap.max_conviction_3_weight * 100),
    )
    assert "{max_c1}" not in system, "FINAL_ASSEMBLY_SYSTEM_PROMPT substitution failed"

    raw = await llm_client.complete(
        function_name="positions_proposer_final_assembly",
        system_prompt=system,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Date: {_today()}\nTotal NAV: ${capital:,.0f}\n\n"
                    f"LOCKED_WEIGHT (frozen, do not touch): {locked_weight:.4f} ({locked_weight * 100:.1f}%)\n"
                    f"FREED_POOL to allocate: {freed_pool:.4f} ({freed_pool * 100:.1f}%)\n"
                    f"  → from closes ({close_list}): computed externally\n"
                    f"  → from trim deltas (you decide): up to {max_trim_freed:.4f}\n\n"
                    f"Conviction caps — C1: {int(cap.max_conviction_1_weight * 100)}% | "
                    f"C2: {int(cap.max_conviction_2_weight * 100)}% | "
                    f"C3: {int(cap.max_conviction_3_weight * 100)}%\n\n"
                    "═══ STAGE 1 ALERTS ═══\n"
                    f"{format_alerts_for_assembly(triggered_alerts)}\n\n"
                    "═══ TRIMMED POSITIONS (set trim_retained_weight) ═══\n"
                    + ("\n\n".join(trimmed_lines) or "(none)")
                    + "\n\n"
                    "═══ NEW TRADE CANDIDATES ═══\n"
                    + (
                        "\n\n".join(candidate_lines)
                        or "(none — return pool to trimmed positions)"
                    )
                    + "\n\n"
                    f"Allocate freed pool of {freed_pool * 100:.1f}%. "
                    "Trimmed positions retain ≥ 2% each. "
                    "New trades must clear 3× friction coverage. "
                    f"Sum of trim_retained + allocated_to_new = exactly {freed_pool:.4f}."
                ),
            }
        ],
        max_tokens=4000,
        task_profile=llm_client.TaskProfile.HIGH_STAKES,
    )

    parsed = _parse_json(raw, fallback={})
    if not parsed or (
        "trimmed_positions" not in parsed and "new_trade_allocations" not in parsed
    ):
        print(
            "[assembly] LLM returned unparseable output — using deterministic fallback"
        )
        return _fallback_assembly(
            proposed_trades, trimmed_positions, locked_weight, cap
        )

    return parsed


def _fallback_assembly(
    proposed_trades: list[dict],
    trimmed_positions: list[dict],
    locked_weight: float,
    settings,
) -> dict:
    """Deterministic fallback: halve trimmed positions, fill new trades by conviction."""
    cap_map = {
        1: settings.max_conviction_1_weight,
        2: settings.max_conviction_2_weight,
        3: settings.max_conviction_3_weight,
    }

    trim_entries = []
    trim_retained_total = 0.0
    for p in trimmed_positions:
        w = _pos_weight(p)
        c = p.get("conviction", 2)
        retained = round(max(0.02, min(w * 0.5, cap_map.get(c, 0.20))), 4)
        trim_entries.append(
            {
                "ticker": p.get("ticker"),
                "trim_retained_weight": retained,
                "trim_freed_weight": round(w - retained, 4),
                "trim_rationale": "Fallback: halved, floor 2%, conviction-capped.",
            }
        )
        trim_retained_total += retained

    freed_pool = round((1.0 - locked_weight) - trim_retained_total, 4)
    candidates = sorted(
        proposed_trades,
        key=lambda t: (t.get("conviction", 0), t.get("proposed_weight", 0.0)),
        reverse=True,
    )

    trade_entries = []
    remaining = freed_pool
    for t in candidates:
        c = t.get("conviction", 2)
        cap = cap_map.get(c, 0.20)
        allocated = round(
            min(t.get("proposed_weight", 0.10) or 0.10, cap, remaining), 4
        )
        if allocated < 0.02:
            allocated = 0.0
        trade_entries.append(
            {
                "ticker": t.get("ticker"),
                "allocated_weight": allocated,
                "conviction": c,
                "allocation_rationale": "Fallback: conviction-ordered, pool-limited.",
                "friction_cleared": allocated > 0,
            }
        )
        remaining = round(remaining - allocated, 4)
        if remaining <= 0:
            break

    # Return unconsumed pool to trimmed positions pro-rata
    if remaining > 0.001 and trim_entries:
        total_retained = sum(e["trim_retained_weight"] for e in trim_entries)
        if total_retained > 0:
            for e in trim_entries:
                share = round(e["trim_retained_weight"] / total_retained * remaining, 4)
                e["trim_retained_weight"] = round(e["trim_retained_weight"] + share, 4)

    return {
        "trimmed_positions": trim_entries,
        "new_trade_allocations": trade_entries,
        "candidate_comparisons": [],
        "pool_summary": {
            "freed_pool_total": freed_pool,
            "from_closes": 0.0,
            "from_trims": freed_pool,
            "allocated_to_new_trades": sum(
                e["allocated_weight"] for e in trade_entries
            ),
            "returned_to_trimmed": 0.0,
            "unallocated_residual": remaining,
            "candidates_funded": [
                e["ticker"] for e in trade_entries if e["allocated_weight"] > 0
            ],
            "candidates_rejected": [
                e["ticker"] for e in trade_entries if e["allocated_weight"] == 0
            ],
            "rejection_reasons": {
                e["ticker"]: "Pool exhausted or below minimum size"
                for e in trade_entries
                if e["allocated_weight"] == 0
            },
            "fallback_used": True,
        },
    }


def apply_assembly_to_portfolio(
    assembly: dict,
    proposed_trades: list[dict],
    unchanged_positions: list[dict],
    trimmed_positions: list[dict],
    capital: float,
) -> tuple[list[dict], list[dict], list[dict], float]:
    """
    Merge LLM-allocated weights back onto position dicts.
    Applies a final normalisation pass to guarantee sum == 1.0.

    Returns (final_trades, final_unchanged, final_trimmed, total_friction_usd).
    """
    trim_by_ticker = {e["ticker"]: e for e in assembly.get("trimmed_positions", [])}
    trade_by_ticker = {
        e["ticker"]: e for e in assembly.get("new_trade_allocations", [])
    }

    # Unchanged: weights are frozen
    final_unchanged = [
        {**p, "_assembly_weight_rationale": "Unchanged position — weight frozen."}
        for p in unchanged_positions
    ]

    # Trimmed: apply LLM-decided retained weight
    final_trimmed = []
    for pos in trimmed_positions:
        t = pos.get("ticker")
        entry = trim_by_ticker.get(t)
        pos = dict(pos)
        if entry:
            pos["proposed_weight"] = max(
                0.02, float(entry.get("trim_retained_weight", _pos_weight(pos)))
            )
            pos["_assembly_weight_rationale"] = entry.get("trim_rationale", "")
        else:
            pos["proposed_weight"] = max(0.02, _pos_weight(pos) * 0.5)
            pos["_assembly_weight_rationale"] = (
                "Trim fallback: assembly omitted ticker."
            )
        final_trimmed.append(pos)

    # New trades: apply allocated weight; drop if 0
    final_trades = []
    total_friction_usd = 0.0
    for trade in proposed_trades:
        t = trade.get("ticker")
        entry = trade_by_ticker.get(t)
        if not entry or entry.get("allocated_weight", 0.0) == 0.0:
            continue  # Not funded — excluded from final portfolio
        trade = dict(trade)
        trade["proposed_weight"] = float(entry["allocated_weight"])
        trade["_assembly_weight_rationale"] = entry.get("allocation_rationale", "")
        trade["_friction_cleared"] = entry.get("friction_cleared", True)
        fe = trade.get("friction_estimate") or {}
        total_friction_usd += fe.get("round_trip_friction_usd") or 0.0
        final_trades.append(trade)

    # Normalise all weights to sum exactly to 1.0
    all_positions = final_unchanged + final_trimmed + final_trades
    total = sum(_pos_weight(p) for p in all_positions)

    if total <= 0:
        print("[assembly] WARNING: total weight is zero — skipping normalisation")
        return final_trades, final_unchanged, final_trimmed, total_friction_usd

    if abs(total - 1.0) > 0.0001:
        print(f"[assembly] Normalising: raw total={total:.6f}")
        scale = 1.0 / total
        for p in all_positions:
            field = "proposed_weight" if "proposed_weight" in p else "weight"
            p[field] = round(_pos_weight(p) * scale, 4)

    # Absorb rounding residual into largest position
    all_weights = [_pos_weight(p) for p in all_positions]
    residual = round(1.0 - sum(all_weights), 4)
    if residual != 0.0 and all_positions:
        largest = max(all_positions, key=_pos_weight)
        field = "proposed_weight" if "proposed_weight" in largest else "weight"
        largest[field] = round(_pos_weight(largest) + residual, 4)

    return final_trades, final_unchanged, final_trimmed, total_friction_usd

# llm_service/agents/idea_generator.py
#
# REFACTORED ARCHITECTURE (3-stage):
#
#   Stage 1 — Ticker Selection
#   Stage 2 — Per-Company Batch Analysis (parallel, web search on)
#   Stage 3 — Idea Synthesis (single call)
#
# FIX #12: _parse_json_safe kept locally (it is generic, not ideas-specific).
#           The ideas-specific parse is now kb_manager.parse_ideas_output,
#           which is the single canonical entry point for reading the ideas JSON.
#           idea_generator uses kb_manager.parse_ideas_output directly when it
#           needs to re-read its own output; positions_proposer delegates to the
#           same function via the thin shim in positions_proposer._parse_ideas_output.

import asyncio
import json
import re
from datetime import datetime, timezone

from agents import deep_dive as deep_dive_agent
from agents import ticker_selector
from models.types import CompanyMiniAnalysis, Idea, IdeasOutput, Settings
from tools import kb_manager, llm_client

EXCLUDED_ECONOMIES = ["japan", "korea"]

# ── Stage 2: per-company batch analysis ──────────────────────────────────────

STAGE2_SYSTEM_PROMPT = """You are a veteran equity analyst at a top-tier hedge fund.
You have been given full fundamental research on a small batch of companies.
Your task: screen each company against the 8 heuristics below and produce a
compact structured analysis.

HEURISTICS (apply all 8 to every company):

1. R&D INFLECTION: R&D as % of revenue declining — GAAP depressed but normalised
   margins expanding. Quant screens exclude these.

2. BULLWHIP RECOVERY: Post-destocking — inventory/revenue normalising, gross margins
   about to snap back, Street still modelling compression forward.

3. SECOND-ORDER SUPPLY CHAIN: A key customer or supplier has a catalyst that will
   flow through to this company within 2-5 trading days before consensus catches on.

4. SECTOR ROTATION LAG: Sector ETF moved >3% in 5 days; this company hasn't
   participated despite having the same underlying exposure.

5. EARNINGS QUALITY DIVERGENCE: High P/E, but DSO rising + deferred revenue declining
   = leading indicators of disappointment.

6. SEC FILING SIGNAL: New top-tier fund position (13-F) or material event (8-K)
   in a company not yet widely covered.

7. REGULATORY CALENDAR: FDA, antitrust, or policy decision in 1-4 weeks not priced in.

8. CROSS-BORDER SPILLOVER: Macro event in Japan/Korea/UK creates dislocation in a
   US-listed name with significant revenue exposure to that region.

For each company, reference SPECIFIC DATA POINTS from the research provided.
If a company has no triggered heuristics and no interesting thesis, set
worth_including_in_synthesis = false.

Output ONLY valid JSON, no other text:
{
  "batch_analyses": [
    {
      "ticker": "AVGO",
      "economy": "us",
      "industry": "semiconductors",
      "industry_file_key": "us_semiconductors",
      "heuristics_triggered": ["second_order_supply_chain"],
      "preliminary_conviction": 2,
      "one_line_thesis": "Custom ASIC backlog acceleration not in consensus",
      "key_catalyst": "Q3 earnings 2026-08-14",
      "time_horizon_days": 42,
      "invalidation": "If hyperscaler capex guidance cut >15%, thesis fails",
      "supporting_evidence_summary": "From catalysts.md: backlog +34% YoY. Supply chain: TSMC CoWoS allocation rising.",
      "direction_bias": "long",
      "overlooked_reason": "Quant screens exclude on GAAP basis; no sell-side note since Feb",
      "worth_including_in_synthesis": true
    }
  ]
}"""

# ── Stage 3: idea synthesis ───────────────────────────────────────────────────

STAGE3_SYSTEM_PROMPT = """You are a veteran equity analyst at a top-tier hedge fund.
You have been given compact analysis summaries for companies that passed an initial
heuristic screen, along with macro context and today's news.

Your task: synthesise these analyses into a final, ranked ideas list.

For each idea:
- Sharpen the thesis — make it more specific and time-bounded than the mini-analysis
- Cross-reference supply chain relationships between companies in the batch
- Identify second-order effects not captured in individual company analyses
- Set final conviction (1=speculative, 2=moderate, 3=high)
- Ensure every supporting_evidence item references a specific file and metric

Generate 5-15 ideas. Prioritise quality and specificity over quantity.
A company with a weak thesis should be excluded even if it passed Stage 2 screening.

Output ONLY valid JSON, no other text:
{
  "ideas": [
    {
      "ticker": "AVGO",
      "exchange": "NASDAQ",
      "name": "Broadcom Inc.",
      "direction": "long",
      "economy": "us",
      "industry": "semiconductors",
      "industry_file_key": "us_semiconductors",
      "heuristic_triggered": "second_order_supply_chain",
      "one_line_thesis": "Inventory destocking complete; gross margins to snap back 400bps in Q2",
      "key_catalyst": "Q2 earnings on 2026-05-15",
      "time_horizon_days": 21,
      "overlooked_reason": "Street still modelling Q1 margin compression forward; no sell-side note since March",
      "supporting_evidence": [
        "From supply_chain.md: TSMC CoWoS capacity rising 40% in H2",
        "From catalysts.md: custom ASIC backlog +34% YoY per Q1 call"
      ],
      "invalidation": "If Q2 gross margin < 38%, thesis is wrong",
      "related_tickers": ["TSM", "MRVL"],
      "conviction_preliminary": 2,
      "kb_sourced": true
    }
  ],
  "macro_themes_driving_ideas": ["Theme 1", "Theme 2"],
  "total_companies_screened": 20,
  "generated_at": "<ISO timestamp>"
}"""


# ── Stage 2 helpers ───────────────────────────────────────────────────────────


async def _get_economy_key_signals(economies: set[str]) -> str:
    """Load Key Signals section only from macro files for the given economies."""
    parts = []
    for economy in sorted(economies):
        for section in kb_manager.MACRO_FILES.get(economy, []):
            content = await kb_manager.read_macro_file(economy, section)
            if not content:
                continue
            signals_match = re.search(
                r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL
            )
            if signals_match:
                parts.append(
                    f"**{economy.upper()} {section}**: "
                    f"{signals_match.group(1).strip()[:300]}"
                )
    return "\n\n".join(parts) if parts else ""


async def _get_industry_key_signals(industry_file_keys: set[str]) -> str:
    """Load Key Signals section only from industry files for the given keys."""
    parts = []
    for key in sorted(industry_file_keys):
        key_parts = key.split("_", 1)
        if len(key_parts) != 2:
            continue
        economy, industry = key_parts
        content = await kb_manager.read_industry_file(economy, industry)
        if not content:
            continue
        signals_match = re.search(
            r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL
        )
        signal_text = (
            signals_match.group(1).strip()[:350] if signals_match else content[:250]
        )
        parts.append(f"**{key}**: {signal_text}")
    return "\n\n".join(parts) if parts else ""


async def _analyse_company_batch(
    batch: list[str],
    seed_data: dict,
) -> list[CompanyMiniAnalysis]:
    """
    Stage 2 call for a batch of up to 5 companies.
    Loads full KB files for those companies only.
    Loads Key Signals from macro/industry files relevant to those companies.
    """
    company_sections = []
    batch_economies: set[str] = set()
    batch_industry_keys: set[str] = set()

    seed_map = {t["ticker"]: t for t in seed_data.get("tickers", [])}

    for ticker in batch:
        catalysts = await kb_manager.read_company_file(ticker, "catalysts")
        overview = await kb_manager.read_company_file(ticker, "overview")
        supply_chain = await kb_manager.read_company_file(ticker, "supply_chain")

        if not any([catalysts, overview, supply_chain]):
            continue

        seed_entry = seed_map.get(ticker, {})
        economy = seed_entry.get("economy", "us")
        industry = seed_entry.get("industry", "unknown")
        industry_file_key = seed_entry.get("industry_file_key", f"{economy}_{industry}")

        batch_economies.add(economy)
        batch_industry_keys.add(industry_file_key)

        text = f"### {ticker} | economy={economy} | industry_file_key={industry_file_key}\n"
        if overview:
            text += f"**Overview:**\n{overview[:600]}\n\n"
        if supply_chain:
            text += f"**Supply chain:**\n{supply_chain[:800]}\n\n"
        if catalysts:
            text += f"**Catalysts & thesis:**\n{catalysts}\n"

        company_sections.append(text)

    if not company_sections:
        return []

    macro_signals = await _get_economy_key_signals(batch_economies)
    industry_signals = await _get_industry_key_signals(batch_industry_keys)

    context_parts = [
        f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "",
        "═══ COMPANY RESEARCH ═══",
        "\n\n---\n\n".join(company_sections),
    ]
    if macro_signals:
        context_parts += ["", "═══ RELEVANT MACRO SIGNALS ═══", macro_signals]
    if industry_signals:
        context_parts += ["", "═══ RELEVANT INDUSTRY SIGNALS ═══", industry_signals]

    context_parts.append(
        "\n\nApply all 8 heuristics to each company using the research above as your "
        "primary evidence base. Reference specific file sections and metrics."
    )

    raw = await llm_client.complete(
        function_name="idea_generator_stage2_batch",
        system_prompt=STAGE2_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": "\n".join(context_parts)}],
        max_tokens=3000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
    )

    parsed = _parse_json_safe(raw, fallback={"batch_analyses": []})
    analyses = []
    for item in parsed.get("batch_analyses", []):
        try:
            analyses.append(CompanyMiniAnalysis(**item))
        except Exception as e:
            print(f"idea_generator Stage 2: failed to parse mini-analysis: {e}")
    return analyses


def _format_mini_analyses_for_stage3(analyses: list[CompanyMiniAnalysis], settings: Settings | None = None) -> str:
    """Render mini-analyses into a compact block for Stage 3."""
    if settings is None:
        return "(No companies passed Stage 2 screening)"
    if not analyses:
        return "(No companies passed Stage 2 screening)"
    lines = []
    for a in analyses:
        if a.economy not in settings.enabled_economies:
            continue
        lines.append(
            f"**{a.ticker}** [{a.economy}/{a.industry_file_key}] "
            f"| conviction={a.preliminary_conviction} | bias={a.direction_bias}"
        )
        lines.append(f"  Heuristics: {', '.join(a.heuristics_triggered)}")
        lines.append(f"  Thesis: {a.one_line_thesis}")
        if a.key_catalyst:
            lines.append(f"  Catalyst: {a.key_catalyst} ({a.time_horizon_days}d)")
        if a.invalidation:
            lines.append(f"  Invalidation: {a.invalidation}")
        lines.append(f"  Evidence: {a.supporting_evidence_summary}")
        if a.overlooked_reason:
            lines.append(f"  Overlooked: {a.overlooked_reason}")
        lines.append("")
    return "\n".join(lines) or "(No companies passed Stage 2 screening)"


# ── Stage 3 helpers ───────────────────────────────────────────────────────────


async def _load_macro_key_signals_all(settings: Settings | None) -> str:
    """Load Key Signals from all macro files for Stage 3 synthesis context."""
    summaries = []
    enabled_economies = [] if settings is None else settings.enabled_economies
    for economy in enabled_economies + ["global"]:
        for section in kb_manager.MACRO_FILES.get(economy, []):
            content = await kb_manager.read_macro_file(economy, section)
            if not content:
                continue
            signals = re.search(r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL)
            if signals:
                summaries.append(
                    f"**{economy.upper()} {section}**: {signals.group(1).strip()[:250]}"
                )
    return "\n\n".join(summaries) if summaries else "(Run update_macro first)"



# ── Main run ──────────────────────────────────────────────────────────────────


async def run(force: bool = False, max_depth: int = 1, settings: Settings | None = None) -> str:
    """
    Full 3-stage pipeline: ticker selection → batched company analysis → synthesis.
    Returns the ideas output as a JSON string (also persisted).
    """
    if not force and not await kb_manager.should_run_scrape("idea_generator"):
        cached = await kb_manager.read_latest_ideas()
        if cached:
            return f"[CACHED - generated within last 24h]\n\n{cached}"

    # ── Stage 1: ticker selection + deep dives ────────────────────────────────
    seed_data = await ticker_selector.run(force=force)
    enabled_economies = [] if settings is None else settings.enabled_economies
    seed_tickers = [t["ticker"] for t in seed_data.get("tickers", []) if t["economy"] in enabled_economies]

    if not seed_tickers:
        return (
            f"ERROR: ticker_selector returned no tickers.\n"
            f"Details: {seed_data.get('error', 'unknown')}\n"
            f"Ensure update_macro and update_industries have run first."
        )

    all_seed_tickers = await ticker_selector.merge_with_kb_companies(seed_tickers)

    # Filter to only covered economies

    dive_result = await deep_dive_agent.run_all_covered(
        seed_tickers=all_seed_tickers,
        force=force,
        max_depth=max_depth,
        settings=settings
    )
    covered_tickers = dive_result.covered_tickers

    # ── Stage 2: batched per-company analysis (parallel) ─────────────────────
    BATCH_SIZE = 5
    batches = [
        covered_tickers[i : i + BATCH_SIZE]
        for i in range(0, len(covered_tickers), BATCH_SIZE)
    ]

    batch_tasks = [_analyse_company_batch(batch, seed_data) for batch in batches]
    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

    all_analyses: list[CompanyMiniAnalysis] = []
    for result in batch_results:
        if isinstance(result, (Exception, BaseException)):
            print(f"idea_generator Stage 2 batch failed: {result}")
            continue
        all_analyses.extend(result)

    synthesis_candidates = [a for a in all_analyses if a.worth_including_in_synthesis]

    print(
        f"idea_generator Stage 2: {len(covered_tickers)} companies screened, "
        f"{len(synthesis_candidates)} passed to Stage 3"
    )

    # ── Stage 3: synthesis ────────────────────────────────────────────────────
    mini_analyses_block = _format_mini_analyses_for_stage3(synthesis_candidates)
    macro_signals = await _load_macro_key_signals_all(settings)
    articles = await kb_manager.read_articles_for_today()
    articles_block = _format_articles(articles)
    selection_summary = _format_seed_selection(seed_data, all_seed_tickers)

    user_message = (
        f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
        "═══════════════════════════════════════════════════════\n"
        "TICKER SELECTION RATIONALE\n"
        "═══════════════════════════════════════════════════════\n"
        f"{selection_summary}\n\n"
        "═══════════════════════════════════════════════════════\n"
        f"COMPANY ANALYSES ({len(synthesis_candidates)} companies passed screening)\n"
        "═══════════════════════════════════════════════════════\n"
        f"{mini_analyses_block}\n\n"
        "═══════════════════════════════════════════════════════\n"
        "MACRO CONTEXT (Key Signals)\n"
        "═══════════════════════════════════════════════════════\n"
        f"{macro_signals}\n\n"
        "═══════════════════════════════════════════════════════\n"
        f"TODAY'S NEWS ({len(articles)} articles — supplementary signal)\n"
        "═══════════════════════════════════════════════════════\n"
        f"{articles_block}\n\n"
        "Synthesise these analyses into a final ideas list. Cross-reference supply "
        "chain relationships. Sharpen theses. Set final conviction. "
        f"Total companies screened: {len(covered_tickers)}."
    )

    raw = await llm_client.complete(
        function_name="idea_generator_stage3",
        system_prompt=STAGE3_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        max_tokens=8000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
    )

    # ── Parse and persist ─────────────────────────────────────────────────────
    try:
        # FIX #12 — use the shared parser to guarantee format consistency
        ideas_raw = kb_manager.parse_ideas_output(raw)
        if not ideas_raw:
            ideas_raw = _parse_json_safe(raw, fallback={"ideas": []})

        ideas_raw["kb_run_metadata"] = {
            "seed_tickers_selected_by_llm": seed_tickers,
            "seed_tickers_from_prior_kb": [
                t for t in all_seed_tickers if t not in seed_tickers
            ],
            "companies_researched": len(covered_tickers),
            "companies_passed_stage2": len(synthesis_candidates),
            "newly_added": dive_result.newly_added,
            "recursively_added": dive_result.related_spawned,
            "max_depth_used": max_depth,
            "macro_themes_from_selection": seed_data.get("macro_themes", []),
        }
        ideas_raw.setdefault("total_companies_screened", len(covered_tickers))
        ideas_raw.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
        clean = json.dumps(ideas_raw, indent=2)
    except Exception:
        clean = raw

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    output = f"# Ideas - {timestamp}\n\n```json\n{clean}\n```"

    await kb_manager.write_latest_ideas(output)
    await kb_manager.set_last_run("idea_generator")

    return output


async def get_covered_tickers_with_context() -> dict:
    """Returns {ticker: {overview, supply_chain, customers, catalysts}}."""
    tickers = await kb_manager.list_covered_companies()
    context = {}
    for ticker in tickers:
        context[ticker] = await kb_manager.read_all_company_files(ticker)
    return context


# ── Formatting helpers ────────────────────────────────────────────────────────


def _format_seed_selection(seed_data: dict, all_seeds: list[str]) -> str:
    lines = []
    themes = seed_data.get("macro_themes", [])
    if themes:
        lines.append("Macro themes driving selection:")
        for t in themes:
            lines.append(f"  • {t}")
        lines.append("")
    tickers = seed_data.get("tickers", [])
    if tickers:
        lines.append("Selected tickers and rationale:")
        for t in tickers:
            lines.append(
                f"  {t['ticker']} ({t.get('economy', '').upper()} / "
                f"{t.get('industry', '')}) "
                f"[{t.get('heuristic_flag', '')}]: {t.get('selection_reason', '')}"
            )
    return "\n".join(lines) if lines else "(No selection data)"


def _format_articles(articles: list[dict]) -> str:
    if not articles:
        return "(No articles today)"
    lines = []
    for a in articles[:40]:
        filing_tag = f" [{a.get('filing_type', '')}]" if a.get("filing_type") else ""
        lines.append(
            f"[{a['source']}]{filing_tag} {a['title']}\n  {a.get('summary', '')[:150]}"
        )
    return "\n".join(lines)


def _parse_json_safe(raw: str, fallback: dict) -> dict:
    """
    Generic JSON extractor — strips markdown fences and finds the first {...} block.
    This is intentionally kept local; it is a general utility not specific to ideas.
    The ideas-specific canonical parser lives in kb_manager.parse_ideas_output.
    """
    try:
        clean = re.sub(r"```(?:json)?", "", raw).strip()
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(clean)
    except Exception:
        return {**fallback, "parse_error": raw[:300]}

# llm_service/agents/ticker_selector.py
#
# PIPELINE POSITION:
#   update_macro → update_industries → ticker_selector → deep_dive.run_all_covered
#
# REFACTORED ARCHITECTURE (2-stage):
#
#   Stage 1 — Per-Economy Theme Extraction (parallel, no web search)
#     For each economy in [us, uk, japan, korea, global]:
#       Input:  that economy's macro files only (full text)
#       Output: 2-3 compact actionable themes (~200 words each)
#     Runs all 5 economies concurrently via asyncio.gather.
#     No web search — these are KB summarisation calls, not research.
#
#   Stage 2 — Ticker Selection (single call, web search on)
#     Input:  compact theme summaries from Stage 1 (not raw files)
#             + industry Key Signals only (not full industry files)
#             + today's articles (top 30)
#     Output: SeedTickersOutput — 20 seed tickers with rationale
#     Web search used here to verify ticker existence + current liquidity.
#
# WHY THIS IS BETTER:
#   Before: ~500KB+ of macro + industry text in one prompt.
#   After:  Stage 1 compresses each economy to ~200 words = ~1KB total into Stage 2.
#           Stage 2 only sees industry Key Signals (~50 words per file × 40 files = ~2KB).
#           Total Stage 2 input: ~3-4KB of distilled signal vs ~500KB raw.
#
# OUTPUT SCHEMA: SeedTickersOutput (see models/types.py)
#   Persisted to knowledge_base/ideas/seed_tickers.json.
#   Override file at knowledge_base/ideas/seed_tickers_override.json takes precedence.

import asyncio
import json
import re
from datetime import datetime, timezone

import aiofiles

from models.types import SeedTicker, SeedTickersOutput, Settings
from tools import kb_manager, llm_client

# ── Paths ─────────────────────────────────────────────────────────────────────

SEED_TICKERS_PATH = kb_manager.IDEAS_DIR / "seed_tickers.json"
OVERRIDE_PATH = kb_manager.IDEAS_DIR / "seed_tickers_override.json"

TARGET_SEED_COUNT = 20

# ── Stage 1: Per-economy theme extraction ────────────────────────────────────

STAGE1_SYSTEM_PROMPT = """You are a macro analyst. You have been given the full macro
briefing for one economy or region.

Extract the 2-3 MOST ACTIONABLE near-term themes (1-8 week horizon) from this material.
Each theme must:
- Be specific and time-bounded (name the catalyst window)
- Identify which industries or supply chain nodes are most affected
- Note whether the effect is a tailwind or headwind
- Be concise: 3-5 sentences maximum per theme

Output ONLY valid JSON, no other text:
{
  "economy": "us",
  "themes": [
    {
      "theme": "One-line label",
      "detail": "3-5 sentence description of the dynamic, timing, and affected industries",
      "affected_industries": ["semiconductors", "consumer_discretionary"],
      "direction": "tailwind" | "headwind" | "mixed",
      "catalyst_window_weeks": 4
    }
  ]
}"""

STAGE1_INDUSTRY_SYSTEM_PROMPT = """You are a sector analyst. You have been given
industry research for a set of industries within one economy.

For each industry, extract the single most actionable near-term signal (1-8 week horizon).
Be specific: name the dynamic, which companies are affected (by type, not necessarily ticker),
and why this is actionable now.

Output ONLY valid JSON, no other text:
{
  "economy": "us",
  "industry_signals": [
    {
      "industry_file_key": "us_semiconductors",
      "industry": "semiconductors",
      "signal": "2-3 sentence description of the most actionable dynamic",
      "direction": "tailwind" | "headwind" | "mixed",
      "urgency_weeks": 3
    }
  ]
}"""

# ── Stage 2: Unified ticker selection ────────────────────────────────────────

STAGE2_SYSTEM_PROMPT = (
    """You are the chief investment officer of a global event-driven hedge fund.
You have been given distilled macro themes and industry signals from a comprehensive
research process. Your task: identify the MOST INTERESTING companies to research
for near-term trades (1 week to 2 months).

SELECTION CRITERIA — a company makes the list if:
1. It is directly exposed to one of the macro themes or industry signals provided
2. The effect is TIME-BOUNDED — clear catalyst window in the next 1-8 weeks
3. Liquid and IBKR-accessible (market cap > $500M, listed on major exchange)
4. The thesis has an OVERLOOKED ANGLE — something sell-side consensus underweights
5. It sits at an interesting SUPPLY CHAIN NODE — beneficiary or victim of adjacent dynamics

ECONOMY COVERAGE:
- US names: NYSE, NASDAQ, NYSE ARCA
- UK names: LSE (accessible via IBKR)
- Japan names: TSE (via IBKR as ADR or direct)
- Korea names: KRX (via IBKR as ADR or direct)

DIVERSITY REQUIREMENT:
- Spread across at least 4 industries
- Include at least 2 non-US names if macro context warrants it
- Include both long AND short candidates where the signals support it

ANTI-BIAS RULES (critical):
- Do NOT default to mega-caps (AAPL, MSFT, AMZN, GOOGL, META) unless thesis is
  specifically compelling and overlooked — these are over-researched
- Do NOT select on recent price momentum alone — thesis must be fundamental
- DO consider mid-caps where liquidity is sufficient (>$1M average daily volume)
- industry_file_key MUST match the format "{economy}_{industry}" exactly,
  using the industry keys provided in the industry signals input

Output ONLY valid JSON, no other text:
{
  "selected_at": "<ISO timestamp>",
  "macro_themes": ["Specific theme driving selections — be precise"],
  "tickers": [
    {
      "ticker": "AVGO",
      "exchange": "NASDAQ",
      "name": "Broadcom Inc.",
      "economy": "us",
      "industry": "semiconductors",
      "industry_file_key": "us_semiconductors",
      "direction_bias": "long",
      "selection_reason": "AI networking silicon demand inflecting per TSMC data; custom ASIC backlog not in consensus",
      "macro_driver": "US AI capex super-cycle intact; net cash insulates from rates",
      "heuristic_flag": "second_order_supply_chain",
      "conviction_to_research": 3
    }
  ],
  "run_metadata": {
    "economies_covered": [],
    "industries_covered": [],
    "target_count": """
    + str(TARGET_SEED_COUNT)
    + """
  }
}

Select exactly """
    + str(TARGET_SEED_COUNT)
    + """ tickers. Quality and specificity over breadth."""
)


# ── Stage 1 helpers ───────────────────────────────────────────────────────────


async def _extract_economy_themes(economy: str) -> dict:
    """
    Single Stage 1 call for one economy's macro files.
    Returns parsed theme dict or empty fallback on failure.
    """
    parts = [f"## {economy.upper()} MACRO BRIEFING\n"]
    for section in kb_manager.MACRO_FILES.get(economy, []):
        content = await kb_manager.read_macro_file(economy, section)
        if content:
            parts.append(f"### {section}\n{content}")

    if len(parts) == 1:
        # No content found for this economy
        return {"economy": economy, "themes": []}

    macro_text = "\n\n".join(parts)

    raw = await llm_client.complete(
        function_name=f"ticker_selector_stage1_{economy}",
        system_prompt=STAGE1_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
                    f"{macro_text}\n\n"
                    "Extract the 2-3 most actionable near-term macro themes from this economy."
                ),
            }
        ],
        max_tokens=800,
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
    )

    return _parse_json_safe(raw, fallback={"economy": economy, "themes": []})


async def _extract_industry_signals_for_economy(economy: str) -> dict:
    """
    Stage 1 industry pass for one economy — loads Key Signals from each industry file.
    Groups all industries for one economy into a single call to avoid excessive parallelism.
    """
    all_files = await kb_manager.list_industry_files()
    economy_files = [f for f in all_files if f.startswith(f"{economy}_")]

    if not economy_files:
        return {"economy": economy, "industry_signals": []}

    parts = []
    for fname in economy_files:
        f_parts = fname.split("_", 1)
        if len(f_parts) != 2:
            continue
        _, industry = f_parts
        content = await kb_manager.read_industry_file(economy, industry)
        if not content:
            continue
        # Extract Key Signals section only — not the full file
        signals_match = re.search(
            r"### Key Signals(.*?)(?=\n###|\Z)", content, re.DOTALL
        )
        signal_text = (
            signals_match.group(1).strip()[:400] if signals_match else content[:300]
        )
        parts.append(f"### {fname}\n{signal_text}")

    if not parts:
        return {"economy": economy, "industry_signals": []}

    raw = await llm_client.complete(
        function_name=f"ticker_selector_industry_{economy}",
        system_prompt=STAGE1_INDUSTRY_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
                    f"Economy: {economy.upper()}\n\n"
                    + "\n\n".join(parts)
                    + "\n\nExtract the most actionable signal per industry."
                ),
            }
        ],
        max_tokens=1200,
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
    )

    return _parse_json_safe(raw, fallback={"economy": economy, "industry_signals": []})


def _format_themes_for_stage2(theme_results: list[dict]) -> str:
    """Render all economy theme dicts into a compact string for Stage 2."""
    lines = []
    for result in theme_results:
        economy = result.get("economy", "?").upper()
        themes = result.get("themes", [])
        if not themes:
            continue
        lines.append(f"\n## {economy}")
        for t in themes:
            lines.append(f"**{t.get('theme', '')}** ({t.get('direction', '')})")
            lines.append(f"  {t.get('detail', '')}")
            industries = t.get("affected_industries", [])
            if industries:
                lines.append(f"  Affects: {', '.join(industries)}")
            lines.append(
                f"  Catalyst window: ~{t.get('catalyst_window_weeks', '?')} weeks"
            )
    return "\n".join(lines) if lines else "(No macro themes extracted)"


def _format_industry_signals_for_stage2(signal_results: list[dict]) -> str:
    """Render all industry signal dicts into a compact string for Stage 2."""
    lines = []
    for result in signal_results:
        signals = result.get("industry_signals", [])
        for s in signals:
            key = s.get("industry_file_key", "?")
            signal = s.get("signal", "")
            direction = s.get("direction", "")
            urgency = s.get("urgency_weeks", "?")
            lines.append(f"**{key}** [{direction}, ~{urgency}w]: {signal}")
    return "\n".join(lines) if lines else "(No industry signals extracted)"


# ── Main run ──────────────────────────────────────────────────────────────────


async def run(force: bool = False, settings: Settings | None = None) -> dict:
    """
    Select seed tickers using the 2-stage pipeline.

    Stage 1: parallel per-economy macro theme extraction (no web search)
    Stage 2: unified ticker selection with web search for ticker verification

    Returns SeedTickersOutput as dict (also persisted to disk).
    """
    # ── Manual override takes precedence ──────────────────────────────────────
    override = await _load_override()
    if override:
        return override

    # ── Cache check ───────────────────────────────────────────────────────────
    if not force and not await kb_manager.should_run_scrape("ticker_selector"):
        existing = await _load_seed_tickers()
        if existing and existing.get("tickers"):
            return existing

    # ── Stage 1: parallel theme + industry extraction ─────────────────────────
    # economies = ["us", "uk", "japan", "korea"]  # "global" handled separately below
    economies = [] if settings is None else settings.enabled_economies

    # Run all macro theme extractions concurrently
    macro_theme_tasks = [_extract_economy_themes(eco) for eco in economies]
    # Always include global (FX, commodities, geopolitical)
    macro_theme_tasks.append(_extract_economy_themes("global"))

    # Run all industry signal extractions concurrently (one call per economy)
    industry_signal_tasks = [
        _extract_industry_signals_for_economy(eco) for eco in economies
    ]

    # Fire all Stage 1 calls in parallel
    all_results = await asyncio.gather(
        *macro_theme_tasks, *industry_signal_tasks, return_exceptions=True
    )

    # Split results: first (len(economies)+1) are macro themes, rest are industry signals
    n_macro = len(economies) + 1  # +1 for global
    macro_theme_results = []
    industry_signal_results = []

    for i, result in enumerate(all_results):
        if isinstance(result, Exception):
            # Graceful degradation — log and continue
            print(f"ticker_selector Stage 1 task {i} failed: {result}")
            if i < n_macro:
                eco = (economies + ["global"])[i]
                macro_theme_results.append({"economy": eco, "themes": []})
            else:
                eco = economies[i - n_macro]
                industry_signal_results.append({"economy": eco, "industry_signals": []})
        elif i < n_macro:
            macro_theme_results.append(result)
        else:
            industry_signal_results.append(result)

    # ── Format Stage 1 outputs for Stage 2 ────────────────────────────────────
    themes_block = _format_themes_for_stage2(macro_theme_results)
    industry_block = _format_industry_signals_for_stage2(industry_signal_results)

    # ── Load today's articles for recency signal ──────────────────────────────
    articles = await kb_manager.read_articles_for_today()
    articles_block = _format_articles(articles)

    # ── Stage 2: ticker selection ─────────────────────────────────────────────
    user_message = (
        f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
        "═══════════════════════════════════════════════════════\n"
        "MACRO THEMES (distilled from full KB)\n"
        "═══════════════════════════════════════════════════════\n"
        f"{themes_block}\n\n"
        "═══════════════════════════════════════════════════════\n"
        "INDUSTRY SIGNALS (Key Signals per industry)\n"
        "═══════════════════════════════════════════════════════\n"
        f"{industry_block}\n\n"
        "═══════════════════════════════════════════════════════\n"
        f"TODAY'S NEWS (recency signal — {len(articles)} articles)\n"
        "═══════════════════════════════════════════════════════\n"
        f"{articles_block}\n\n"
        f"Select exactly {TARGET_SEED_COUNT} tickers. Every selection must trace to "
        "a specific macro theme or industry signal above. Use web search to verify "
        "each ticker exists, is liquid (ADV > $1M), and is accessible via IBKR."
    )

    raw = await llm_client.complete(
        function_name="ticker_selector_stage2",
        system_prompt=STAGE2_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        max_tokens=5000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
    )

    result = _parse_json_safe(raw, fallback={"tickers": [], "macro_themes": []})
    result["selected_at"] = datetime.now(timezone.utc).isoformat()

    # Attach stage 1 outputs to metadata for auditability
    result.setdefault("run_metadata", {})
    result["run_metadata"]["stage1_macro_themes"] = macro_theme_results
    result["run_metadata"]["stage1_industry_signals"] = industry_signal_results
    result["run_metadata"]["economies_covered"] = economies + ["global"]
    result["run_metadata"]["target_count"] = TARGET_SEED_COUNT

    await _save_seed_tickers(result)
    await kb_manager.set_last_run("ticker_selector")

    return result


async def get_seed_tickers() -> list[str]:
    """
    Return just the list of ticker strings for use by deep_dive.run_all_covered().
    Loads from disk — does NOT trigger a fresh selection run.
    """
    override = await _load_override()
    if override:
        return [t["ticker"] for t in override.get("tickers", [])]

    data = await _load_seed_tickers()
    return [t["ticker"] for t in data.get("tickers", [])] if data else []


async def merge_with_kb_companies(seed_tickers: list[str]) -> list[str]:
    """
    Union of LLM-selected seeds + companies already in the KB from previous runs.
    Ensures recursively-added companies from prior runs are not dropped.
    """
    kb_companies = await kb_manager.list_covered_companies()
    combined = list(dict.fromkeys(seed_tickers + kb_companies))
    return combined


# ── Persistence helpers ───────────────────────────────────────────────────────


async def _load_seed_tickers() -> dict:
    if not SEED_TICKERS_PATH.exists():
        return {}
    async with aiofiles.open(SEED_TICKERS_PATH) as f:
        return json.loads(await f.read())


async def _save_seed_tickers(data: dict) -> None:
    SEED_TICKERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(SEED_TICKERS_PATH, "w") as f:
        await f.write(json.dumps(data, indent=2))


async def _load_override() -> dict | None:
    if not OVERRIDE_PATH.exists():
        return None
    async with aiofiles.open(OVERRIDE_PATH) as f:
        return json.loads(await f.read())


# ── Formatting helpers ────────────────────────────────────────────────────────


def _format_articles(articles: list[dict]) -> str:
    if not articles:
        return "(No articles today)"
    lines = []
    for a in articles[:30]:
        filing_tag = f" [{a.get('filing_type', '')}]" if a.get("filing_type") else ""
        lines.append(
            f"[{a['source']}]{filing_tag} {a['title']}\n  {a.get('summary', '')[:150]}"
        )
    return "\n".join(lines)


def _parse_json_safe(raw: str, fallback: dict) -> dict:
    try:
        clean = re.sub(r"```(?:json)?", "", raw).strip()
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(clean)
    except Exception:
        return {**fallback, "parse_error": raw[:300]}

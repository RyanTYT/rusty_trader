# llm_service/agents/deep_dive.py
#
# ARCHITECTURE CHANGE:
#   deep_dive.run() now returns a DeepDiveResult dataclass containing:
#     - result_text: human-readable summary of what was done
#     - covered_tickers: full list of tickers now in the KB (including recursively added ones)
#     - newly_added: tickers added in this run (not previously in KB)
#     - skipped: tickers skipped due to 24h cache
#
#   Recursion:
#     run() accepts max_depth (default 1) and _visited (internal set to prevent cycles).
#     After researching a ticker, the supply_chain.md is parsed for "interesting" related
#     tickers. The LLM scores each related ticker on tradeable interest (0-10). Any scoring
#     >= RECURSION_THRESHOLD (default 7) and not already visited triggers a recursive dive
#     at depth-1.
#
#   idea_generator calls:
#     deep_dive.run_all_covered(force=False, max_depth=1) which iterates
#     list_covered_companies(), runs each, collects all DeepDiveResults, and returns
#     the union of all covered tickers for the proposer to use.

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from models.types import Settings
from tools import kb_manager, llm_client

RECURSION_THRESHOLD = 8
MAX_CONCURRENT_DIVES = 3
MAX_RELATED_PER_TICKER = 4


@dataclass
class DeepDiveResult:
    ticker: str
    result_text: str
    covered_tickers: list
    newly_added: list
    skipped: list
    related_spawned: list


ECONOMY_IDENTIFIER_PROMPT = """You are a financial data parser.
For each ticker provided, identify its primary listing economy.
Choose EXACTLY one from: ["us", "uk", "korea", "japan"]. If it belongs to none, use "other".

Output ONLY valid JSON as a flat dictionary mapping ticker to economy:
{
  "XOM": "us",
  "AAPL": "us"
  "NG": "uk"
}"""

OVERVIEW_PROMPT = """You are a senior equity research analyst. Produce a comprehensive fundamental overview of the company.

COVER:
- Business model and revenue breakdown by segment (with % contributions)
- Key competitive moats or lack thereof
- Unit economics: gross margin, operating margin, FCF margin (trailing 4Q)
- Balance sheet health: net cash/debt, interest coverage, upcoming debt maturities
- Valuation: EV/EBITDA, P/E (GAAP and adjusted), P/FCF vs. sector peers
- Management quality: track record of guidance accuracy, capital allocation history
- Consensus vs. reality: where does Street consensus look wrong?

LENGTH: 700-1000 words. Dense with specific numbers and dates. No fluff."""

SUPPLY_CHAIN_PROMPT = """You are a supply chain intelligence analyst. Map the complete supply chain for this company.

COVER TIER-1 SUPPLIERS:
- For each major input/component: who are the top 1-3 suppliers?
- What % of revenue/COGS does each represent?
- Geographic concentration risk
- Are any suppliers single-source? What's the switching cost?
- Current financial health of each supplier (briefly)
- Any supply disruption risk in the next 6 months?

COVER TIER-1 CUSTOMERS:
- Who are the top 3-5 customers by revenue?
- Customer concentration risk (any customer >10% of revenue?)
- Are customer relationships healthy? Any contract renewal risks?

SECOND-ORDER SIGNALS:
- If this company reports weak results, which OTHER companies are most affected?
- If a key supplier has trouble, how does it flow through to this company?
- What do the supplier/customer financial health indicators tell us about THIS company's near-term trajectory?

RELATED TICKERS (REQUIRED SECTION - must be last):
List every specific publicly-traded company mentioned as supplier or customer.
Format EXACTLY as:
RELATED_TICKERS: TICKER1, TICKER2, TICKER3

LENGTH: 800-1200 words. Include specific company names, contract details where available."""

CATALYSTS_PROMPT = """You are an event-driven equity analyst. Identify all time-bounded catalysts for this company.

NEAR-TERM CATALYSTS (next 4 weeks):
- Earnings date and key metrics the Street is focused on
- Any regulatory decisions pending
- Product launches, clinical readouts, contract announcements expected
- Macro data releases with direct relevance

MEDIUM-TERM CATALYSTS (1-3 months):
- Strategic developments (M&A rumours, capital allocation decisions)
- Industry conferences or analyst days
- Competitive dynamics (new product from competitor? Market share shifts?)

ACTIVE THESIS:
- What is the most compelling long/short thesis right now?
- What specific data point or event will confirm or deny it?
- What does the options market imply about expected move around next catalyst?

INVALIDATION:
- Be specific. Not "if results disappoint" but "if gross margin < 38.5% in Q2 2026"

LENGTH: 600-900 words. Extremely specific dates and thresholds."""

RELATED_TICKER_SCORING_PROMPT = """You are an event-driven equity portfolio manager.
You are given a list of tickers that are supply-chain related to a company we just researched.

For each ticker, score its NEAR-TERM TRADING INTEREST from 0-10 based on:
- Is there a specific time-bounded catalyst in the next 1-8 weeks?
- Is the stock likely mispriced due to second-order effects from its supply chain relationship?
- Is it liquid enough to trade via IBKR (avoid micro-caps <$200M market cap)?
- Is it NOT already well-covered by sell-side (overlooked angle)?

Score 8-10: Strong catalyst, likely mispriced, liquid, overlooked. Research immediately.
Score 6-7: Interesting but less urgent. Research if capacity allows.
Score 0-5: Low priority. Skip for now.

Output ONLY valid JSON:
{
  "scores": [
    {"ticker": "XXXX", "score": 8, "reason": "Q2 earnings in 3 weeks, supply chain read-through from parent company not priced in"},
    {"ticker": "YYYY", "score": 4, "reason": "Covered heavily by sell-side, no near-term catalyst"}
  ]
}"""

MERGE_SYSTEM_PROMPT = """You are updating an existing company research document.
You will be given the existing document and fresh research.

Produce a <MERGE> block that:
1. Updates any data points that have changed (with new values and dates)
2. Adds new catalysts or supply chain developments
3. Removes or flags any thesis elements that have been invalidated
4. Keeps all still-relevant existing content

OUTPUT FORMAT:
<MERGE>
[Complete updated document content]
</MERGE>

The merged output should be a clean, complete document - not a diff."""


async def _get_economies(tickers):
    if not tickers:
        return {}

    result = await llm_client.complete(
        task_profile=llm_client.TaskProfile.BROAD_SEARCH,
        function_name="deep_dive.identify_economies",
        system_prompt=ECONOMY_IDENTIFIER_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Tickers to classify: {', '.join(tickers)}",
            }
        ],
        max_tokens=500,
    )

    try:
        # Parse the direct ticker-to-economy dictionary
        raw_dict = json.loads(_extract_json(result))

        # Filter down strictly to the allowed economies
        return {ticker.upper(): economy.lower() for ticker, economy in raw_dict.items()}
    except Exception:
        return {}


async def run(
    ticker,
    force=False,
    max_depth=1,
    _visited=None,
    _semaphore=None,
):
    ticker = ticker.upper()

    if _visited is None:
        _visited = set()
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT_DIVES)

    if ticker in _visited:
        return DeepDiveResult(
            ticker=ticker,
            result_text=f"CYCLE: {ticker} already visited in this run",
            covered_tickers=list(_visited),
            newly_added=[],
            skipped=[],
            related_spawned=[],
        )

    _visited.add(ticker)

    if not force:
        last_updated = await kb_manager.get_company_last_updated(ticker)
        if last_updated is not None:
            age = datetime.now(timezone.utc) - last_updated
            if age.total_seconds() < 86400:
                related = await _extract_related_tickers(ticker)
                result = DeepDiveResult(
                    ticker=ticker,
                    result_text=f"SKIPPED: {ticker} updated {int(age.total_seconds() / 3600)}h ago",
                    covered_tickers=list(_visited),
                    newly_added=[],
                    skipped=[ticker],
                    related_spawned=[],
                )
                if max_depth > 0 and related:
                    await _recurse_related(
                        related, max_depth, _visited, _semaphore, result, force
                    )
                return result

    kb_manager.ensure_dirs()
    existing = await kb_manager.read_all_company_files(ticker)
    is_new = all(len(v) < 50 for v in existing.values())
    log_lines = []

    async with _semaphore:
        if is_new:
            overview_task = asyncio.create_task(
                _research_section(ticker, OVERVIEW_PROMPT)
            )
            supply_chain_task = asyncio.create_task(
                _research_section(ticker, SUPPLY_CHAIN_PROMPT)
            )
            catalysts_task = asyncio.create_task(
                _research_section(ticker, CATALYSTS_PROMPT)
            )
            overview, supply_chain, catalysts = await asyncio.gather(
                overview_task, supply_chain_task, catalysts_task
            )
            await kb_manager.write_company_file(ticker, "overview", overview)
            await kb_manager.write_company_file(ticker, "supply_chain", supply_chain)
            await kb_manager.write_company_file(
                ticker, "customers", _extract_customers(supply_chain)
            )
            await kb_manager.write_company_file(ticker, "catalysts", catalysts)
            log_lines.append(f"+ {ticker} (new, full research)")
        else:
            sections = ["overview", "supply_chain", "catalysts"]
            tasks = [
                _incremental_section(ticker, s, existing.get(s, "")) for s in sections
            ]
            merged_contents = await asyncio.gather(*tasks)
            for section, content in zip(sections, merged_contents):
                await kb_manager.write_company_file(ticker, section, content)
            sc = await kb_manager.read_company_file(ticker, "supply_chain")
            await kb_manager.write_company_file(
                ticker, "customers", _extract_customers(sc)
            )
            log_lines.append(f"~ {ticker} (incremental merge)")

    await _update_master_index(ticker)
    await kb_manager.set_last_run(f"deep_dive_{ticker}")

    newly_added = [ticker] if is_new else []

    result = DeepDiveResult(
        ticker=ticker,
        result_text="\n".join(log_lines),
        covered_tickers=list(_visited),
        newly_added=newly_added,
        skipped=[],
        related_spawned=[],
    )

    if max_depth > 0:
        related = await _extract_related_tickers(ticker)
        if related:
            scored = await _score_related_tickers(ticker, related)
            interesting = [
                s["ticker"]
                for s in scored
                if s.get("score", 0) >= RECURSION_THRESHOLD
                and s["ticker"] not in _visited
            ][:MAX_RELATED_PER_TICKER]

            if interesting:
                result.related_spawned = interesting
                await _recurse_related(
                    interesting, max_depth - 1, _visited, _semaphore, result, force
                )

    result.covered_tickers = list(_visited)
    return result


async def run_all_covered(
    seed_tickers=None, force=False, max_depth=1, settings: Settings | None = None
):
    """
    Run deep_dive for every company in the provided seed list (plus interesting
    related tickers discovered by recursion).

    Args:
        seed_tickers: List of tickers to research. If None, falls back to
                      kb_manager.list_covered_companies() for backwards compatibility
                      (e.g. when called directly from the API endpoint).
        force:        If False, skip tickers updated within 24h.
        max_depth:    Supply-chain recursion depth.

    Returns a single aggregated DeepDiveResult.
    """
    if seed_tickers is None:
        seed_tickers = await kb_manager.list_covered_companies()

    if not seed_tickers:
        return DeepDiveResult(
            ticker="__all__",
            result_text="No companies in KB. Add tickers via /news_ideas/deep_dive first.",
            covered_tickers=[],
            newly_added=[],
            skipped=[],
            related_spawned=[],
        )

    # Filter to only enabled economies
    enabled_economies = [] if settings is None else settings.enabled_economies
    economies_dict = await _get_economies(seed_tickers)
    print(f"Economies Dict: {economies_dict}")
    print(f"Seed Tickers: {seed_tickers}")
    # Filtered OUT by default
    seed_tickers = [
        i for i in seed_tickers if economies_dict.get(i, "other") in enabled_economies
    ]

    visited = set()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DIVES)
    all_newly_added = []
    all_skipped = []
    all_spawned = []
    log_lines = [
        f"run_all_covered: {len(seed_tickers)} seed tickers — {', '.join(seed_tickers)}"
    ]

    tasks = [
        run(
            ticker,
            force=force,
            max_depth=max_depth,
            _visited=visited,
            _semaphore=semaphore,
        )
        for ticker in seed_tickers
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, Exception) or isinstance(r, BaseException):
            log_lines.append(f"  ERROR: {r}")
            continue
        log_lines.append(f"  {r.result_text}")
        all_newly_added.extend(r.newly_added)
        all_skipped.extend(r.skipped)
        all_spawned.extend(r.related_spawned)

    final_covered = list(visited)
    log_lines.append(
        f"\nFinal: {len(final_covered)} tickers covered | "
        f"{len(set(all_newly_added))} new | "
        f"{len(set(all_skipped))} cached | "
        f"{len(set(all_spawned))} recursively added"
    )

    return DeepDiveResult(
        ticker="__all__",
        result_text="\n".join(log_lines),
        covered_tickers=final_covered,
        newly_added=list(set(all_newly_added)),
        skipped=list(set(all_skipped)),
        related_spawned=list(set(all_spawned)),
    )


async def _research_section(ticker, system_prompt):
    return await llm_client.complete(
        task_profile=llm_client.TaskProfile.BROAD_SEARCH,
        function_name="deep_dive.research",
        system_prompt=system_prompt,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Research company: {ticker}\n"
                    f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
                    "Use web search extensively. Find the most recent earnings transcript, "
                    "SEC filings, analyst reports, and news. Be specific with numbers and dates."
                ),
            }
        ],
        max_tokens=5000,
        use_web_search=True,
    )


async def _incremental_section(ticker, section, existing_content):
    if not existing_content:
        return await _research_section(ticker, _get_prompt(section))
    fresh = await _research_section(ticker, _get_prompt(section))
    return await _merge_content(existing_content, fresh, ticker, section)


async def _merge_content(existing, fresh, ticker, section):
    result = await llm_client.complete(
        task_profile=llm_client.TaskProfile.LONG_MERGE,
        function_name="deep_dive.merge",
        system_prompt=MERGE_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Company: {ticker} | Section: {section}\n\n"
                    f"EXISTING DOCUMENT:\n{existing}\n\n"
                    f"FRESH RESEARCH (today: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}):\n{fresh}\n\n"
                    "Produce the <MERGE> block."
                ),
            }
        ],
        max_tokens=5000,
    )
    match = re.search(r"<MERGE>(.*?)</MERGE>", result, re.DOTALL)
    return match.group(1).strip() if match else fresh


async def _extract_related_tickers(ticker):
    sc = await kb_manager.read_company_file(ticker, "supply_chain")
    if not sc:
        return []
    match = re.search(r"RELATED_TICKERS:\s*([A-Z0-9,\s]+)", sc)
    if not match:
        return []
    raw = match.group(1)
    tickers = [t.strip() for t in raw.split(",") if t.strip()]
    return [t for t in tickers if re.match(r"^[A-Z]{1,5}$", t)]


async def _score_related_tickers(parent_ticker, related):
    if not related:
        return []
    sc_context = await kb_manager.read_company_file(parent_ticker, "supply_chain")
    result = await llm_client.complete(
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
        function_name="deep_dive.score",
        system_prompt=RELATED_TICKER_SCORING_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Parent company: {parent_ticker}\n"
                    f"Related tickers to score: {', '.join(related)}\n\n"
                    f"Supply chain context from {parent_ticker}:\n{sc_context[:1500]}\n\n"
                    f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
                    "Score each ticker's near-term trading interest."
                ),
            }
        ],
        max_tokens=1000,
        use_web_search=True,
    )
    try:
        data = json.loads(_extract_json(result))
        return data.get("scores", [])
    except Exception:
        return []


async def _recurse_related(tickers, depth, visited, semaphore, parent_result, force):
    tasks = [
        run(t, force=force, max_depth=depth, _visited=visited, _semaphore=semaphore)
        for t in tickers
        if t not in visited
    ]
    if not tasks:
        return
    sub_results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in sub_results:
        if isinstance(r, Exception) or isinstance(r, BaseException):
            continue
        parent_result.newly_added.extend(r.newly_added)
        parent_result.skipped.extend(r.skipped)
        parent_result.related_spawned.extend(r.related_spawned)
    parent_result.covered_tickers = list(visited)


async def _update_master_index(ticker):
    import aiofiles

    index_path = kb_manager.COMPANIES_DIR / "_master_index.md"
    existing_index = ""
    if index_path.exists():
        async with aiofiles.open(index_path) as f:
            existing_index = await f.read()
    entry = f"| {ticker} | {datetime.now(timezone.utc).strftime('%Y-%m-%d')} | [view](./companies/{ticker}/) |"
    if ticker in existing_index:
        existing_index = re.sub(rf"\| {ticker} \|.*?\|", entry, existing_index)
    else:
        if "| Ticker |" not in existing_index:
            existing_index = "# Companies Index\n\n| Ticker | Last Updated | Link |\n|--------|-------------|------|\n"
        existing_index += f"\n{entry}"
    async with aiofiles.open(index_path, "w") as f:
        await f.write(existing_index)


def _get_prompt(section):
    return {
        "overview": OVERVIEW_PROMPT,
        "supply_chain": SUPPLY_CHAIN_PROMPT,
        "catalysts": CATALYSTS_PROMPT,
    }.get(section, "")


def _extract_customers(supply_chain_content):
    match = re.search(
        r"(?:TIER-1 CUSTOMERS|CUSTOMERS)(.*?)(?:SECOND-ORDER|RELATED_TICKERS|$)",
        supply_chain_content,
        re.DOTALL | re.IGNORECASE,
    )
    return match.group(1).strip() if match else supply_chain_content


def _extract_json(text):
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group() if match else text

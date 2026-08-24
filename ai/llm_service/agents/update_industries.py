# llm_service/agents/update_industries.py
#
# Identifies key industries for each economy from the macro KB,
# then researches and writes/updates one .md file per economy-industry pair.
# Format: industries/{economy}_{sector}.md
#
# On incremental updates: performs single-pass triage+rewrite.
#   ===ARCHIVE=== section → appended (with datestamp) to sidecar archive file
#   ===DOCUMENT=== section → replaces the main file wholesale

import re
from datetime import datetime, timezone

from tools import kb_manager, llm_client

IDENTIFY_INDUSTRIES_PROMPT = """You are a senior sector strategist.
Given the macro context for an economy, identify the 5-8 MOST MARKET-RELEVANT industries.
These should be the industries that:
1. Have the most liquid, tradeable companies accessible via IBKR
2. Are most sensitive to the current macro environment
3. Have active catalysts (earnings cycles, regulatory events, commodity linkages)

OUTPUT: JSON array only. No other text.
["technology", "financials", "energy", "healthcare", "consumer_discretionary", "industrials"]"""

INDUSTRY_RESEARCH_PROMPT = """You are a senior equity sector analyst.
Write a comprehensive industry intelligence report for trading purposes.

COVER:
## Overview
- Current cycle position (early/mid/late expansion, contraction)
- Key macro drivers amplifying or dampening this sector right now
- Typical P/E or EV/EBITDA range and where it trades today

## Key Players
- Top 5-8 publicly traded companies (focus on IBKR-accessible names)
- Market cap, rough valuation, key metrics for each

## Headwinds (be specific with data)
- Each headwind: what it is, magnitude, duration expected

## Tailwinds (be specific with data)
- Each tailwind: what it is, magnitude, duration expected

## Supply Chain Dynamics
- Who are the key upstream suppliers to this industry?
- Are there any current supply constraints or gluts?
- Key raw materials and their current price trends

## Catalysts & Calendar
- Upcoming earnings dates for key players
- Regulatory decisions pending
- Macro data releases most relevant to this sector

## Trading Signals
- What indicators most reliably lead sector performance?
- Current reading of those indicators
- Any current dislocations between sector fundamentals and price?

### Key Signals
[5 bullet points: most actionable, current market-relevant data points]

LENGTH: 800-1100 words. Extremely specific — avoid generic statements."""

INCREMENTAL_SYSTEM_PROMPT = """You are a senior equity sector analyst updating an existing industry research document.
You will be given the existing document and today's relevant news.

YOUR TASK:
First, assess each ## sub-section in the existing document by age and relevance:

TRIAGE CATEGORIES:
- STALE: Data is outdated enough to be misleading or useless (superseded earnings, resolved
  catalysts, closed forecast windows, replaced key players data). Archive and drop from document.
- COMPRESS: Still directionally relevant — the trend, cycle position, or structural dynamic
  matters — but the specific figures/narrative can be reduced to 1-3 tight bullets. Do this
  for anything that's background context rather than active signal.
- KEEP: Still current, still useful as a trading input. Trim lightly only if verbose.

Then, research today's developments using web search and produce a full replacement document
integrating: compressed COMPRESS sections, fresh content for updated topics, retained KEEP
sections. Drop STALE sections from the document entirely.

OUTPUT FORMAT — output the two sections below, separated exactly by the delimiter lines shown.
Do not add any text before the first delimiter or between the two sections.

===ARCHIVE===
[For each STALE sub-section, one entry:]
### {sub-section name} (stale as of {today's date})
- [Compressed bullet: key figure + date it was current]
- [Compressed bullet: key figure + date it was current]
[Repeat for each stale sub-section. If nothing is stale, write: NOTHING_STALE]

===DOCUMENT===
[Full replacement markdown document — 800-1100 words.
Structure using the same ## section headers as the original.
Integrate KEEP content (lightly trimmed), COMPRESS content (as tight bullet summaries
under their headers), and new research for updated/new topics.
End with ### Key Signals listing 5 most actionable current data points.]

IMPORTANT:
- Everything after ===DOCUMENT=== is the complete standalone replacement — do not reference triage inside it.
- Keep the document to 800-1100 words. Prefer density and actionability over completeness.
- Every quantitative claim must carry a figure and its date."""


async def run(force: bool = False) -> str:
    if not force and not await kb_manager.should_run_scrape("update_industries"):
        return "SKIPPED: update_industries ran within last 24h. Use force=true to override."

    kb_manager.ensure_dirs()
    results = []

    for economy in ["us", "uk", "japan", "korea"]:
        # Load macro overview to identify relevant industries
        macro_overview = await kb_manager.read_macro_file(economy, "overview")
        if not macro_overview:
            results.append(f"⚠ {economy}: no macro overview — run update_macro first")
            continue

        # Identify industries for this economy
        industries = await _identify_industries(economy, macro_overview)

        for industry in industries:
            existing = await kb_manager.read_industry_file(economy, industry)
            is_empty = len(existing) < 100

            if is_empty or force:
                content = await _full_research(economy, industry, macro_overview)
                await kb_manager.write_industry_file(economy, industry, content)
            else:
                document, archive = await _incremental_update(
                    economy, industry, existing
                )
                await kb_manager.write_industry_file(economy, industry, document)
                if archive:
                    await kb_manager.append_industry_archive(economy, industry, archive)

            results.append(f"✓ {economy}/{industry}")

    await kb_manager.set_last_run("update_industries")
    return "update_industries complete:\n" + "\n".join(results)


async def _identify_industries(economy: str, macro_overview: str) -> list[str]:
    result = await llm_client.complete(
        function_name="update_industries",
        system_prompt=IDENTIFY_INDUSTRIES_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Economy: {economy.upper()}\n\nMacro overview:\n{macro_overview[:1500]}\n\nList the 5-8 most relevant, tradeable industries for this economy right now.",
            }
        ],
        max_tokens=200,
        task_profile=llm_client.TaskProfile.DEEP_REASONING,
    )
    try:
        import json

        match = re.search(r"\[.*?\]", result, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception:
        pass
    # Fallback defaults by economy
    defaults = {
        "us": [
            "technology",
            "financials",
            "energy",
            "healthcare",
            "consumer_discretionary",
            "industrials",
        ],
        "uk": ["financials", "energy", "consumer_staples", "healthcare", "industrials"],
        "japan": ["automotive", "technology", "financials", "industrials", "consumer"],
        "korea": [
            "semiconductors",
            "automotive",
            "technology",
            "shipbuilding",
            "financials",
        ],
    }
    return defaults.get(economy, ["technology", "financials", "energy"])


async def _full_research(economy: str, industry: str, macro_overview: str) -> str:
    economy_labels = {
        "us": "United States",
        "uk": "United Kingdom",
        "japan": "Japan",
        "korea": "South Korea",
    }
    return await llm_client.complete(
        function_name="update_industries",
        system_prompt=INDUSTRY_RESEARCH_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""Economy: {economy_labels.get(economy, economy)}
Industry: {industry.replace("_", " ").title()}
Date: {datetime.now(timezone.utc).strftime("%Y-%m-%d")}

Macro context:
{macro_overview[:1000]}

Use web search to find current data, earnings calendars, and sector news.""",
            }
        ],
        max_tokens=4000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.BROAD_SEARCH,
    )


async def _incremental_update(
    economy: str, industry: str, existing: str
) -> tuple[str, str]:
    """
    Returns (document, archive) where:
      document — full replacement content for the main file
      archive  — stale content block to append to the sidecar (may be empty string)
    """
    today_articles = await kb_manager.read_articles_for_today()
    relevant = [a for a in today_articles if a.get("market", "") in [economy, "global"]]
    articles_summary = "\n".join(
        f"- {a['title']}: {a.get('summary', '')[:150]}" for a in relevant[:8]
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    result = await llm_client.complete(
        function_name="update_industries",
        system_prompt=INCREMENTAL_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""Economy: {economy} | Industry: {industry} | Today: {today}

EXISTING DOCUMENT:
{existing}

TODAY'S RELEVANT ARTICLES:
{articles_summary or "(none — rely on web search)"}

Search for today's sector-specific developments. Triage the existing content, research
updates, and produce the ===ARCHIVE=== and ===DOCUMENT=== sections as instructed.""",
            }
        ],
        max_tokens=3000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.LONG_MERGE,
    )

    return _parse_response(result, today)


def _parse_response(raw: str, today: str) -> tuple[str, str]:
    """
    Split on ===ARCHIVE=== and ===DOCUMENT=== delimiter lines.
    Everything between them is the archive; everything after ===DOCUMENT=== is the document.
    Falls back gracefully if the model omits delimiters.
    """
    ARCHIVE_DELIM = "=ARCHIVE="
    DOCUMENT_DELIM = "=DOCUMENT="

    archive_pos = raw.find(ARCHIVE_DELIM)
    document_pos = raw.find(DOCUMENT_DELIM)

    if archive_pos == -1 and document_pos == -1:
        # No delimiters at all — use whole response as document
        return raw.strip(), ""

    if document_pos == -1:
        # Only archive delimiter found — treat everything after it as archive, no document
        archive_raw = raw[archive_pos + len(ARCHIVE_DELIM) :].strip("=").strip()
        return raw.strip(), _wrap_archive(archive_raw, today)

    if archive_pos == -1 or archive_pos > document_pos:
        # No archive delimiter, or it appears after document (malformed) — skip archive
        document = raw[document_pos + len(DOCUMENT_DELIM) :].strip("=").strip()
        return document, ""

    archive_raw = (
        raw[archive_pos + len(ARCHIVE_DELIM) : document_pos].strip("=").strip()
    )
    document = raw[document_pos + len(DOCUMENT_DELIM) :].strip("=").strip()

    archive = _wrap_archive(archive_raw, today)
    return document, archive

def _wrap_archive(archive_raw: str, today: str) -> str:
    """Wrap archive content with a datestamped header, or return empty if nothing stale."""
    if not archive_raw or "NOTHING_STALE" in archive_raw:
        return ""
    return f"\n\n## Archived {today}\n\n{archive_raw}"

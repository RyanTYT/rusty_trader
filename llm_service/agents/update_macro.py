# llm_service/agents/update_macro.py
#
# Builds and incrementally updates the macro knowledge base.
# On first run (empty files): full research from scratch.
# On subsequent runs: loads existing content, researches only today's
# updates, and produces a triage+rewrite in a single pass:
#   <ARCHIVE> block  → appended (with datestamp) to sidecar archive file
#   <DOCUMENT> block → replaces the main file wholesale

from datetime import datetime, timezone

from tools import kb_manager, llm_client

ECONOMIES = ["global", "us", "uk", "japan", "korea"]

ECONOMY_LABELS = {
    "global": "Global / Cross-Border",
    "us": "United States",
    "uk": "United Kingdom",
    "japan": "Japan",
    "korea": "South Korea",
}

SECTION_DESCRIPTIONS = {
    "overview": "GDP growth rate, monetary policy stance, central bank rate, currency strength vs USD, composite economic health assessment",
    "labour": "Unemployment rate, participation rate, wage growth (YoY), latest jobs report, JOLTS/vacancy data, layoff trends",
    "consumer": "CPI and core CPI, retail sales, consumer confidence index (UMich/GFK/etc), credit card delinquency rates, savings rate",
    "producer": "PPI, PMI Manufacturing, PMI Services, ISM readings, capacity utilisation, inventory-to-sales ratio, order backlogs",
    "bonds": "Yield curve shape (2s10s spread), current 10Y yield, credit spreads (IG/HY), central bank balance sheet direction, treasury/gilt issuance calendar",
    "fiscal": "Government deficit % of GDP, debt-to-GDP, upcoming budget events, major fiscal stimulus/austerity measures",
    "geopolitical": "Active conflicts and trade tensions affecting markets, sanctions, major diplomatic developments, energy supply risks",
    "commodities": "WTI and Brent oil, natural gas, copper (global growth proxy), gold (risk sentiment), agricultural commodities, LNG",
    "fx": "USD index (DXY), EUR/USD, GBP/USD, USD/JPY, USD/KRW, CNY/USD, key cross-rate drivers, carry trade dynamics",
}

FULL_SYSTEM_PROMPT = """You are a senior macro economist and cross-asset strategist with 20 years of experience.
Your task is to produce a comprehensive, factual macro intelligence document for a given economy and topic.

REQUIREMENTS:
- Use web search extensively to find the most current data (today's date or as recent as possible)
- Every quantitative claim must include the specific figure and its date/source
- Identify not just current state but DIRECTION and RATE OF CHANGE (is it improving/deteriorating/stable?)
- Flag any data points that have surprised consensus expectations
- Note any upcoming data releases or events in the next 2-4 weeks that could be market-moving
- Be specific — avoid vague statements like "growth is slowing"; say "Q1 2026 GDP came in at 1.2% QoQ vs 1.8% consensus"
- Length: 600-900 words per section. Dense with facts, not narrative fluff.

OUTPUT FORMAT:
Start directly with the content — no preamble. Use markdown headers for sub-topics within the section.
End each section with a ### Key Signals box listing 3-5 bullet points of the most market-relevant current data points."""

INCREMENTAL_SYSTEM_PROMPT = """You are a senior macro economist updating an existing research document.
You will be given:
1. The existing document content
2. Today's scraped news articles relevant to this economy/topic
3. A web search capability to verify and expand on the news

YOUR TASK:
First, assess each ### sub-section in the existing document by age and relevance:

TRIAGE CATEGORIES:
- STALE: Data is outdated enough to be misleading or useless (superseded figures, past events
  that have resolved, forecasts whose windows have closed). These should be archived, not kept.
- COMPRESS: Still directionally relevant — the trend or context matters — but the specific
  numbers/narrative detail can be condensed to 1-3 bullet points. Compress ruthlessly.
- KEEP: Still current, still useful at full fidelity. Trim lightly only if verbose.

Then, using web search, research today's developments and produce a full replacement document
that integrates: compressed COMPRESS sections, fresh content for updated or new topics, and
retained KEEP sections (lightly trimmed). Drop STALE sections entirely from the document.

OUTPUT FORMAT — output the two sections below, separated exactly by the delimiter lines shown.
Do not add any text before the first delimiter or between the two sections.

===ARCHIVE===
[For each STALE sub-section, one entry:]
### {sub-section name} (stale as of {today's date})
- [Compressed bullet: key figure + date it was current]
- [Compressed bullet: key figure + date it was current]
[Repeat for each stale sub-section. If nothing is stale, write: NOTHING_STALE]

===DOCUMENT===
[Full replacement markdown document — 600-900 words, dense with current facts.
Integrate KEEP content (lightly trimmed), COMPRESS content (as tight bullet summaries
under their headers), and new research for updated/new topics.
End with ### Key Signals listing 3-5 most market-relevant current data points.]

IMPORTANT:
- Everything after ===DOCUMENT=== is the complete replacement document — write it as standalone.
- Do not reference the archive or triage process anywhere after ===DOCUMENT===.
- Keep the document to 600-900 words. Prefer density over completeness.
- Every claim in the document must carry a figure and its date."""


async def run(force: bool = False) -> str:
    """
    Run the macro update agent.
    force=True: rebuild from scratch regardless of last run time.
    force=False: skip if run within 24h.
    """
    if not force and not await kb_manager.should_run_scrape("update_macro"):
        return "SKIPPED: update_macro ran within last 24h. Use force=true to override."

    kb_manager.ensure_dirs()
    today_articles = await kb_manager.read_articles_for_today()
    results = []

    for economy in ECONOMIES:
        sections = kb_manager.MACRO_FILES[economy]
        for section in sections:
            existing = await kb_manager.read_macro_file(economy, section)
            is_empty = len(existing) < 100

            if is_empty or force:
                content = await _full_research(economy, section, today_articles)
                await kb_manager.write_macro_file(economy, section, content)
            else:
                document, archive = await _incremental_update(
                    economy, section, existing, today_articles
                )
                await kb_manager.write_macro_file(economy, section, document)
                if archive:
                    await kb_manager.append_macro_archive(economy, section, archive)

            results.append(f"✓ {economy}/{section}")

    await kb_manager.set_last_run("update_macro")
    return "update_macro complete:\n" + "\n".join(results)


async def _full_research(economy: str, section: str, articles: list[dict]) -> str:
    label = ECONOMY_LABELS[economy]
    description = SECTION_DESCRIPTIONS[section]
    relevant_articles = _filter_articles(articles, economy)
    articles_context = _format_articles(relevant_articles[:15])

    user_message = f"""Research and write the {section.upper()} section for: {label}

TOPIC COVERAGE: {description}

TODAY'S RELEVANT NEWS (use as starting points for deeper searches):
{articles_context}

Use web search to find the most current official statistics, central bank statements,
and analyst commentary. Today's date: {datetime.now(timezone.utc).strftime("%Y-%m-%d")}."""

    return await llm_client.complete(
        function_name="update_macro",
        system_prompt=FULL_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        max_tokens=4000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.BROAD_SEARCH,
    )


async def _incremental_update(
    economy: str, section: str, existing: str, articles: list[dict]
) -> tuple[str, str]:
    """
    Returns (document, archive) where:
      document — full replacement content for the main file
      archive  — stale content block to append to the sidecar (may be empty string)
    """
    label = ECONOMY_LABELS[economy]
    relevant_articles = _filter_articles(articles, economy)
    articles_context = _format_articles(relevant_articles[:10])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    user_message = f"""Economy: {label} | Section: {section.upper()} | Today: {today}

EXISTING DOCUMENT:
{existing}

TODAY'S RELEVANT NEWS:
{articles_context}

Search for today's data releases and events. Triage the existing content, research updates,
and produce the ===ARCHIVE=== and ===DOCUMENT=== sections as instructed."""

    raw = await llm_client.complete(
        function_name="update_macro",
        system_prompt=INCREMENTAL_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        max_tokens=3000,
        use_web_search=True,
        task_profile=llm_client.TaskProfile.LONG_MERGE,
    )

    return _parse_response(raw, today)


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


def _filter_articles(articles: list[dict], economy: str) -> list[dict]:
    economy_markets = {
        "global": ["global", "us", "uk", "japan", "korea"],
        "us": ["us", "global"],
        "uk": ["uk", "global"],
        "japan": ["japan", "global"],
        "korea": ["korea", "global"],
    }
    allowed = economy_markets.get(economy, ["global"])
    return [a for a in articles if a.get("market", "global") in allowed]


def _format_articles(articles: list[dict]) -> str:
    if not articles:
        return "(No articles scraped yet for today — rely on web search)"
    lines = []
    for a in articles:
        lines.append(f"- [{a['source']}] {a['title']}\n  {a['summary'][:200]}")
    return "\n".join(lines)

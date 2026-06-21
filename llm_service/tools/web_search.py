# llm_service/tools/web_search.py
#
# Standalone async web search used by the OpenRouter tool loop.
# Strategy:
#   1. DuckDuckGo (duckduckgo-search) to get candidate URLs + snippets
#   2. httpx to scrape the top N pages for real body text
#   3. If scraping is blocked (403/429/Cloudflare), fall back to DDG snippet only
#
# Returns a list of SearchResult dicts ready to be serialised into the LLM context.

from __future__ import annotations

import asyncio
import logging
import re
import warnings
from dataclasses import dataclass, field
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from ddgs import DDGS

log = logging.getLogger(__name__)

# Suppress duckduckgo_search renaming RuntimeWarning
warnings.filterwarnings(
    "ignore", category=RuntimeWarning, message=".*duckduckgo_search.*renamed to.*"
)

# ── Config ────────────────────────────────────────────────────────────────────

MAX_RESULTS_PER_QUERY = 8  # DDG results to fetch per query
MAX_PAGES_TO_SCRAPE = 3  # how many of those URLs we actually scrape
SCRAPE_BODY_CHAR_LIMIT = 2000  # chars of body text kept per page
SCRAPE_TIMEOUT = 4.0  # seconds per page fetch (lowered to keep the loop responsive)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Sites that reliably block scraping — fall back to snippet only
_SCRAPE_BLOCKLIST = re.compile(
    r"(bloomberg\.com|ft\.com|wsj\.com|nytimes\.com|reuters\.com"
    r"|tradingeconomics\.com|statista\.com|imf\.org/Publications"
    r"|investing\.com)",
    re.IGNORECASE,
)

# Global semaphores to prevent connection congestion and rate-limits
_ddg_semaphore = asyncio.Semaphore(1)
_scrape_semaphore = asyncio.Semaphore(3)


# ── Public types ──────────────────────────────────────────────────────────────


@dataclass
class SearchResult:
    query: str
    title: str
    url: str
    snippet: str
    body: str = ""  # scraped page body, may be empty
    scraped: bool = False  # True if body came from live scrape

    def to_context_block(self) -> str:
        """Render as a compact text block for LLM injection."""
        parts = [
            f"SOURCE: {self.title}",
            f"URL: {self.url}",
            f"SNIPPET: {self.snippet}",
        ]
        if self.body:
            parts.append(f"CONTENT:\n{self.body}")
        return "\n".join(parts)


# ── Public entry point ────────────────────────────────────────────────────────


async def search(query: str, count: int = MAX_RESULTS_PER_QUERY) -> list[SearchResult]:
    """
    Run a single search query.
    Returns up to `count` SearchResult objects, with body text where scraping succeeded.
    """
    log.info("web_search | query=%r count=%d", query, count)

    ddg_results = await _ddg_search(query, count)
    if not ddg_results:
        log.warning("web_search | DDG returned no results for %r", query)
        return []

    # Scrape top N pages in parallel
    to_scrape = [
        r
        for r in ddg_results[:MAX_PAGES_TO_SCRAPE]
        if not _SCRAPE_BLOCKLIST.search(r.url)
    ]
    rest = [r for r in ddg_results if r not in to_scrape]

    async with httpx.AsyncClient(
        headers=_HEADERS, follow_redirects=True, timeout=SCRAPE_TIMEOUT
    ) as client:
        scrape_tasks = [_scrape(client, result) for result in to_scrape]
        scraped = await asyncio.gather(*scrape_tasks, return_exceptions=True)

    final: list[SearchResult] = []
    for result, outcome in zip(to_scrape, scraped):
        if isinstance(outcome, Exception):
            log.debug("scrape failed for %s: %s", result.url, outcome)
        final.append(result)  # body already mutated in-place by _scrape if successful

    final.extend(rest)
    return final[:count]


async def search_many(queries: list[tuple[str, int]]) -> dict[str, list[SearchResult]]:
    """
    Run multiple queries in parallel.
    `queries` is a list of (query_string, count) tuples.
    Returns {query: [SearchResult, ...]}
    """
    tasks = [search(q, c) for q, c in queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out: dict[str, list[SearchResult]] = {}
    for (q, _), res in zip(queries, results):
        if isinstance(res, Exception):
            log.error("search_many | failed for %r: %s", q, res)
            out[q] = []
        else:
            out[q] = res  # type: ignore[assignment]
    return out


# ── DDG search ────────────────────────────────────────────────────────────────


async def _ddg_search(query: str, count: int) -> list[SearchResult]:
    """Blocking DDG call wrapped in a thread executor with retry logic and rate-limiting protection."""

    def _sync() -> list[SearchResult]:
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=count):
                results.append(
                    SearchResult(
                        query=query,
                        title=r.get("title", ""),
                        url=r.get("href", ""),
                        snippet=r.get("body", ""),
                    )
                )
        return results

    async with _ddg_semaphore:
        for attempt in range(3):
            try:
                res = await asyncio.get_event_loop().run_in_executor(None, _sync)
                if res:
                    return res
                log.debug(
                    "DDG returned empty results for %r, retrying (attempt %d/3)...",
                    query,
                    attempt + 1,
                )
            except Exception as exc:
                log.warning(
                    "DDG search attempt %d failed for %r: %s", attempt + 1, query, exc
                )
                if attempt == 2:
                    return []
            await asyncio.sleep(0.2 * (attempt + 1))
        return []


# ── Page scraper ──────────────────────────────────────────────────────────────


async def _scrape(client: httpx.AsyncClient, result: SearchResult) -> None:
    """
    Fetch result.url and extract readable body text.
    Mutates result.body and result.scraped in-place.
    Silently returns on any failure so the caller still has the snippet.
    """
    try:
        async with _scrape_semaphore:
            resp = await client.get(result.url)

        if resp.status_code in (401, 403, 429, 503):
            log.debug("scrape blocked (%d) for %s", resp.status_code, result.url)
            return
        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type and "text/plain" not in content_type:
            return

        body = _extract_body(resp.text)
        if body:
            result.body = body[:SCRAPE_BODY_CHAR_LIMIT]
            result.scraped = True

    except (httpx.TimeoutException, httpx.ConnectError, httpx.TooManyRedirects) as exc:
        log.debug("scrape network error for %s: %s", result.url, exc)
    except Exception as exc:
        log.debug("scrape unexpected error for %s: %s", result.url, exc)


def _extract_body(html: str) -> str:
    """
    Parse HTML and extract meaningful text, stripping nav/footer/scripts.
    Returns cleaned plain text.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove noise elements
    for tag in soup(
        [
            "script",
            "style",
            "nav",
            "footer",
            "header",
            "aside",
            "form",
            "noscript",
            "iframe",
        ]
    ):
        tag.decompose()

    # Prefer article/main content blocks if present
    for selector in (
        "article",
        "main",
        "[role='main']",
        ".article-body",
        ".post-content",
        ".entry-content",
        "#content",
    ):
        block = soup.select_one(selector)
        if block:
            text = block.get_text(separator=" ", strip=True)
            if len(text) > 200:
                return _clean_text(text)

    # Fallback: all paragraph text
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(separator=" ", strip=True) for p in paragraphs)
    if len(text) > 200:
        return _clean_text(text)

    # Last resort: full body text
    return _clean_text(soup.get_text(separator=" ", strip=True))


def _clean_text(text: str) -> str:
    """Collapse whitespace and remove zero-width/control characters."""
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = re.sub(r"\s{3,}", "  ", text)
    return text.strip()

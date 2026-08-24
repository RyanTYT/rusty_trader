# llm_service/tools/llm_client.py
#
# Routing is driven by TASK PROFILE, not function name.
#
#   BROAD_SEARCH   — web gather + synthesis (Gemini Flash default)
#   DEEP_REASONING — KB cross-reference + structured output (Claude Sonnet default)
#   LONG_MERGE     — large-doc faithful merge/edit (Gemini Flash default)
#   HIGH_STAKES    — positions proposals; hardcoded to Claude Sonnet, never cost-optimised
#
# Provider routing:
#   "anthropic/claude-*" -> Anthropic SDK direct  (native web_search tool)
#   "google/gemini-*"    -> Google GenAI SDK direct (native Search grounding)
#   everything else      -> OpenRouter with simulated [SEARCH: ...] tool loop
#
# Web search for OpenRouter models:
#   The system prompt is augmented with tool-use instructions.
#   The model emits [SEARCH: <query>] lines when it wants to search.
#   We execute those searches via web_search.py (DDG + httpx scraping),
#   inject the results as a user turn, and re-prompt until no searches remain.

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from enum import Enum

import anthropic
from google import genai
from google.genai import types
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from models.settings_manager import load_settings
from tools import kb_manager
from tools.web_search import search_many, SearchResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── Task profiles ─────────────────────────────────────────────────────────────


class TaskProfile(str, Enum):
    BROAD_SEARCH = "broad_search"
    DEEP_REASONING = "deep_reasoning"
    LONG_MERGE = "long_merge"
    HIGH_STAKES = "high_stakes"


# ── Clients ───────────────────────────────────────────────────────────────────

_anthropic = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

_openrouter = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY", ""),
)

gemini_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY", ""))

_ANTHROPIC_WEB_SEARCH = [{"type": "web_search_20260209", "name": "web_search"}]

_HIGH_STAKES_MODEL = "anthropic/claude-sonnet-4-6"

# Maximum agentic search rounds for the OpenRouter loop (guards against runaway calls)
_MAX_SEARCH_ROUNDS = 8

# Results injected per query (caps context growth)
_RESULTS_PER_QUERY = 6

# ── Search tool prompt injection ──────────────────────────────────────────────
# Appended to the system prompt for any OpenRouter call with use_web_search=True.

# ── Search tool prompt injection ──────────────────────────────────────────────

_SEARCH_TOOL_INSTRUCTIONS = """
## Web Search Tool

You have access to a web search tool. Use it freely and proactively whenever you need
current data, prices, statistics, news, or any information that may have changed recently.

### Strict Round Tracking
- **Rounds Remaining:** You have **{rounds_left}** subsequent search round(s) left after this turn.
- Batch your queries — emit multiple search calls in this single response to run them in parallel.
- When your remaining rounds hit 0, the search tool will be deactivated completely.

TO SEARCH: use either of these formats:

  Format A (compact):
    [SEARCH: your search query here]

  Format B (structured):
    <longcat_tool_call>web_search
    <longcat_arg_key>query</longcat_arg_key>
    <longcat_arg_value>your search query here</longcat_arg_value>
    </longcat_tool_call>

Rules:
- Emit multiple search calls per response — they run in parallel.
- Be specific: include dates, tickers, series names.
- After emitting search calls, STOP — do not continue writing the main response.
- Cite sources inline as (Source: <title>, <url>) where relevant.
"""

_SEARCH_TOOL_DISABLED_INSTRUCTIONS = """
## Web Search Tool — FINAL SYNTHESIS TURN

You have exhausted all available web search rounds. The search tool is now DISABLED.

**Your task now is to write the complete, final response.**

Critical rules for this turn:
1. Do NOT emit any [SEARCH: ...] or <longcat_tool_call> blocks — they will be silently ignored.
2. Do NOT say you need more information, that your answer is incomplete, or that you would like to search further.
3. Do NOT apologise for missing data — simply work with what you have.
4. Synthesise everything gathered across all previous search rounds into a thorough, well-structured answer.
5. Where data gaps exist, note them briefly inline and move on — do not make them the focus.
6. Cite sources you already have as (Source: <title>, <url>).

Write the full response now.
"""

# ── Public interface ──────────────────────────────────────────────────────────


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30))
async def complete(
    task_profile: TaskProfile,
    system_prompt: str,
    messages: list[dict],
    max_tokens: int = 8000,
    use_web_search: bool = False,
    function_name: str = "",
) -> str:
    model = await _resolve_model(task_profile)
    log.info(
        "call | fn=%-28s profile=%-15s model=%-38s web=%s",
        function_name or "?",
        task_profile.value,
        model,
        use_web_search,
    )

    if _is_anthropic(model):
        return await _anthropic_complete(
            _bare(model), system_prompt, messages, max_tokens, use_web_search
        )
    elif _is_gemini(model):
        return await _gemini_complete(
            _bare(model), system_prompt, messages, max_tokens, use_web_search
        )
    else:
        return await _openrouter_complete(
            model, system_prompt, messages, max_tokens, use_web_search
        )


async def _resolve_model(profile: TaskProfile) -> str:
    settings = await load_settings()
    if profile == TaskProfile.HIGH_STAKES:
        return settings.deep_reasoning_model
    return {
        TaskProfile.BROAD_SEARCH: settings.broad_search_model,
        TaskProfile.DEEP_REASONING: settings.deep_reasoning_model,
        TaskProfile.LONG_MERGE: settings.long_merge_model,
    }[profile]


def _is_anthropic(m: str) -> bool:
    return m.startswith("anthropic/") or m.startswith("claude-")


def _is_gemini(m: str) -> bool:
    return m.startswith("google/") or m.startswith("gemini-")


def _bare(m: str) -> str:
    for p in ("anthropic/", "google/"):
        if m.startswith(p):
            return m[len(p) :]
    return m


# ── Anthropic (native web search) ─────────────────────────────────────────────


async def _anthropic_complete(
    model, system_prompt, messages, max_tokens, use_web_search
) -> str:
    tools = _ANTHROPIC_WEB_SEARCH if use_web_search else []
    current_messages = list(messages)
    text_parts = []

    while True:
        response = await _anthropic.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=current_messages,
            tools=tools if tools else anthropic.NOT_GIVEN,
        )
        for block in response.content:
            if hasattr(block, "text"):
                text_parts.append(block.text)

        if response.stop_reason == "end_turn":
            break
        if response.stop_reason == "tool_use":
            current_messages.append({"role": "assistant", "content": response.content})
            results = [
                {
                    "type": "tool_result",
                    "tool_use_id": b.id,
                    "content": "Search executed — results included in context above.",
                }
                for b in response.content
                if b.type == "tool_use"
            ]
            if results:
                current_messages.append({"role": "user", "content": results})
        else:
            break

    return "\n".join(text_parts)


# ── Gemini (native Search grounding) ─────────────────────────────────────────


async def _gemini_complete(
    model, system_prompt, messages, max_tokens, use_web_search
) -> str:
    tools = []
    if use_web_search:
        tools.append(types.Tool(google_search=types.GoogleSearch()))

    contents = [
        types.Content(
            role="user" if msg["role"] == "user" else "model",
            parts=[types.Part.from_text(text=msg["content"])],
        )
        for msg in messages
    ]

    response = gemini_client.models.generate_content(
        model=model,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            tools=tools if tools else None,
            max_output_tokens=max_tokens,
            temperature=0.3,
        ),
    )

    return response.text or ""


# ── OpenRouter (simulated [SEARCH: ...] tool loop) ────────────────────────────


async def _openrouter_complete(
    model, system_prompt, messages, max_tokens, use_web_search
) -> str:
    current_messages = list(messages)
    final_text = ""

    for round_num in range(_MAX_SEARCH_ROUNDS):
        rounds_left = _MAX_SEARCH_ROUNDS - 1 - round_num
        is_final_round = rounds_left == 0

        if use_web_search:
            tool_instructions = (
                _SEARCH_TOOL_DISABLED_INSTRUCTIONS
                if is_final_round
                else _SEARCH_TOOL_INSTRUCTIONS.format(rounds_left=rounds_left)
            )
            effective_system = system_prompt + tool_instructions
        else:
            effective_system = system_prompt

        raw = await _openrouter_call(
            model, effective_system, current_messages, max_tokens
        )

        search_queries = _parse_search_queries(raw) if not is_final_round else []

        if not search_queries or not use_web_search:
            # Clean response — no searches requested (or search disabled entirely)
            final_text = _strip_search_calls(raw)
            break

        # is_final_round is already handled above (search_queries forced empty),
        # so reaching here always means we have queries to execute.
        log.info(
            "openrouter tool loop | round=%d (rounds remaining=%d) queries=%d model=%s",
            round_num + 1,
            rounds_left,
            len(search_queries),
            model,
        )

        results_map = await search_many(
            [(q, _RESULTS_PER_QUERY) for q in search_queries]
        )
        results_block = _format_search_results(results_map)

        current_messages = current_messages + [
            {"role": "assistant", "content": raw},
            {"role": "user", "content": results_block},
        ]

    else:
        # Fell off the loop without breaking — run one unconditional synthesis turn
        log.warning(
            "openrouter tool loop | hit max rounds (%d), forcing synthesis turn",
            _MAX_SEARCH_ROUNDS,
        )
        effective_system = system_prompt + _SEARCH_TOOL_DISABLED_INSTRUCTIONS
        raw = await _openrouter_call(
            model, effective_system, current_messages, max_tokens
        )
        final_text = _strip_search_calls(raw)

    return final_text


async def _openrouter_call(model, system_prompt, messages, max_tokens) -> str:
    all_messages = [{"role": "system", "content": system_prompt}] + messages
    response = await _openrouter.chat.completions.create(
        model=model,
        messages=all_messages,
        max_tokens=max_tokens,
        extra_headers={
            "HTTP-Referer": "https://autotrader.local",
            "X-Title": "AutoTrader LLM Service",
        },
    )
    return response.choices[0].message.content or ""


# ── Search parsing helpers ────────────────────────────────────────────────────
#
# Two formats are accepted — primary is tried first, longcat is the fallback:
#
#   Primary (compact):
#     [SEARCH: query text]
#
#   Fallback (longcat XML):
#     <longcat_tool_call>web_search
#     <longcat_arg_key>query</longcat_arg_key>
#     <longcat_arg_value>query text</longcat_arg_value>
#     </longcat_tool_call>
#
# Both formats may appear in the same response — all are extracted.

# Primary: [SEARCH: ...]
_SEARCH_LINE_RE = re.compile(r"\[SEARCH:\s*(.+?)\]", re.IGNORECASE)

# Fallback: <longcat_tool_call>web_search ... <longcat_arg_value>query</longcat_arg_value> ...
# Captures the value that follows the "query" arg_key within each tool_call block.
_LONGCAT_BLOCK_RE = re.compile(
    r"<longcat_tool_call>\s*web_search.*?</longcat_tool_call>",
    re.DOTALL | re.IGNORECASE,
)
_LONGCAT_QUERY_RE = re.compile(
    r"<longcat_arg_key>\s*query\s*</longcat_arg_key>\s*"
    r"<longcat_arg_value>\s*(.+?)\s*</longcat_arg_value>",
    re.DOTALL | re.IGNORECASE,
)


def _parse_search_queries(text: str) -> list[str]:
    """
    Extract all unique search queries from a model response.
    Tries [SEARCH: ...] first, then <longcat_tool_call> blocks.
    Deduplicates while preserving order.
    """
    queries: list[str] = []
    seen: set[str] = set()

    def _add(q: str) -> None:
        q = q.strip()
        if q and q not in seen:
            seen.add(q)
            queries.append(q)

    # Primary format
    for match in _SEARCH_LINE_RE.finditer(text):
        _add(match.group(1))

    # Longcat fallback — parse per block so we extract the right arg
    for block in _LONGCAT_BLOCK_RE.finditer(text):
        query_match = _LONGCAT_QUERY_RE.search(block.group(0))
        if query_match:
            _add(query_match.group(1))

    return queries


def _strip_search_calls(text: str) -> str:
    """Remove all search call syntax from the final response text."""
    text = _SEARCH_LINE_RE.sub("", text)
    text = _LONGCAT_BLOCK_RE.sub("", text)
    return text.strip()


def _format_search_results(results_map: dict[str, list[SearchResult]]) -> str:
    """
    Render all search results into a single user-turn string for LLM injection.
    Structure:
      ## Search Results

      ### Query: "..."
      **1. Title** (scraped/snippet)
      URL: ...
      SNIPPET: ...
      CONTENT: ...      ← only if scraped
      ...
    """
    if not results_map:
        return "## Search Results\n\n(No results returned — continue with available knowledge.)"

    sections = ["## Search Results\n"]

    for query, results in results_map.items():
        sections.append(f'### Query: "{query}"')
        if not results:
            sections.append("_(no results)_\n")
            continue
        for i, r in enumerate(results, 1):
            provenance = "scraped" if r.scraped else "snippet only"
            sections.append(f"**{i}. {r.title}** ({provenance})")
            sections.append(f"URL: {r.url}")
            if r.snippet:
                sections.append(f"SNIPPET: {r.snippet}")
            if r.body:
                sections.append(f"CONTENT:\n{r.body}")
            sections.append("")  # blank line between results

    return "\n".join(sections)

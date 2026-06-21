# llm_service/tools/kb_manager.py
#
# All filesystem operations for the knowledge base.
# Directory structure lives on the named Docker volume at /data/knowledge_base.

import json
import os
from datetime import datetime, timezone, time, timedelta
from pathlib import Path
from typing import Optional
import re
from zoneinfo import ZoneInfo

import aiofiles

from models.types import CounterProposal, CounterProposalSession

KB_ROOT = Path(os.getenv("KNOWLEDGE_BASE_PATH", "/data/knowledge_base"))

# ── Sub-directories ───────────────────────────────────────────────────────────

MACRO_DIR = KB_ROOT / "macro"
INDUSTRIES_DIR = KB_ROOT / "industries"
COMPANIES_DIR = KB_ROOT / "companies"
IDEAS_DIR = KB_ROOT / "ideas"
PROPOSALS_DIR = KB_ROOT / "proposals"
POSITIONS_DIR = KB_ROOT / "positions"
PROMPTS_DIR = KB_ROOT / "prompts"


def ensure_dirs() -> None:
    for d in [
        MACRO_DIR,
        INDUSTRIES_DIR,
        COMPANIES_DIR,
        IDEAS_DIR,
        PROPOSALS_DIR,
        POSITIONS_DIR,
        PROMPTS_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)
    # Economy sub-dirs
    for economy in ["global", "us", "uk", "japan", "korea"]:
        (MACRO_DIR / economy).mkdir(exist_ok=True)


# ── Last-run tracking ─────────────────────────────────────────────────────────


async def get_last_run(function_name: str) -> Optional[datetime]:
    path = KB_ROOT / "_last_run.json"
    if not path.exists():
        return None
    async with aiofiles.open(path) as f:
        data = json.loads(await f.read())
    ts = data.get(function_name)
    if ts is None:
        return None
    return datetime.fromisoformat(ts)


async def set_last_run(function_name: str) -> None:
    path = KB_ROOT / "_last_run.json"
    data: dict = {}
    if path.exists():
        async with aiofiles.open(path) as f:
            data = json.loads(await f.read())
    data[function_name] = datetime.now(timezone.utc).isoformat()
    async with aiofiles.open(path, "w") as f:
        await f.write(json.dumps(data, indent=2))


async def should_run_scrape(function_name: str) -> bool:
    now = datetime.now(timezone.utc)
    last_run = await get_last_run(function_name)

    # 1. Always run if it has never run before
    if last_run is None:
        return True

    # 2. NEW: Check if currently during live market hours
    # Using zoneinfo handles local weekdays and DST changes perfectly.
    market_definitions = [
        {"tz": ZoneInfo("Europe/London"), "open": time(8, 0), "close": time(16, 30)},
        {
            "tz": ZoneInfo("America/New_York"),
            "open": time(9, 30),  # Change to time(8, 0) if you want pre-market
            "close": time(16, 0),
        },
        {"tz": ZoneInfo("Asia/Tokyo"), "open": time(9, 0), "close": time(15, 0)},
    ]

    for market in market_definitions:
        local_now = now.astimezone(market["tz"])
        # Check if it's a weekday in that specific market's local time
        if local_now.weekday() <= 4:
            if market["open"] <= local_now.time() <= market["close"]:
                return True

    # 3. Weekday Check for standard cron windows (0 = Monday, 4 = Friday)
    # NOTE: See the warning below regarding Tokyo's 23:00 UTC window.
    if now.weekday() > 4:
        return False

    # 4. Define target start times (UTC) for the specific cron slots
    target_times = [time(6, 30), time(13, 0), time(23, 0)]
    window_minutes = 120
    active_window_start = None

    for t in target_times:
        scheduled_dt = datetime.combine(now.date(), t).replace(tzinfo=timezone.utc)

        if t == time(23, 0) and now.hour < 2:
            scheduled_dt -= timedelta(days=1)

        window_end = scheduled_dt + timedelta(minutes=window_minutes)

        if scheduled_dt <= now <= window_end:
            active_window_start = scheduled_dt
            break

    if active_window_start is None:
        return False

    # 5. Constraint: Only run once per specific window
    if last_run < active_window_start:
        return True

    return False


# ── Macro files ───────────────────────────────────────────────────────────────

MACRO_FILES = {
    "us": ["overview", "labour", "consumer", "producer", "bonds", "fiscal"],
    "uk": ["overview", "labour", "consumer", "producer", "bonds", "fiscal"],
    "japan": ["overview", "labour", "consumer", "producer", "bonds", "fiscal"],
    "korea": ["overview", "labour", "consumer", "producer", "bonds", "fiscal"],
    "global": ["geopolitical", "commodities", "fx"],
}


async def read_macro_file(economy: str, section: str) -> str:
    path = MACRO_DIR / economy / f"{section}.md"
    if not path.exists():
        return ""
    async with aiofiles.open(path) as f:
        return await f.read()


async def write_macro_file(economy: str, section: str, content: str) -> None:
    path = MACRO_DIR / economy / f"{section}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(path, "w") as f:
        await f.write(content)
    # Update last_updated tracker
    await _update_last_updated(MACRO_DIR / economy / "_last_updated.json", section)


async def is_macro_empty(economy: str) -> bool:
    for section in MACRO_FILES.get(economy, []):
        path = MACRO_DIR / economy / f"{section}.md"
        if path.exists() and path.stat().st_size > 100:
            return False
    return True


async def append_macro_archive(economy: str, section: str, content: str) -> None:
    """Append a datestamped stale-content block to the macro archive sidecar."""
    path = _macro_archive_path(economy, section)
    await _append_to_archive(path, content)


def _macro_archive_path(economy: str, section: str) -> Path:
    # Mirrors the pattern used by your existing read/write_macro_file helpers,
    # just with `.archive.md` instead of `.md`.
    return MACRO_DIR / f"{economy}_{section}.archive.md"


# ── Industry files ────────────────────────────────────────────────────────────


async def read_industry_file(economy: str, industry: str) -> str:
    path = INDUSTRIES_DIR / f"{economy}_{industry}.md"
    if not path.exists():
        return ""
    async with aiofiles.open(path) as f:
        return await f.read()


async def write_industry_file(economy: str, industry: str, content: str) -> None:
    path = INDUSTRIES_DIR / f"{economy}_{industry}.md"
    async with aiofiles.open(path, "w") as f:
        await f.write(content)
    await _update_last_updated(
        INDUSTRIES_DIR / "_last_updated.json", f"{economy}_{industry}"
    )


async def list_industry_files() -> list[str]:
    return [p.stem for p in INDUSTRIES_DIR.glob("*.md") if not p.stem.startswith("_")]


async def append_industry_archive(economy: str, industry: str, content: str) -> None:
    """Append a datestamped stale-content block to the industry archive sidecar."""
    path = _industry_archive_path(economy, industry)
    await _append_to_archive(path, content)


def _industry_archive_path(economy: str, industry: str) -> Path:
    return INDUSTRIES_DIR / f"{economy}_{industry}.archive.md"


# ── Private helpers ───────────────────────────────────────────────────────────
async def _append_to_archive(path: Path, content: str) -> None:
    """
    Append content to the archive file at path, creating it if it doesn't exist.
    content is expected to already contain a datestamped ## header from _parse_response.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(path, "a", encoding="utf-8") as f:
        await f.write(content)


# ── Company files ─────────────────────────────────────────────────────────────


def company_dir(ticker: str) -> Path:
    return COMPANIES_DIR / ticker.upper()


COMPANY_FILES = ["overview", "supply_chain", "customers", "catalysts"]


async def read_company_file(ticker: str, section: str) -> str:
    path = company_dir(ticker) / f"{section}.md"
    if not path.exists():
        return ""
    async with aiofiles.open(path) as f:
        return await f.read()


async def write_company_file(ticker: str, section: str, content: str) -> None:
    d = company_dir(ticker)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{section}.md"
    async with aiofiles.open(path, "w") as f:
        await f.write(content)
    await _update_last_updated(d / "_last_updated.json", section)


async def get_company_last_updated(ticker: str) -> Optional[datetime]:
    path = company_dir(ticker) / "_last_updated.json"
    if not path.exists():
        return None
    async with aiofiles.open(path) as f:
        data = json.loads(await f.read())
    # Return the most recent update across all sections
    times = [datetime.fromisoformat(v) for v in data.values() if v]
    return max(times) if times else None


async def read_all_company_files(ticker: str) -> dict[str, str]:
    return {
        section: await read_company_file(ticker, section) for section in COMPANY_FILES
    }


async def list_covered_companies() -> list[str]:
    return [d.name for d in COMPANIES_DIR.iterdir() if d.is_dir()]


# ── Ideas cache ───────────────────────────────────────────────────────────────


async def write_latest_ideas(content: str) -> None:
    path = IDEAS_DIR / "latest_ideas.md"
    async with aiofiles.open(path, "w") as f:
        await f.write(content)


async def read_latest_ideas() -> str:
    path = IDEAS_DIR / "latest_ideas.md"
    if not path.exists():
        return ""
    async with aiofiles.open(path) as f:
        return await f.read()


# ── Proposals ─────────────────────────────────────────────────────────────────


async def read_proposal() -> str:
    """Read latest proposal file and extract the JSON payload."""

    proposal_files = list(PROPOSALS_DIR.glob("*.md"))
    if not proposal_files:
        raise FileNotFoundError("No proposal files found.")

    latest_file = max(proposal_files, key=lambda p: p.name)

    async with aiofiles.open(latest_file, "r") as f:
        content = await f.read()

    # Find start of JSON
    obj_start = content.find("{")
    arr_start = content.find("[")

    starts = [i for i in [obj_start, arr_start] if i != -1]

    if not starts:
        raise ValueError("No JSON start found.")

    start = min(starts)

    clean = content[start:].strip()

    # Validate JSON
    json.loads(clean)

    return clean


async def write_proposal(proposal_json: str) -> str:
    """Write proposal to dated file. Returns the file path."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = PROPOSALS_DIR / f"{date_str}.md"
    # timestamp = datetime.now(timezone.utc).strftime("%H:%M UTC")
    # header = f"# Positions Proposal — {date_str} {timestamp}\n\n"
    async with aiofiles.open(path, "w") as f:
        await f.write(proposal_json)
    return str(path)


# ── Positions ───────────────────────────────────────────────────────────────────


async def write_counter_proposal(final_positions: CounterProposalSession):
    date_str = final_positions.proposal.generated_at.strftime("%Y-%m-%d(%H-%M)UTC.json")
    path = POSITIONS_DIR / date_str
    async with aiofiles.open(path, "w") as f:
        await f.write(final_positions.model_dump_json(indent=2))
    return str(path)


async def read_latest_counter_proposal() -> CounterProposal | None:
    proposals = os.listdir(POSITIONS_DIR)
    if len(proposals) == 0:
        return None

    latest_proposal = sorted(proposals)[-1]
    res = None
    async with aiofiles.open(POSITIONS_DIR / latest_proposal, "r") as f:
        res = json.loads(await f.read())

    return res


# ── Prompts ───────────────────────────────────────────────────────────────────


async def write_prompt(prompt: str):
    now = datetime.now(timezone.utc)

    # Windows-safe filename and avoid ':' in path
    filename = now.strftime("%Y-%m-%dT%H-%M-%S.%fZ.txt")

    path = PROMPTS_DIR / filename

    async with aiofiles.open(path, "w") as f:
        await f.write(prompt)

    return str(path)


# ── Helpers ───────────────────────────────────────────────────────────────────


async def read_articles_for_today() -> list[dict]:
    """Load all scraped articles from today's directory."""
    articles_root = Path(os.getenv("ARTICLES_PATH", "/data/scraped_articles"))
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = articles_root / today
    if not day_dir.exists():
        return []
    articles = []
    for batch_file in sorted(day_dir.glob("batch_*.json")):
        async with aiofiles.open(batch_file) as f:
            batch = json.loads(await f.read())
        articles.extend(batch)
    return articles


async def _update_last_updated(path: Path, key: str) -> None:
    data: dict = {}
    if path.exists():
        async with aiofiles.open(path) as f:
            data = json.loads(await f.read())
    data[key] = datetime.now(timezone.utc).isoformat()
    async with aiofiles.open(path, "w") as f:
        await f.write(json.dumps(data, indent=2))


def parse_ideas_output(raw: str) -> list[dict]:
    """
    Canonical parser for the ideas JSON written by idea_generator.

    Handles both storage formats:
      A) Plain JSON dict with an "ideas" key.
      B) Markdown-fenced JSON (legacy format written as
         "# Ideas - <timestamp>\\n\\n```json\\n{...}\\n```").

    Returns the list of idea dicts, or [] on any parse failure.
    This is the single source of truth — import and call this from any agent
    that needs to read the latest ideas.
    """
    if not raw:
        return []

    try:
        # Strip markdown code fences (handles format B)
        clean = re.sub(r"```(?:json)?", "", raw).strip()

        # Find the outermost JSON object
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        if not match:
            return []

        data = json.loads(match.group())
        ideas = data.get("ideas", [])

        if not isinstance(ideas, list):
            return []

        return ideas

    except Exception as exc:
        # Log but do not raise — callers should degrade gracefully to no ideas
        print(f"[kb_manager.parse_ideas_output] Failed to parse ideas JSON: {exc}")
        return []

# llm_service/main.py
#
# FastAPI service exposing all LLM functions as REST endpoints.
# Called by the Rust backend (which proxies from the Tauri frontend).

from contextlib import asynccontextmanager
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi.exceptions import RequestValidationError
import httpx
from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from agents import (
    deep_dive,
    idea_generator,
    positions_counter_proposer,
    positions_proposer,
    ticker_selector,
)
from agents import update_macro, update_industries
from models.settings_manager import load_settings, update_settings
from models.types import (
    CounterProposal,
    CounterProposalSession,
    PositionsProposal,
    Settings,
    WeightAdjustment,
)
from tools import kb_manager
from api.kb_browser import router as kb_router

# ── Startup ───────────────────────────────────────────────────────────────────


async def _auto_seed_if_empty():
    await asyncio.sleep(5)  # Let the service fully start first
    for economy in ["us", "uk", "japan", "korea", "global"]:
        if await kb_manager.is_macro_empty(economy):
            await update_macro.run(force=True)
            await _notify_backend(
                "Macro knowledge base built from scratch", "update_macro"
            )
            break  # run() covers all economies in one pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    kb_manager.ensure_dirs()
    # Auto-seed KB if empty (first run)
    asyncio.create_task(_auto_seed_if_empty())
    yield


app = FastAPI(title="AutoTrader LLM Service", version="1.0.0", lifespan=lifespan)
app.include_router(kb_router)


# ── Health ─────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


# ── Internal: articles ready notification from scraper ─────────────────────────


class ArticlesReadyPayload(BaseModel):
    date: str


@app.post("/internal/articles_ready")
async def articles_ready(
    payload: ArticlesReadyPayload, background_tasks: BackgroundTasks
):
    """Scraper calls this when a fresh batch of articles is written."""
    background_tasks.add_task(_run_pipeline_for_date, payload.date)
    return {"status": "pipeline queued"}


async def _run_pipeline_for_date(date: str):
    """
    Background task fired when scraper delivers new articles.
    Pipeline order:
      1. update_macro (incremental)
      2. update_industries (incremental)
      3. ticker_selector — LLM re-evaluates seed tickers from updated KB
      4. idea_generator — deep dives + synthesis (uses ticker_selector output)
    """
    settings = await load_settings()
    await update_macro.run(force=False)
    await update_industries.run(force=False)
    # ticker_selector re-runs if >24h since last selection; otherwise reuses cache
    await ticker_selector.run(force=False, settings=settings)
    result = await idea_generator.run(force=False, settings=settings)
    await _notify_backend(
        message="New ideas generated — review positions proposal",
        function="idea_generator",
    )


# ── LLM Function endpoints ────────────────────────────────────────────────────


class ForceParam(BaseModel):
    force: bool = False


@app.post("/functions/update_macro")
async def run_update_macro(params: ForceParam):
    result = await update_macro.run(force=params.force)
    await _notify_backend("Macro knowledge base updated", "update_macro")
    return {"result": result}


@app.post("/functions/update_industries")
async def run_update_industries(params: ForceParam):
    result = await update_industries.run(force=params.force)
    return {"result": result}


@app.post("/functions/ticker_selector")
async def run_ticker_selector(params: ForceParam):
    """
    Manually trigger ticker selection from the macro/industry KB.
    Returns the full seed_tickers dict including selection rationale.
    force=True forces re-selection even if run within 24h.
    """
    settings = await load_settings()
    result = await ticker_selector.run(force=params.force, settings=settings)
    return {"result": result}


@app.get("/functions/ticker_selector/seeds")
async def get_seed_tickers():
    """Return just the current seed ticker list (no re-run)."""
    tickers = await ticker_selector.get_seed_tickers()
    return {"tickers": tickers, "count": len(tickers)}


@app.post("/functions/idea_generator")
async def run_idea_generator(params: ForceParam):
    settings = await load_settings()
    result = await idea_generator.run(force=params.force, settings=settings)
    await _notify_backend("Ideas generated — check /news_ideas", "idea_generator")
    return {"result": result}


class DeepDiveParams(BaseModel):
    ticker: str
    force: bool = False


@app.post("/functions/deep_dive")
async def run_deep_dive(params: DeepDiveParams):
    result = await deep_dive.run(ticker=params.ticker, force=params.force)
    return {"result": result}


class ProposerParams(BaseModel):
    options_mode_override: Optional[bool] = None
    force: bool = False


@app.post("/functions/positions_proposer")
async def run_positions_proposer(params: ProposerParams):
    result = await positions_proposer.run(force=params.force)
    if result is None:
        await _notify_backend(
            "Could not fetch any last positions proposal ", "positions_proposer"
        )
    else:
        await _notify_backend(
            f"Positions proposal ready — {len(result.get('proposed_trades', []))} positions",
            "positions_proposer",
        )
    return result


# ── Counter-proposer chat ──────────────────────────────────────────────────────


class CounterProposerRequest(BaseModel):
    session_id: Optional[str] = None
    conversation_history: list[dict] = []
    original_proposal: PositionsProposal
    weight_adjustments: list[dict] = []
    hold_current_positions: bool = False
    hold_current_reason: Optional[str] = None
    user_message: str


@app.post("/functions/positions_counter_proposer")
async def run_counter_proposer(req: CounterProposerRequest):
    session_id = req.session_id or str(uuid.uuid4())
    counter = CounterProposal(
        session_id=session_id,
        proposal=req.original_proposal,
        weight_adjustments=[WeightAdjustment(**a) for a in req.weight_adjustments],
        hold_current_positions=req.hold_current_positions,
        hold_current_reason=req.hold_current_reason,
        user_message=req.user_message,
    )
    response = await positions_counter_proposer.chat(
        session_id=session_id,
        conversation_history=req.conversation_history,
        counter_proposal=counter,
    )
    return {
        "session_id": session_id,
        "response": response,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Positions endpoints ─────────────────────────────────────────────────────────


@app.post("/positions/update")
async def write_final_positions(final_positions: CounterProposalSession):
    print("Received request to save counter proposal")

    await kb_manager.write_counter_proposal(final_positions)
    print("Wrote counter proposal")
    return {
        "result": {"status": 200},
    }


@app.get("/positions")
async def read_final_positions():
    res = await kb_manager.read_latest_counter_proposal()
    return {
        "result": res,
    }


@app.get("/proposal/latest")
async def read_latest_proposal():
    res = await kb_manager.read_proposal()
    return {
        "result": res,
    }


# ── Settings endpoints ─────────────────────────────────────────────────────────


@app.get("/settings")
async def get_settings():
    return await load_settings()


@app.put("/settings")
async def put_settings(settings: Settings):
    await update_settings(**settings.model_dump())
    return {"status": "ok"}


@app.patch("/settings/options_mode")
async def patch_options_mode(body: dict):
    """Quick toggle for options mode — called from frontend switch."""
    mode = body.get("options_mode", False)
    updated = await update_settings(options_mode=mode)
    return {"options_mode": updated.options_mode}

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print("422 body:", await request.body())
    print("422 errors:", exc.errors())
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

# ── Notify Rust backend (→ WebSocket → Tauri frontend) ───────────────────────


async def _notify_backend(message: str, function: str) -> None:
    backend_url = os.getenv("BACKEND_URL", "http://backend:3000")
    bearer = os.getenv("BEARER_TOKEN", "")
    payload = {
        "title": "News Ideas",
        "body": message,
        "alert_type": "news_ideas",
        "function": function,
    }
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(
                f"{backend_url}/send_notification",
                json=payload,
                headers={"Authorization": f"Bearer {bearer}"},
            )
    except Exception as e:
        print(f"Error trying to notify backend: {e}")
        pass  # Non-critical — don't fail if notification fails

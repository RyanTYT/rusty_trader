# llm_service/models/types.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, List, Optional, Literal, Any
from pydantic import BaseModel, Field


# ── Article (from scraper) ────────────────────────────────────────────────────


class Article(BaseModel):
    id: str
    source: str
    market: str
    url: str
    title: str
    summary: str
    full_text: Optional[str] = None
    published_at: Optional[datetime] = None
    scraped_at: datetime
    filing_type: Optional[str] = None
    ticker: Optional[str] = None


# ── Position types (mirrors Rust backend models) ──────────────────────────────


class StockPosition(BaseModel):
    stock: str
    primary_exchange: str
    strategy: str
    quantity: Optional[float] = None
    avg_price: Optional[float] = None


class OptionPosition(BaseModel):
    stock: str
    primary_exchange: str
    strategy: str
    expiry: str
    strike: float
    multiplier: str
    option_type: Literal["C", "P"]
    quantity: Optional[float] = None
    avg_price: Optional[float] = None


# ── Proposal types ────────────────────────────────────────────────────────────


class Driver(BaseModel):
    title: str
    description: str
    type: Literal[
        "fundamental",  # e.g. R&D inflection, earnings quality
        "technical",  # e.g. support level, momentum
        "macro",  # e.g. rate sensitivity, FX
        "supply_chain",  # e.g. upstream/downstream dislocation
        "sentiment",  # e.g. institutional flow, short interest
        "regulatory",  # e.g. upcoming FDA decision, antitrust
    ]
    overlooked_reason: Optional[str] = None  # Why consensus may have missed this


class PriceThreshold(BaseModel):
    level: float
    rationale: str  # e.g. "200-day MA support" or "1.5× historical P/E fair value"
    action: Literal["buy", "sell", "trim", "add", "close"]
    signal_type: Literal["technical", "fundamental", "volatility_stop"]


class Timing(BaseModel):
    horizon_days: int
    catalyst_date: Optional[str] = None

    validation_condition: PriceThreshold = Field(
        ...,
        description=(
            "Price level that confirms the thesis. Defines where to take profits "
            "or add to the position."
        ),
    )
    invalidation_condition: PriceThreshold = Field(
        ...,
        description=(
            "Price level that falsifies the thesis. Defines the hard exit point "
            "with justification for why that level is structurally significant."
        ),
    )
    price_corridor_rationale: str  # Why these specific bounds (e.g. ATR-based, key S/R)
    monitoring_checklist: List[str]


class IndustryContext(BaseModel):
    economy: str
    industry: str
    # NEW: explicit KB file key for downstream lazy-loading — format: "{economy}_{industry}"
    industry_file_key: Optional[str] = None  # e.g. "us_semiconductors"
    headwind: Optional[str] = None
    tailwind: Optional[str] = None
    macro_linkage: str  # How the macro state amplifies or dampens this


class TradingFriction(BaseModel):
    estimated_shares_or_contracts: int
    commission_usd: float  # IBKR Pro fixed: $0.005/share, min $1, max 1% of trade
    estimated_slippage_usd: float
    total_friction_usd: float
    friction_as_pct_of_position: float
    round_trip_friction_usd: float  # ×2 for entry + exit
    round_trip_friction_pct: float
    ibkr_tier: str = "pro_fixed"
    adv_used: Optional[int] = None  # Shares, from Alpaca
    spread_tier: Literal["large_cap", "mid_cap", "small_cap"] = "mid_cap"


class TriggeredAlert(BaseModel):
    ticker: str
    alert_type: Literal[
        "validation_hit",
        "invalidation_hit",
        "catalyst_changed",
        "thesis_stale",
        "approaching_invalidation",
        "approaching_validation",
    ]
    severity: Literal["informational", "action_required", "urgent"]
    description: str
    recommended_action: Literal["hold", "trim", "close", "add", "reassess"]


class ProposedPosition(BaseModel):
    ticker: str
    primary_exchange: str
    currency: str
    exchange: str
    direction: Literal["long", "short"]
    asset_type: Literal["stock", "call_option", "put_option"]
    proposed_weight: float  # 0.0–1.0 of total portfolio
    current_weight: float = 0.0  # 0.0 if new
    conviction: Literal[1, 2, 3]
    drivers: List[Driver]  # Minimum 2

    # NEW: explicit economy + industry tagging on every position
    economy: Optional[str] = None  # e.g. "us", "uk", "japan", "korea", "taiwan"
    industry: Optional[str] = None  # e.g. "semiconductors", "financials"
    industry_file_key: Optional[str] = (
        None  # e.g. "us_semiconductors" — KB filename stem
    )

    industry_context: IndustryContext
    timing: Timing

    # Position state
    position_state: Literal["new", "increase", "decrease", "replace", "hold"]
    displaced_ticker: Optional[str] = None
    why_better_than_displaced: Optional[str] = None  # Required if displaced_ticker set

    # Friction awareness
    friction_estimate: TradingFriction
    friction_justification: str  # Must reference friction numbers explicitly

    # Options-specific
    option_expiry: Optional[str] = None
    option_strike: Optional[float] = None
    option_vs_stock_rationale: Optional[str] = None
    option_greeks_context: Optional[str] = None
    option_monitoring: Optional[str] = None

    def __getattribute__(self, name: str, /) -> Any:
        return super().__getattribute__(name)


# ── NEW: pipeline audit trail embedded in PositionsProposal ──────────────────


class Stage1AuditSummary(BaseModel):
    """Programmatically assembled — no LLM call."""

    positions_reviewed: List[str]
    alerts_generated: int


class MacroDecision(BaseModel):
    """Output of positions_proposer Stage 2 — one entry per economy considered."""

    economy: str
    decision: Literal["maintain", "explore", "reduce", "skip"]
    rationale: str
    current_exposure_pct: (
        float  # Sum of proposed_weight for positions tagged to this economy
    )


class IndustryDecision(BaseModel):
    """Output of positions_proposer Stage 3 — one entry per industry considered."""

    economy: str
    industry_file_key: str  # e.g. "uk_financials"
    decision: Literal["maintain", "explore", "reduce", "skip"]
    rationale: str
    relevant_idea_tickers: Optional[List[Dict[str, str]]] = (
        None  # Drives which KB files are loaded in Stage 4
    )

    def __getattribute__(self, name: str, /) -> Any:
        return super().__getattribute__(name)


class Stage4CompanySummary(BaseModel):
    """Programmatically assembled — no LLM call."""

    explored: List[dict[str, str]]  # All tickers evaluated in Stage 4
    friction_cleared: List[str]  # Tickers where displacement hurdle was cleared
    friction_failed: List[str]  # Tickers where displacement hurdle was not cleared


class PipelineStages(BaseModel):
    """Audit trail of all pipeline stage decisions. Assembled programmatically."""

    stage1_audit: Stage1AuditSummary
    stage2_macro: Optional[Dict] = None  # {economy: decision} map + decisions list
    stage3_industry: Optional[Dict] = None  # {economy: {industry: decision}} map
    stage4_companies: Optional[Stage4CompanySummary] = None


class CandidateComparison(BaseModel):
    ticker_a: str = Field(..., description="First ticker being compared")
    ticker_b: str = Field(..., description="Second ticker being compared")

    conviction_comparison: str
    catalyst_comparison: str
    risk_reward_comparison: str
    friction_comparison: str

    verdict: str


class PoolSummary(BaseModel):
    freed_pool_total: float
    from_closes: float
    from_trims: float

    allocated_to_new_trades: float
    returned_to_trimmed: float
    unallocated_residual: float

    candidates_funded: List[str]
    candidates_rejected: List[str]

    rejection_reasons: Dict[str, str]


class TrimmedPosition(BaseModel):
    ticker: str
    trim_retained_weight: float
    trim_freed_weight: float
    trim_rationale: str


class PositionsProposal(BaseModel):
    generated_at: datetime = Field(default_factory=datetime.now)
    capital_at_proposal: float
    trimmed_positions: List[TrimmedPosition]
    weight_sum_check: float

    # 1. Audit of current state
    triggered_alerts: List[TriggeredAlert] = Field(
        ...,
        description=(
            "Positions where a catalyst changed, price hit a threshold, "
            "or the thesis has gone stale."
        ),
    )

    # 2. Actions
    proposed_trades: List[ProposedPosition]
    unchanged_positions: List[ProposedPosition]
    removed_positions: List[str]

    # 3. High-level context
    portfolio_thesis: str
    macro_backdrop: str
    total_estimated_friction_usd: float
    total_friction_as_pct_nav: float

    # 4. Assembly stage
    candidate_comparisons: List[CandidateComparison]
    assembly_pool_summary: PoolSummary

    # NEW: pipeline audit trail — always populated, never sent to any LLM
    pipeline_stages: Optional[PipelineStages] = None

    def __getattribute__(self, name: str, /) -> Any:
        return super().__getattribute__(name)


# ── Counter-proposer chat ─────────────────────────────────────────────────────


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class WeightAdjustment(BaseModel):
    ticker: str
    old_weight: float
    new_weight: float
    reason: str


class CounterProposal(BaseModel):
    """Sent from frontend when user challenges the proposal"""

    session_id: str
    proposal: PositionsProposal
    weight_adjustments: List[WeightAdjustment]
    hold_current_positions: bool
    hold_current_reason: Optional[str] = None
    user_message: str

    def __getattribute__(self, name: str, /) -> Any:
        return super().__getattribute__(name)


class CounterProposalSession(BaseModel):
    """Sent from frontend when user challenges the proposal"""

    session_id: str
    proposal: PositionsProposal
    conversation: List[str]
    weight_adjustments: List[WeightAdjustment]
    hold_current_positions: bool
    hold_current_reason: Optional[str] = None

    def __getattribute__(self, name: str, /) -> Any:
        return super().__getattribute__(name)


# ── NEW: ticker_selector output types ────────────────────────────────────────


class SeedTicker(BaseModel):
    """One entry in the ticker_selector output tickers list."""

    ticker: str
    exchange: str
    name: str
    economy: str  # "us" | "uk" | "japan" | "korea" | "global" etc.
    industry: str  # free-text industry name
    industry_file_key: str  # NEW: "{economy}_{industry}" matching KB filename stem
    direction_bias: Literal["long", "short", "neutral"] = "long"
    selection_reason: str
    macro_driver: str
    heuristic_flag: str
    conviction_to_research: Literal[1, 2, 3]


class SeedTickersOutput(BaseModel):
    """
    Full output of ticker_selector — persisted to seed_tickers.json.
    NEW: structured Pydantic model replacing raw dict.
    """

    selected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    macro_themes: List[str]
    tickers: List[SeedTicker]
    run_metadata: dict = Field(default_factory=dict)


# ── NEW: idea_generator intermediate + output types ───────────────────────────


class CompanyMiniAnalysis(BaseModel):
    """
    Internal intermediate schema produced per company batch in idea_generator Stage 2.
    Not persisted independently — collected and fed to Stage 3 synthesis.
    """

    ticker: str
    economy: str
    industry: str
    industry_file_key: str  # "{economy}_{industry}"
    heuristics_triggered: List[str]
    preliminary_conviction: Literal[1, 2, 3]
    one_line_thesis: str
    key_catalyst: Optional[str] = None
    time_horizon_days: Optional[int] = None
    invalidation: Optional[str] = None
    supporting_evidence_summary: str  # Compact — sourced from KB files, not full text
    direction_bias: Literal["long", "short", "neutral"] = "long"
    overlooked_reason: Optional[str] = None
    worth_including_in_synthesis: bool  # False = skip in Stage 3, reducing prompt size


class Idea(BaseModel):
    """One idea in the idea_generator final output."""

    ticker: str
    exchange: str
    name: str
    direction: Literal["long", "short"]
    # NEW: explicit economy + industry tagging for downstream filtering
    economy: str
    industry: str
    industry_file_key: str  # "{economy}_{industry}"
    heuristic_triggered: str
    one_line_thesis: str
    key_catalyst: Optional[str] = None
    time_horizon_days: Optional[int] = None
    overlooked_reason: Optional[str] = None
    supporting_evidence: List[str]
    invalidation: Optional[str] = None
    related_tickers: List[str] = Field(default_factory=list)
    conviction_preliminary: Literal[1, 2, 3]
    kb_sourced: bool = True


class IdeasOutput(BaseModel):
    """
    Full output of idea_generator — persisted to latest_ideas.json.
    NEW: structured Pydantic model; previously stored as freeform markdown+JSON.
    The markdown wrapper is preserved for backward compat in write_latest_ideas()
    but this model is the canonical in-memory form.
    """

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    ideas: List[Idea]
    macro_themes_driving_ideas: List[str]
    total_companies_screened: int
    kb_run_metadata: dict = Field(default_factory=dict)


# ── Settings ──────────────────────────────────────────────────────────────────


class Settings(BaseModel):
    options_mode: bool = False

    # ── Model routing — one model per task profile ────────────────────────────
    # BROAD_SEARCH: web gather + synthesis. Gemini Flash = fast, cheap, native Search grounding.
    broad_search_model: str = "google/gemini-2.5-flash"
    # DEEP_REASONING: KB cross-reference + structured output.
    # Claude Sonnet default — swap to "deepseek/deepseek-r1" to cut cost when ready.
    deep_reasoning_model: str = "anthropic/claude-sonnet-4-6"
    # LONG_MERGE: large-doc faithful merge/edit. Gemini Flash 1M ctx handles large KB files.
    long_merge_model: str = "google/gemini-2.5-flash"
    # HIGH_STAKES is HARDCODED in llm_client.py — not settable here by design.
    # Always claude-sonnet-4-5 direct. Change _HIGH_STAKES_MODEL in llm_client.py if needed.

    max_positions: int = 10
    max_conviction_1_weight: float = 0.05
    max_conviction_2_weight: float = 0.12
    max_conviction_3_weight: float = 0.20

    enabled_economies: List[str] = Field(
        default_factory=lambda: ["us", "uk", "japan", "korea", "global"],
        description="Economies to include in ticker selection and idea generation.",
    )

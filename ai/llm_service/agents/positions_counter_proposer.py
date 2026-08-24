# llm_service/agents/positions_counter_proposer.py
#
# Stateful chat thread attached to a specific proposal.
# The user can:
#   1. Adjust weights with reasons
#   2. Counter-propose holding current positions with a reason
#   3. Have a free-form argument with the LLM
# The LLM defends its thesis using the full KB or concedes if the user's point is valid.

import json
from datetime import datetime, timezone

from models.types import CounterProposal
from tools import kb_manager, llm_client

SYSTEM_PROMPT = """You are a portfolio manager defending or revising a positions proposal.
You have access to the full knowledge base: macro context, industry research, company deep dives, and today's news.

YOUR ROLE:
- Engage seriously with the user's counter-arguments
- If the user makes a VALID POINT you hadn't considered, CONCEDE and explain what you'd change
- If the user's argument is WEAK or based on incomplete information, defend your position with specific evidence
- If the user adjusts a weight, acknowledge it and explain whether you agree with the reasoning
- If the user says "hold current positions", engage with why that might be right or wrong

QUALITY STANDARDS FOR YOUR RESPONSES:
- Never be defensive for its own sake
- Always anchor to specific data points from the KB or your research
- If you don't know something, say so and offer to research it
- Keep responses focused: 3-5 paragraphs maximum unless the argument requires more

TONE: Professional, direct, intellectually honest. Like a smart colleague who will push back."""


async def chat(
    session_id: str, # session_id
    conversation_history: list[dict],
    counter_proposal: CounterProposal,
) -> str:
    """
    Continue a counter-proposal conversation.
    
    conversation_history: [{role: user/assistant, content: str}]
    counter_proposal: includes original proposal, weight adjustments, and current user message
    """
    # Load KB context relevant to the positions being discussed
    tickers = [p["ticker"] for p in counter_proposal.proposal.get("proposed_trades", [])]
    kb_context = await _load_relevant_kb(tickers)
    macro_summary = await _load_macro_summary()

    # Build the full system context
    full_system = f"""{SYSTEM_PROMPT}

ORIGINAL PROPOSAL:
{json.dumps(counter_proposal.proposal, indent=2)[:3000]}

USER'S WEIGHT ADJUSTMENTS:
{_format_adjustments(counter_proposal.weight_adjustments)}

HOLD CURRENT POSITIONS: {counter_proposal.hold_current_positions}
{f'REASON FOR HOLDING: {counter_proposal.hold_current_reason}' if counter_proposal.hold_current_positions else ''}

RELEVANT KNOWLEDGE BASE CONTEXT:
{kb_context}

MACRO CONTEXT:
{macro_summary}

Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"""

    # The conversation history becomes the messages
    messages = list(conversation_history)
    messages.append({
        "role": "user",
        "content": counter_proposal.user_message,
    })

    response = await llm_client.complete(
        function_name="counter_proposer",
        system_prompt=full_system,
        messages=messages,
        max_tokens=3000,
        use_web_search=True,  # Allow real-time lookup if user challenges a data point
        task_profile=llm_client.TaskProfile.HIGH_STAKES
    )

    return response


async def _load_relevant_kb(tickers: list[str]) -> str:
    snippets = []
    for ticker in tickers[:8]:  # Cap to manage tokens
        overview = await kb_manager.read_company_file(ticker, "overview")
        catalysts = await kb_manager.read_company_file(ticker, "catalysts")
        supply_chain = await kb_manager.read_company_file(ticker, "supply_chain")
        if overview or catalysts:
            snippets.append(
                f"**{ticker}**\n"
                f"Overview: {overview[:400]}\n"
                f"Catalysts: {catalysts[:400]}\n"
                f"Supply chain: {supply_chain[:300]}"
            )
    return "\n\n---\n\n".join(snippets) or "(No company KB available)"


async def _load_macro_summary() -> str:
    import re
    parts = []
    for economy in ["us", "uk"]:
        overview = await kb_manager.read_macro_file(economy, "overview")
        if overview:
            signals = re.search(r"### Key Signals(.*?)(?=\n###|\Z)", overview, re.DOTALL)
            if signals:
                parts.append(f"**{economy.upper()}**: {signals.group(1).strip()[:300]}")
    return "\n".join(parts)


def _format_adjustments(adjustments: list) -> str:
    if not adjustments:
        return "No weight adjustments"
    lines = []
    for adj in adjustments:
        direction = "↑" if adj.new_weight > adj.old_weight else "↓"
        lines.append(
            f"- {adj.ticker}: {adj.old_weight*100:.1f}% → {adj.new_weight*100:.1f}% {direction}"
            f"\n  Reason: {adj.reason}"
        )
    return "\n".join(lines)

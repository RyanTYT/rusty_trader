# llm_service

FastAPI service that turns scraped news into portfolio research: it maintains a knowledge base, selects tickers, generates ideas, deep-dives names, and produces a proposal plus a counter-proposal for the Rust backend.

It is **stateless apart from three mounted volumes** and talks to the rest of the system over HTTP — it never touches the trading database.

```text
scraper ──POST /internal/articles_ready──▶ llm_service ──proposals──▶ backend :3000 ──▶ trading-bot
                                                │
                                    ┌───────────┴───────────┐
                                    ▼                       ▼
                            /data/knowledge_base      /data/settings
                            /data/scraped_articles
```

## Pipeline

Each stage is an endpoint in `main.py` backed by an agent module; the backend (or the UI through it) calls them in order:

1. `update_macro` — refresh the macro thesis.
2. `update_industries` — refresh industry notes.
3. `ticker_selector` — pick candidate tickers from the knowledge base.
4. `idea_generator` — turn candidates into trade ideas.
5. `deep_dive` — research a single name in depth.
6. `positions_proposer` — assemble a portfolio proposal from the ideas.
7. `positions_counter_proposer` — critique the proposal before it reaches the bot.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness probe (used by the compose healthcheck). |
| POST | `/internal/articles_ready` | Scraper → service: new articles landed. |
| POST | `/functions/update_macro` | Refresh the macro thesis. |
| POST | `/functions/update_industries` | Refresh industry notes. |
| POST | `/functions/ticker_selector` | Select candidate tickers. |
| GET | `/functions/ticker_selector/seeds` | Current seed list for the selector. |
| POST | `/functions/idea_generator` | Generate trade ideas. |
| POST | `/functions/deep_dive` | Deep-dive one ticker. |
| POST | `/functions/positions_proposer` | Build the portfolio proposal. |
| POST | `/functions/positions_counter_proposer` | Critique the proposal. |
| POST | `/positions/update` | Persist an updated position set. |
| GET | `/positions` | Read the current position set. |
| GET | `/proposal/latest` | Latest proposal. |
| GET | `/settings` | Read prompt/model settings. |
| PUT | `/settings` | Update settings. |

Progress is pushed back to the backend as notifications (`POST $BACKEND_URL/send_notification`) so the UI can stream status.

## Layout

```text
main.py                     FastAPI app, routes, notification plumbing
agents/
  update_macro.py           macro thesis
  update_industries.py      industry notes
  ticker_selector.py        candidate selection
  idea_generator.py         trade ideas
  deep_dive.py              single-name research
  positions_proposer.py     proposal assembly
  positions_proposer_mods/  stages, prompts, enrichment, currency, friction, backend I/O
  positions_counter_proposer.py
api/kb_browser.py           KB tree/file browsing endpoints used by the UI
models/                     pydantic types + persisted settings
tools/
  llm_client.py             Anthropic / OpenRouter / Gemini clients, web-search tool
  kb_manager.py             knowledge-base CRUD and article ingestion
  web_search.py             search helpers
  field_corrector.py        output-schema repair for LLM responses
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `KNOWLEDGE_BASE_PATH` | `/data/knowledge_base` | Curated research KB (markdown tree). |
| `ARTICLES_PATH` | `/data/scraped_articles` | Raw scraper output (`YYYY-MM-DD/`). |
| `SETTINGS_PATH` | `/data/settings` | Persisted `settings.json` (prompts, model choices). |
| `BACKEND_URL` | `http://backend:3000` | Where notifications are posted. |
| `BEARER_TOKEN` | — | Must match the backend's `BACKEND_BEARER_TOKEN`. |
| `ANTHROPIC_API_KEY` | — | Anthropic models + web-search tool. |
| `OPENROUTER_API_KEY` | — | OpenRouter-routed models. |
| `GOOGLE_API_KEY` | — | Gemini. |
| `ALPACA_API_KEY` / `ALPACA_API_SECRET` | — | Market data used during research. |

Provider routing is inside `tools/llm_client.py`; settings choose which model handles each stage.

## Running it

Local development (Python 3.10, [uv](https://docs.astral.sh/uv/)):

```sh
uv sync
uv run uvicorn main:app --reload --port 8001
curl localhost:8001/health
```

The service expects the three data directories to exist; create them locally or point the env vars at your checkouts.

Docker / compose:

```sh
docker build -t rusty_trader/llm-service .
# or, from the repo root:
scripts/up.sh platform     # starts backend, llm_service, scraper, and their volumes
```

In compose the three volumes are named `knowledge_base`, `scraped_articles`, and `llm_settings` (see `compose/platform.yml`).

## Notes

- There are no unit tests in this service; it is exercised through the backend and the compose stack.
- LLM output is schema-checked and repaired (`tools/field_corrector.py`) before it is accepted; settings are the only persisted state.
- The service is deliberately not on the trading path: nothing here can place or cancel an order.

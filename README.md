# rusty_trader

A personal live-trading stack for Interactive Brokers (IBKR). A Rust trading bot ingests real-time bars, runs strategies, places and reconciles orders against the broker, and self-heals through IBKR's nightly restarts; around it sit a Rust REST API, a Python LLM research pipeline, a news scraper, and a Python research/backtest lab.

> **Scope:** run against a paper-trading account at personal scale. This is not production financial infrastructure and makes no claims about fitness for live capital.

## What's in the repo

| Path | What it is | Port | Docs |
| --- | --- | --- | --- |
| [`trading/trading-app/`](trading/trading-app/) | Rust core: sync `ibapi`, SPMC bar rings, strategies, order engine, reconciliation, self-healing | 8000 | [`README`](trading/trading-app/README.md) · [`migrations`](trading/trading-app/migrations/README.md) |
| [`api/backend/`](api/backend/) | Rust REST API for portfolio, strategies, positions, orders, transactions, logs | 3000 | [`README`](api/backend/README.md) |
| [`ai/llm_service/`](ai/llm_service/) | Python FastAPI research pipeline: KB triage → proposal → counter-proposer | 8001 | [`README`](ai/llm_service/README.md) |
| [`ai/scraper/`](ai/scraper/) | Rust scraper: RSS + Playwright, dedupes articles, feeds `llm_service` | — | [`src`](ai/scraper/src/main.rs) |
| [`stratlab/`](stratlab/) | Python research and backtesting lab (bars, costs, metrics, engine) | — | [`README`](stratlab/README.md) |
| [`compose/`](compose/) · [`infra/`](infra/) | Docker stacks and the IB Gateway/IBC image | 4002 | [`README`](compose/README.md) |
| [`scripts/`](scripts/) | Build, release, deploy, and the IBKR data puller | — | [`data_puller`](scripts/data_puller/README.md) |

## Architecture

```mermaid
flowchart LR
    SCRAPER["scraper<br/>(Rust)"] --> ARTICLES[("scraped_articles")]
    ARTICLES --> LLM["llm_service<br/>(Python :8001)"]
    LLM --> KB[("knowledge_base")]
    LLM -->|"proposals"| API["backend<br/>(Rust :3000)"]
    KB --> API
    API -->|"targets"| TP[("trading.target_*_positions")]
    API -->|"REST"| BOT["trading-bot<br/>(Rust :8000)"]
    IBKR["IB Gateway :4002"] <-->|"sync ibapi"| BOT
    BOT -->|"bars, orders,<br/>positions, fills"| DB[("TimescaleDB :5432")]
    API --> DB
```

The defining constraint in `trading-app` is that **`ibapi` is compiled with its `sync` feature**: every IBKR call blocks, so blocking I/O lives on `std::thread`s while DB work runs on tokio, bridged by `Handle`. That split keeps the strategy layer pure. For the reasoning, see [Threading model & sync ibapi](trading/trading-app/README.md#threading-model--sync-ibapi).

Inside the bot, one producer thread per contract pushes 5-second bars into a lock-free SPMC ring; DB and strategy consumers aggregate them independently; the strategy returns a `BarUpdateOutcome`, and the order engine owns placement, FX funding, and reconciliation.

## Quickstart (dev)

Prerequisites: Docker Compose v2, a Rust toolchain (edition 2024), and an IBKR paper account.

```sh
# 1. Configuration
cp .env.example .env            # set DB_USER / DB_PW / DB_DB and any API keys
# IB Gateway credentials: edit infra/ib-gateway/IBCMacos-3.20.0/config.ini locally; never commit it

# 2. Start the trading stack (db, IB Gateway, trading-bot, hotpath-console)
scripts/up.sh trading

# 3. Optionally bring up the API + research services, or everything
scripts/up.sh platform
scripts/up.sh all               # trading + platform + monitoring
```

Ports on the host: `8000` trading-bot, `3000` backend, `8001` llm_service, `4002` IB Gateway (API), `5432` TimescaleDB. Migrations run automatically when the bot starts (`sqlx::migrate!`), so the first boot creates the schema.

`compose/README.md` covers dev builds vs prebuilt images, release tags, deployment, and rollback; `scripts/up.sh tests` starts the `test-db` on port `5433`.

## Tests

```sh
cd trading/trading-app

# Offline unit tests (bulk of coverage)
SQLX_OFFLINE=true cargo test --lib

# Mirrored suites: database, execution, market_data, server, init_app, schedule, helpers
cargo test --test market_data
cargo test --test server

# DB-backed suites (needs test-db; it truncates tables between tests)
TEST_DATABASE_URL=postgres://$DB_USER:$DB_PW@127.0.0.1:5433/$DB_DB \
  cargo test --test database -- --include-ignored
TEST_DATABASE_URL=postgres://$DB_USER:$DB_PW@127.0.0.1:5433/$DB_DB \
  cargo test --test execution -- --include-ignored --skip known_bug

# Backtester (feature-gated)
cargo test --test backtester --features backtest
```

`tests/database` mirrors `src/database` table-by-table plus ops/concurrency suites; `tests/execution` combines Rust tests with JSON scenario suites (order-engine outcomes, syncer passes, event handlers, FX funding). `--skip known_bug` filters tests pinned to known defects.

## Backtesting

```sh
cd trading/trading-app
cargo run --bin backtest --features backtest
```

The binary reads `backtest.json` from the working directory (schema documented at the top of [`src/bin/backtest.rs`](trading/trading-app/src/bin/backtest.rs)). `mode` selects the `db` lane (real TimescaleDB reads/writes, fills at bar close) or the `in_memory` lane (no DB writes); both replay the same strategies.

For research outside Rust, `stratlab` carries the bar/cost/metrics/engine library and the CLI, providing an easy interface for LLMs to work with the architecture design and test, backtest and iterate strategies.

## Where to read next

- [`trading/trading-app/README.md`](trading/trading-app/README.md) — threading model, database/CRUD and transaction layer, execution, lifecycle, testing.
- [`trading/trading-app/migrations/README.md`](trading/trading-app/migrations/README.md) — the schema: every table, index, trigger, and retention option.
- [`compose/README.md`](compose/README.md) — stacks, ports, volumes, release/deploy/rollback.
- [`api/backend/README.md`](api/backend/README.md) — REST routes and how the UI drives strategies.
- [`stratlab/README.md`](stratlab/README.md) — research workflow, cost model, parity checks, and known gaps.

## Design notes & limits

- **Sync `ibapi` is deliberate** — it forces blocking calls off the async runtime, at the cost of a thread-per-contract model and `Handle::block_on` bridges.
- **Strategies are private.** Only the framework, the `manual`/`unknown`/`noise` examples, and the public gates are in this repo; real strategies I'm testing/running aren't included.
- **Paper scale.** Hard-coded SGD base currency and account id, tuned-in-practice constants, and a minimal FX holiday calendar.
- **Thread leaks on timeout.** `sync_timeout` cannot cancel a wedged Rust thread, and a leaked consolidator reference can stall teardown.
- **Stringly `order_ref`.** Parked orders carry `"{strategy}:{price}"` conventions parsed at runtime, all orders submitted carry "{strategy}" as order_ref.

## Journal

The design memoir — reasoning, dead ends, and what I learned building this — is at **[My journey →](https://ryantyt.github.io/RTYT/journal/summer-trading-1/)**.

---

_Built by `RyanTYT`. Rust · Python · IBKR · PostgreSQL/TimescaleDB · tokio · redb · moka._

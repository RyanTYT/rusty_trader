# Compose stacks

The application is split by domain/lifecycle, not by build mode. Each stack is
its own Compose project and can be started, stopped, and rebuilt independently.

| Stack | File | Services | Project |
|---|---|---|---|
| Trading | `compose/trading.yml` | `db`, `trading-bot`, optional `hotpath-console` | `autotrader-trading` |
| Platform | `compose/platform.yml` | `backend`, `llm_service`, `scraper` | `autotrader-platform` |
| Monitoring | `compose/monitoring.yml` | `prometheus`, `alertmanager`, `grafana`, `autoheal` | `autotrader-monitoring` |
| Tests | `compose/tests.yml` | `test-db`, `test-trading-bot`, `backtester`, `optimizer` | `autotrader-tests` |
| Data pulling | `scripts/data_puller/docker-compose.yml` | `tws`, `pull-1min` | `autotrader-data` |

The data-pulling stack is intentionally separate from `compose/`. It has its
own README and does not join the `autotrader` network.

## Prerequisites

- Docker Compose v2.
- A root `.env` (gitignored). Start from `.env.example`.
- The shared external network:

On the current dev VM the daemon requires `sudo -n`. The helper scripts detect
this automatically; if you run raw `docker` commands, prefix them with
`sudo -n` when `docker info` fails.

```bash
docker network create autotrader
```

`scripts/up.sh` creates it automatically for the app stacks.

## Dev build vs prod run

Runtime files contain only `image:`. The `*.build.yml` overlays add `build:`
for dev. Production never passes an overlay.

Dev build:

```bash
docker compose --env-file .env -f compose/trading.yml -f compose/trading.build.yml build
docker compose --env-file .env -f compose/platform.yml -f compose/platform.build.yml build
docker compose --env-file .env -f compose/tests.yml build
```

Or:

```bash
scripts/build.sh trading
scripts/build.sh platform
scripts/build.sh tests
```

Prod run (prebuilt images only):

```bash
docker compose --env-file .env -f compose/trading.yml up -d
```

## Start order

Cross-stack `depends_on` is not possible in Compose, so start in this order:

1. `trading` — owns `db` and `trading-bot`.
2. `platform` — `backend` resolves `db` on the shared network.
3. `monitoring` — Prometheus scrapes `trading-bot:6772`.

```bash
scripts/up.sh all
```

or individually:

```bash
scripts/up.sh trading
scripts/up.sh platform
scripts/up.sh monitoring
```

Stop in reverse order:

```bash
scripts/down.sh all
```

If `platform` starts before `trading`, it still runs, but database calls fail
until `db` is reachable. Prometheus simply retries its scrape.

`autoheal` also lives in the monitoring stack, so unhealthy-container restarts
only happen while monitoring is running.

## Common commands

```bash
# logs
docker compose --env-file .env -f compose/trading.yml logs -f trading-bot

# optional hotpath TUI (attaches to trading-bot's netns)
docker compose --env-file .env -f compose/trading.yml --profile console up -d

# rebuild one service
scripts/rebuild.sh backend
scripts/rebuild.sh trading-bot --no-cache

# status
docker compose --env-file .env -f compose/trading.yml ps
docker compose --env-file .env -f compose/platform.yml ps
docker compose --env-file .env -f compose/monitoring.yml ps
```

`--env-file .env` is explicit because the first compose file lives in
`compose/`; without it Compose would look for `compose/.env`.

## Tests

Tests are dev/CI only. They build inline and use their own network and volume.

```bash
docker compose --env-file .env -f compose/tests.yml run --rm test-trading-bot
docker compose --env-file .env -f compose/tests.yml run --rm backtester
docker compose --env-file .env -f compose/tests.yml run --rm optimizer
```

`run` starts `test-db` automatically. The test job containers publish no host
ports; only `test-db` is exposed on 5433. Use an override if you need host
access to a test container.

## Data pulling

This stack is separate because it owns its own IB Gateway and must keep
`pull-1min` in `network_mode: service:tws`.

```bash
docker compose --env-file .env \
  -f scripts/data_puller/docker-compose.yml \
  -f scripts/data_puller/compose.build.yml build

docker compose --env-file .env -f scripts/data_puller/docker-compose.yml up -d tws
docker compose --env-file .env -f scripts/data_puller/docker-compose.yml run --rm pull-1min
```

Do not run its `tws` while the trading stack is running: both publish
`127.0.0.1:4002`.

## Ports

| Service | Host port | Notes |
|---|---|---|
| `trading-bot` | 7462, 4002, 8000 | IB Gateway API and HTTP |
| `db` | 5432 | TimescaleDB |
| `backend` | 3000 | |
| `llm_service` | 8001 | |
| `alertmanager` | 9093 | |
| `grafana` | 3001 | |
| `test-db` | 5433 | tests only |
| data `tws` | 4002 | separate stack; do not run with trading |

## Volumes

Volumes use explicit names so backups and migration do not depend on Compose
project prefixes:

- `autotrader_pgdata`
- `autotrader_orders-data`
- `autotrader_knowledge_base`
- `autotrader_scraped_articles`
- `autotrader_llm_settings`
- `autotrader_playwright_cache`
- `autotrader_prometheus-data`
- `autotrader_alertmanager-data`
- `autotrader_grafana-data`
- `autotrader_tests_orders`

## Alertmanager config

`infra/alertmanager/alertmanager.yml` is gitignored. By default the stack
mounts `alertmanager.example.yml`, so it starts with placeholder credentials.
Create the real file and set `ALERTMANAGER_CONFIG` in `.env`:

```bash
cp infra/alertmanager/alertmanager.example.yml infra/alertmanager/alertmanager.yml
# edit the token/chat_id
```

```dotenv
ALERTMANAGER_CONFIG=../infra/alertmanager/alertmanager.yml
```

## Paths

With files under `compose/`, the Compose project directory is `compose/`.
Bind mounts therefore use `../infra`, `../data`, and `../scripts`; build
contexts use `..` for repo-root builds. Do not pass `--project-directory .`,
because that would resolve those paths from the repo root and break them.

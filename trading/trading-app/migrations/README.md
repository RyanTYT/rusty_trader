# Database schema (final state)

Single consolidated baseline: `20250729035729_init.sql`. It is the cumulative result of the original
7 migrations, squashed and reorganised into three parts:

1. Tables + indexes
2. Triggers (and the functions they call)
3. Hypertable declarations

Deprecated objects were removed rather than carried forward:

| Removed object | Kind | Why |
|---|---|---|
| `market_data.daily_ohlcv` | Continuous aggregate | Never used by the app |
| `market_data.daily_volatility` | View over `daily_ohlcv` | Depended on the cagg |
| `trading.threshold_rebalancing` | Table | Unused; strategy module not compiled |
| `trading.calendar_rebalancing` | Table | Unused |
| `trading.fractional_momentum_weekly_positions` | Table | Unused; strategy wiring commented out |

The `ALTER DATABASE ... SET timescaledb.enable_cagg_window_functions = TRUE` setting was also dropped,
since it only existed to support the continuous aggregate.

Three schemas: `trading` (positions, orders, transactions, strategy metadata), `market_data` (OHLCV history), `logs` (application logs + cancelled-order audit).

## Enums

| Enum | Values | Used by |
|---|---|---|
| `trading.status` | `active`, `stopping`, `inactive` | `trading.strategy.status` |
| `trading.option_type` | `C` (call), `P` (put) | option positions, orders, transactions, historical options |

---

## `trading` schema

### `trading.strategy`

Strategy registry. One row per strategy. Capital tracking lives in application logic, not here.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) PK | Strategy name (identity = name string) |
| `status` | status NOT NULL | `active` / `stopping` / `inactive` |

### `trading.notifications`

Ad-hoc alert store for the backend service.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGINT GENERATED ALWAYS AS IDENTITY PK | Surrogate key; the same title can be raised repeatedly |
| `title` | TEXT NOT NULL | |
| `body` | TEXT | |
| `alert_type` | TEXT | |

### `trading.current_stock_positions`

Live stock holdings. `last_updated` auto-set on UPDATE via trigger.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |
| `last_updated` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Bumped by `trg_current_stock_positions_last_updated` |

**PK:** `(stock, primary_exchange, currency, strategy)`.

### `trading.current_option_positions`

Live option holdings. Same lifecycle as stock positions.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | `C` / `P` |
| `last_updated` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Bumped by `trg_current_option_positions_last_updated` |

**PK:** `(stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type)`.

### `trading.target_stock_positions`

Desired stock holdings per strategy. Written by `on_bar_update`; read by reconciliation to compute target-vs-current delta.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |

**PK:** `(strategy, primary_exchange, currency, stock)`.

### `trading.target_option_positions`

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | |

**PK:** `(strategy, stock, primary_exchange, currency, expiry, strike, multiplier, option_type)`.

### `trading.open_stock_orders`

Live IBKR stock orders. `order_perm_id = -1` is a sentinel for optimistic rows inserted before IBKR confirmation (replaced by real `perm_id` on `OpenOrder::submitted`).

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `order_perm_id` | INTEGER | IBKR permanent ID; `-1` = pre-confirmation sentinel |
| `order_id` | INTEGER | IBKR ephemeral order ID |
| `time` | TIMESTAMPTZ | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | NOT in PK (nullable in Rust) |
| `quantity` | DOUBLE PRECISION | |
| `filled` | DOUBLE PRECISION | |
| `executions` | TEXT[] NOT NULL DEFAULT '{}' | Array of execution IDs |
| `parent_order_id` | INTEGER NOT NULL DEFAULT 0 | Pre-existing drift; unused in `src` |

**PK:** `(order_perm_id, order_id)`.

### `trading.open_option_orders`

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `order_perm_id` | INTEGER | `-1` = pre-confirmation sentinel |
| `order_id` | INTEGER | |
| `time` | TIMESTAMPTZ | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `quantity` | DOUBLE PRECISION | |
| `filled` | DOUBLE PRECISION | |
| `executions` | TEXT[] NOT NULL DEFAULT '{}' | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | |
| `parent_order_id` | INTEGER NOT NULL DEFAULT 0 | Pre-existing drift; unused in `src` |

**PK:** `(order_perm_id, order_id)`.

### `trading.stock_transactions`

Filled stock executions. Commissions may arrive later via `staged_commissions` triggers.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE RESTRICT | Audit trail must survive strategy deletion |
| `execution_id` | TEXT PK | IBKR execution ID |
| `order_perm_id` | INTEGER | |
| `time` | TIMESTAMPTZ | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `price` | DOUBLE PRECISION | |
| `fees` | NUMERIC(12, 6) | Initially 0; updated by staged-commission trigger |
| `quantity` | DOUBLE PRECISION | |

### `trading.option_transactions`

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE RESTRICT | Audit trail must survive strategy deletion |
| `execution_id` | TEXT PK | |
| `order_perm_id` | INTEGER | |
| `time` | TIMESTAMPTZ | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `price` | DOUBLE PRECISION | |
| `fees` | NUMERIC(12, 6) | |
| `quantity` | DOUBLE PRECISION | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | |

**Unique index:** `(stock, strategy, time, expiry, strike, multiplier, option_type)`. The old `(order_perm_id, time)` unique index was removed: one order can produce several partial fills sharing the order timestamp.

### `trading.staged_commissions`

Holds commissions that arrive before their matching transaction. The `BEFORE INSERT` trigger tries to apply immediately; if no matching transaction exists yet, the row is staged here. The `AFTER INSERT` triggers on the transaction tables drain matching staged rows.

| Column | Type | Notes |
|---|---|---|
| `execution_id` | TEXT PK | |
| `fees` | NUMERIC(12, 6) | |
| `created_at` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | When the commission was staged; exposes rows that never matched |
| `last_updated` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Bumped by `trg_staged_commissions_last_updated` |

---

## `market_data` schema

### `market_data.historical_data`

Intraday 5-minute stock OHLCV. TimescaleDB hypertable.

| Column | Type | Notes |
|---|---|---|
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL | |
| `time` | TIMESTAMPTZ | |
| `open` | DOUBLE PRECISION | |
| `high` | DOUBLE PRECISION | |
| `low` | DOUBLE PRECISION | |
| `close` | DOUBLE PRECISION | |
| `volume` | NUMERIC(30, 6) | |

**PK:** `(stock, primary_exchange, currency, time)`.

**Indexes:** `idx_historical_data_stock_exchange_time (stock, primary_exchange, time)`, `historical_data_time_idx (time DESC)`.

Dropped as redundant: `historical_data_stock (stock)` (prefix of `historical_data_stock_time`), `idx_historical_data_stock_exchange_time_desc` (btree scans backwards), and `historical_data_stock_time (stock, time)` (old artefact from before `primary_exchange`/`currency` joined the key; it was serving 5,230 scans vs 1.7M for the exchange index).

### `market_data.historical_options_data`

Intraday option OHLCV.

| Column | Type | Notes |
|---|---|---|
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | |
| `time` | TIMESTAMPTZ | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | |
| `open` | DOUBLE PRECISION | |
| `high` | DOUBLE PRECISION | |
| `low` | DOUBLE PRECISION | |
| `close` | DOUBLE PRECISION | |
| `volume` | NUMERIC(30, 6) | |

**PK:** `(stock, primary_exchange, currency, expiry, strike, multiplier, option_type, time)`.

### `market_data.historical_forex_data`

Intraday 1-minute forex bid/ask OHLCV.

| Column | Type | Notes |
|---|---|---|
| `pair` | VARCHAR(30) | e.g. `USD.SGD` |
| `time` | TIMESTAMPTZ | |
| `bid_open` | DOUBLE PRECISION | |
| `bid_high` | DOUBLE PRECISION | |
| `bid_low` | DOUBLE PRECISION | |
| `bid_close` | DOUBLE PRECISION | |
| `ask_open` | DOUBLE PRECISION | |
| `ask_high` | DOUBLE PRECISION | |
| `ask_low` | DOUBLE PRECISION | |
| `ask_close` | DOUBLE PRECISION | |

**PK:** `(pair, time)`.

---

## `logs` schema

### `logs.logs`

Application logs. TimescaleDB hypertable with 3-day retention.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGINT GENERATED ALWAYS AS IDENTITY | Surrogate key so same-microsecond lines cannot collide |
| `time` | TIMESTAMPTZ | |
| `level` | VARCHAR(50) | |
| `name` | VARCHAR(100) | |
| `message` | TEXT | |

**PK:** `(time, id)`.

### `logs.cancelled_orders`

Audit trail for cancelled orders.

| Column | Type | Notes |
|---|---|---|
| `time` | TIMESTAMPTZ | |
| `order_perm_id` | INTEGER | |
| `order_id` | INTEGER | |
| `strategy` | VARCHAR(50) FK→strategy ON DELETE RESTRICT | Audit trail must survive strategy deletion |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL | |
| `quantity` | DOUBLE PRECISION | |
| `filled` | DOUBLE PRECISION | |
| `executions` | TEXT[] NOT NULL DEFAULT '{}' | |
| `reason` | VARCHAR | |

**PK:** `(time, order_perm_id, order_id)`.

---

## TimescaleDB artifacts

| Artifact | Type | Details |
|---|---|---|
| `market_data.historical_data` | Hypertable | `create_hypertable('...', 'time')`; compressed, `segmentby = stock, primary_exchange, currency`, `orderby = time DESC` |
| `market_data.historical_options_data` | Hypertable | Same; `segmentby = stock, primary_exchange, currency, expiry, strike, multiplier, option_type` |
| `market_data.historical_forex_data` | Hypertable | Same; `segmentby = pair` |
| `logs.logs` | Hypertable | 1-day chunks (`chunk_time_interval => INTERVAL '1 day'`); not compressed |
| `logs.logs` retention | Retention policy | `add_retention_policy('logs.logs', INTERVAL '3 days')` |
| Compression policies | Policies (3) | One per OHLCV hypertable, `compress_after => INTERVAL '30 days'` |

---

## Triggers

### Staged-commission triggers

Solve the executions-vs-commissions event-ordering race: a commission may arrive before or after its matching transaction. Three triggers cooperate; the **latest commission wins**.

| Trigger | Event | Table | Function |
|---|---|---|---|
| `trg_try_apply_commission_stocks` | BEFORE INSERT OR UPDATE | `trading.staged_commissions` | Locks the matching transaction row (`SELECT … FOR UPDATE`, stock first then option) and overwrites `fees` with `NEW.fees`. If a transaction row exists, returns NULL (do not stage). Otherwise returns NEW (stage the row). |
| `trg_apply_staged_commission_stocks` | AFTER INSERT | `trading.stock_transactions` | When a new stock transaction arrives, a data-modifying CTE copies the latest staged fee onto it and deletes only the staging row(s) it applied. |
| `trg_apply_staged_commission_options` | AFTER INSERT | `trading.option_transactions` | Same as above for option transactions. |

The `FOR UPDATE` lock is what closes the race: a fill committing its transaction and a commission deciding whether to stage serialise on the same row, so the fee is applied exactly once instead of landing on neither or both.

### `last_updated` triggers

| Trigger | Event | Table | Function |
|---|---|---|---|
| `trg_current_stock_positions_last_updated` | BEFORE UPDATE | `trading.current_stock_positions` | Sets `NEW.last_updated = NOW()` |
| `trg_current_option_positions_last_updated` | BEFORE UPDATE | `trading.current_option_positions` | Sets `NEW.last_updated = NOW()` |
| `trg_staged_commissions_last_updated` | BEFORE UPDATE | `trading.staged_commissions` | Sets `NEW.last_updated = NOW()` |

---

## Options

Per-table storage tuning and the server resource profile live in the `OPTIONS` section at the end of the migration.

| Table | Setting | Why |
|---|---|---|
| `trading.open_stock_orders` | `autovacuum_vacuum_scale_factor = 0.01`, `autovacuum_vacuum_threshold = 50` | Orders are inserted then deleted; the default 20% threshold never fires on a table with ~0 live rows, so dead tuples accumulate |
| `trading.open_option_orders` | same | same |

### Resource profiles

The `ENVIRONMENT` section at the top selects a profile; `OPTIONS` applies it.

Resolution order: the session GUC `trading_app.env`, else the `DEFAULT_ENV` constant (`'local'`).

SQL cannot read OS environment variables, so the client forwards one:

```
psql:  PGOPTIONS='-c trading_app.env=small' psql -d trading_system -f ...
Rust:  PgConnectOptions::new().options([
           ("trading_app.env", std::env::var("APP_ENV").unwrap_or("local".into()))
       ])
```

| Parameter | local | small (4 GB) | medium (8 GB) | large (16 GB) |
|---|---|---|---|---|
| `shared_buffers` | 1 GB | 1 GB | 2 GB | 3 GB |
| `effective_cache_size` | 8 GB | 3 GB | 6 GB | 10 GB |
| `maintenance_work_mem` | 512 MB | 256 MB | 512 MB | 1 GB |
| `autovacuum_work_mem` | 256 MB | 128 MB | 256 MB | 256 MB |
| `autovacuum_max_workers` | 3 | 2 | 3 | 3 |
| `work_mem` | 8 MB | 8 MB | 8 MB | 16 MB |
| `max_connections` | 50 | 30 | 40 | 50 |
| `max_worker_processes` | 16 | 12 | 16 | 16 |
| `max_parallel_workers` | 8 | 8 | 8 | 8 |
| `wal_buffers` | 16 MB | 16 MB | 16 MB | 16 MB |
| `timescaledb.max_background_workers` | 6 | 4 | 6 | 8 |

`effective_cache_size`, `maintenance_work_mem`, `work_mem`, `temp_buffers` and `max_parallel_workers` are applied immediately per database. The rest have context `postmaster` (or cannot be set per database, like `autovacuum_work_mem`), so the migration prints them as `ALTER SYSTEM` statements in a `WARNING`; run those once, then `SELECT pg_reload_conf();` (or restart for `shared_buffers`, `max_connections`, `max_worker_processes`, `wal_buffers`, `autovacuum_max_workers`, `timescaledb.max_background_workers`).

---

## Migration history

| # | File | Summary |
|---|---|---|
| 1 | `20250729035729_init.sql` | Consolidated baseline: 3 schemas, 2 enums (`trading.status`, `trading.option_type`), 16 tables, triggers, 4 hypertables, 3 compression policies, autovacuum options |

The pre-squash history (for reference only): base init → `fractional_momentum_weekly_positions` → composite
`historical_data` indexes → `historical_forex_data` → `last_updated` columns/triggers → `currency` columns +
PK recomposition → drop `capital`/`initial_capital` from `strategy`.

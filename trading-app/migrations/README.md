# Database schema (final state)

Cumulative result of applying all 7 migrations in chronological order. Generated from:
1. `20250729035729_init.sql` — base schema, hypertables, continuous aggregate, staged-commission triggers
2. `20251025002212_add_frac_mom_table.sql` — fractional momentum table (legacy)
3. `20251025012201_add_indexes_for_speedup.sql` — composite indexes on `historical_data`
4. `20260114231514_add_forex_data.sql` — forex bid/ask OHLCV table
5. `20260515030804_add_position_update_dt.sql` — `last_updated` column + triggers on position tables
6. `20260523004146_add_currency.sql` — `currency` column on 9 tables + PK recomposition
7. `20260602023150_minimise_strategy.sql` — drop `capital`/`initial_capital` from `strategy`

Three schemas: `trading` (positions, orders, transactions, strategy metadata), `market_data` (OHLCV history), `logs` (application logs + cancelled-order audit).

## Enums

| Enum | Values | Used by |
|---|---|---|
| `status` | `active`, `stopping`, `inactive` | `trading.strategy.status` |
| `option_type` | `C` (call), `P` (put) | option positions, orders, transactions, historical options |

---

## `trading` schema

### `trading.strategy`

Strategy registry. One row per strategy. `capital`/`initial_capital` were dropped (migration 7) — capital tracking moved to application logic.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) PK | Strategy name (identity = name string) |
| `status` | status NOT NULL | `active` / `stopping` / `inactive` |

### `trading.notifications`

Ad-hoc alert store for the backend service.

| Column | Type | Notes |
|---|---|---|
| `title` | TEXT PK | |
| `body` | TEXT | |
| `alert_type` | TEXT | |

### `trading.current_stock_positions`

Live stock holdings. `last_updated` auto-set on UPDATE via trigger.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |
| `last_updated` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Added migration 5 |

**PK:** `(stock, primary_exchange, currency, strategy)` — recomposed in migration 6 to include `currency`.

### `trading.current_option_positions`

Live option holdings. Same lifecycle as stock positions.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | `C` / `P` |
| `last_updated` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Added migration 5 |

**PK:** `(stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type)`.

### `trading.target_stock_positions`

Desired stock holdings per strategy. Written by `on_bar_update`; read by reconciliation to compute target-vs-current delta.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
| `avg_price` | DOUBLE PRECISION | |
| `quantity` | DOUBLE PRECISION | |

**PK:** `(strategy, primary_exchange, currency, stock)`.

### `trading.target_option_positions`

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
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
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6; NOT in PK (nullable in Rust) |
| `quantity` | DOUBLE PRECISION | |
| `filled` | DOUBLE PRECISION | |
| `executions` | TEXT[] NOT NULL DEFAULT '{}' | Array of execution IDs |

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
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
| `quantity` | DOUBLE PRECISION | |
| `filled` | DOUBLE PRECISION | |
| `executions` | TEXT[] NOT NULL DEFAULT '{}' | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | |

**PK:** `(order_perm_id, order_id)`.

### `trading.stock_transactions`

Filled stock executions. Commissions may arrive later via `staged_commissions` triggers.

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `execution_id` | TEXT PK | IBKR execution ID |
| `order_perm_id` | INTEGER | |
| `time` | TIMESTAMPTZ | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
| `price` | DOUBLE PRECISION | |
| `fees` | NUMERIC(12, 6) | Initially 0; updated by staged-commission trigger |
| `quantity` | DOUBLE PRECISION | |

### `trading.option_transactions`

| Column | Type | Notes |
|---|---|---|
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
| `execution_id` | TEXT PK | |
| `order_perm_id` | INTEGER | |
| `time` | TIMESTAMPTZ | |
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
| `price` | DOUBLE PRECISION | |
| `fees` | NUMERIC(12, 6) | |
| `quantity` | DOUBLE PRECISION | |
| `expiry` | VARCHAR(20) | |
| `strike` | DOUBLE PRECISION | |
| `multiplier` | VARCHAR(50) | |
| `option_type` | option_type NOT NULL | |

**Unique indexes:** `(order_perm_id, time)` and `(stock, strategy, time, expiry, strike, multiplier, option_type)`.

### `trading.staged_commissions`

Holds commissions that arrive before their matching transaction. The `BEFORE INSERT` trigger tries to apply immediately; if no matching transaction exists yet, the row is staged here. The `AFTER INSERT` triggers on the transaction tables drain matching staged rows.

| Column | Type | Notes |
|---|---|---|
| `execution_id` | TEXT PK | |
| `fees` | NUMERIC(12, 6) | |

### `trading.fractional_momentum_weekly_positions` — legacy

Added migration 2. The corresponding Rust code (`models_crud/frac_mom_weekly_pos.rs`) references a non-existent macro and is dead. Kept for reference.

| Column | Type | Notes |
|---|---|---|
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `quantity` | DOUBLE PRECISION | |

**PK:** `(stock, primary_exchange)`.

### `trading.threshold_rebalancing` — legacy

Added in init. The corresponding Rust file (`strategy/threshold_rebalancing.rs`) is not declared in `mod.rs` and won't compile against the current trait. Kept for reference.

| Column | Type | Notes |
|---|---|---|
| `time` | TIMESTAMPTZ PK | |
| `threshold_equity_prop_000`–`025` | DOUBLE PRECISION | 26 threshold columns |

### `trading.calendar_rebalancing` — legacy

| Column | Type | Notes |
|---|---|---|
| `time` | TIMESTAMPTZ PK | |
| `calendar_equity_prop` | DOUBLE PRECISION | |

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

**Indexes:** `historical_data_stock (stock)`, `historical_data_stock_time (stock, time)`, `idx_historical_data_stock_exchange_time (stock, primary_exchange, time)`, `idx_historical_data_stock_exchange_time_desc (stock, primary_exchange, time DESC)`.

### `market_data.historical_options_data`

Intraday option OHLCV.

| Column | Type | Notes |
|---|---|---|
| `stock` | VARCHAR(50) | |
| `primary_exchange` | VARCHAR(50) | |
| `currency` | VARCHAR(10) NOT NULL DEFAULT 'USD' | Added migration 6 |
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

**PK:** `(stock, primary_exchange, currency, expiry, strike, multiplier, option_type, time)` — recomposed in migration 6 to include `currency`.

### `market_data.historical_forex_data`

Intraday 1-minute forex bid/ask OHLCV. Added migration 4.

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
| `time` | TIMESTAMPTZ | |
| `level` | VARCHAR(50) | |
| `name` | VARCHAR(100) | |
| `message` | TEXT | |

**PK:** `(time, level, name)`.

### `logs.cancelled_orders`

Audit trail for cancelled orders. Has `currency` from init (unlike most tables).

| Column | Type | Notes |
|---|---|---|
| `time` | TIMESTAMPTZ | |
| `order_perm_id` | INTEGER | |
| `order_id` | INTEGER | |
| `strategy` | VARCHAR(50) FK→strategy ON DELETE CASCADE | |
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
| `market_data.historical_data` | Hypertable | `create_hypertable('...', 'time')` |
| `logs.logs` | Hypertable | 1-day chunks (`chunk_time_interval => INTERVAL '1 day'`) |
| `logs.logs` retention | Retention policy | `add_retention_policy('logs.logs', INTERVAL '3 days')` |
| `market_data.daily_ohlcv` | Continuous aggregate | 1-day OHLCV buckets from `historical_data`; `WITH NO DATA` (refreshed on demand) |
| `market_data.daily_ohlcv` policy | Cagg policy | Refresh every 30 min; `start_offset => 1 month`, `end_offset => 1 hour` |
| `market_data.daily_volatility` | View | 14-day rolling `stddev_samp(close / open)` over `daily_ohlcv`, partitioned by stock/exchange/currency |

---

## Triggers

### Staged-commission triggers

Solve the executions-vs-commissions event-ordering race: a commission may arrive before or after its matching transaction. Three triggers cooperate:

| Trigger | Event | Table | Function |
|---|---|---|---|
| `trg_try_apply_commission_stocks` | BEFORE INSERT OR UPDATE | `trading.staged_commissions` | Tries to apply `NEW.fees` to a matching transaction (stock first, then option) where `fees = 0`. If applied, returns NULL (prevents staging). Otherwise returns NEW (stages the row). |
| `trg_apply_staged_commission_stocks` | AFTER INSERT | `trading.stock_transactions` | When a new stock transaction arrives, checks for a matching staged commission and applies it, then deletes the staging row. |
| `trg_apply_staged_commission_options` | AFTER INSERT | `trading.option_transactions` | Same as above for option transactions. |

### `last_updated` triggers

| Trigger | Event | Table | Function |
|---|---|---|---|
| `trg_current_stock_positions_last_updated` | BEFORE UPDATE | `trading.current_stock_positions` | Sets `NEW.last_updated = NOW()` |
| `trg_current_option_positions_last_updated` | BEFORE UPDATE | `trading.current_option_positions` | Sets `NEW.last_updated = NOW()` |

---

## Migration history

| # | File | Summary |
|---|---|---|
| 1 | `20250729035729_init.sql` | Base schema: 3 schemas, 2 enums, 14 tables, hypertables, continuous aggregate, staged-commission triggers |
| 2 | `20251025002212_add_frac_mom_table.sql` | `fractional_momentum_weekly_positions` table (legacy) |
| 3 | `20251025012201_add_indexes_for_speedup.sql` | Composite indexes on `historical_data` (stock + exchange + time) |
| 4 | `20260114231514_add_forex_data.sql` | `historical_forex_data` table (bid/ask OHLCV) |
| 5 | `20260515030804_add_position_update_dt.sql` | `last_updated` column + triggers on position tables |
| 6 | `20260523004146_add_currency.sql` | `currency` column on 9 tables; PK recomposition to include `currency` on 5 tables |
| 7 | `20260602023150_minimise_strategy.sql` | Drop `capital`/`initial_capital` from `strategy` |

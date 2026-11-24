-- Consolidated baseline. Structure: (1) tables + indexes, (2) triggers, (3) hypertables.
--
-- Removed as deprecated (were never used by the live app):
--   market_data.daily_ohlcv                      (continuous aggregate)
--   market_data.daily_volatility                 (view over daily_ohlcv)
--   trading.threshold_rebalancing
--   trading.calendar_rebalancing
--   trading.fractional_momentum_weekly_positions
--
-- Also removed: the ALTER DATABASE ... enable_cagg_window_functions setting, which only
-- existed to support the daily_ohlcv continuous aggregate.
------------------------------------- ENVIRONMENT -------------------------------------
-- Which resource profile the OPTIONS section at the bottom applies.
--
-- Resolution order:
--   1. session GUC `trading_app.env` (forwarded by the client, see below)
--   2. the literal DEFAULT_ENV in this block
--
-- Valid values for DEFAULT_ENV / trading_app.env (full parameter values are in
-- the OPTIONS section at the bottom of this file):
--
--   local   default. Tuned for the 16 GB development machine.
--             1 GB shared_buffers, 8 GB effective_cache_size, 512 MB maintenance,
--             50 connections, 6 Timescale background workers.
--   small   a 4 GB RAM server.
--             1 GB shared_buffers, 3 GB effective_cache_size, 256 MB maintenance,
--             30 connections, 4 Timescale background workers.
--   medium  an 8 GB RAM server.
--             2 GB shared_buffers, 6 GB effective_cache_size, 512 MB maintenance,
--             40 connections, 6 Timescale background workers.
--   large   a 16 GB RAM server.
--             3 GB shared_buffers, 10 GB effective_cache_size, 1 GB maintenance,
--             50 connections, 8 Timescale background workers.
--
-- Any other value (typo, empty string) falls through to the `local` profile in
-- OPTIONS rather than failing the migration.
--
-- SQL cannot read OS environment variables, so the client must forward one:
--   psql:  PGOPTIONS='-c trading_app.env=small' psql -d trading_system -f ...
--   Rust:  PgConnectOptions::new().options(
--              [("trading_app.env", std::env::var("APP_ENV").unwrap_or("local".into()))])
DO $env$
DECLARE
    default_env CONSTANT text := 'small';
    env text;
BEGIN
    env := coalesce(nullif(current_setting('trading_app.env', true), ''), default_env);
    PERFORM set_config('trading_app.env', env, false);
END
$env$;
------------------------------------- ENVIRONMENT -------------------------------------
------------------------------------- TIMESCALE DB INIT -------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;
------------------------------------- TIMESCALE DB INIT -------------------------------------

-- Create schema
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS logs;

------------------------------------- TRADING -------------------------------------

-- Enums live in the trading schema: an unqualified CREATE TYPE would otherwise
-- land in whatever schema is first on the migrator's search_path (public today).
CREATE TYPE status AS ENUM ('active', 'stopping', 'inactive');
CREATE TYPE option_type AS ENUM ('C', 'P');

-- Notifications table
-- id is a surrogate key so the same title can be raised more than once.
CREATE TABLE trading.notifications (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    alert_type TEXT NOT NULL
);

-- Strategy table
CREATE TABLE trading.strategy (
    strategy VARCHAR(50) PRIMARY KEY,
    status status NOT NULL
);

-- Current stock positions
CREATE TABLE trading.current_stock_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (stock, primary_exchange, currency, strategy)
);
CREATE INDEX current_positions_strategy ON trading.current_stock_positions(strategy);

-- Current option positions: want this cos i want to be able to filter by the columns
CREATE TABLE trading.current_option_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type)
);
CREATE INDEX current_option_positions_expiry ON trading.current_option_positions(expiry);

-- Target stock positions
CREATE TABLE trading.target_stock_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (strategy, primary_exchange, currency, stock)
);
CREATE INDEX target_positions_strategy ON trading.target_stock_positions(strategy);

-- Target Option Positions
CREATE TABLE trading.target_option_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    PRIMARY KEY (strategy, stock, primary_exchange, currency, expiry, strike, multiplier, option_type)
);
CREATE INDEX target_option_positions_expiry ON trading.target_option_positions(expiry);

-- Open Stock Orders
CREATE TABLE trading.open_stock_orders (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,
    order_perm_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',

    quantity DOUBLE PRECISION NOT NULL,
    filled DOUBLE PRECISION NOT NULL,
    executions TEXT[] NOT NULL DEFAULT '{}',
    parent_order_id INTEGER NOT NULL DEFAULT 0, -- pre-existing drift, unused in src

    PRIMARY KEY (order_perm_id, order_id)
);
CREATE INDEX open_orders_strategy ON trading.open_stock_orders(strategy);

-- Open Option Orders
CREATE TABLE trading.open_option_orders (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,
    order_perm_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',

    quantity DOUBLE PRECISION NOT NULL,
    filled DOUBLE PRECISION NOT NULL,
    executions TEXT[] NOT NULL DEFAULT '{}',
    parent_order_id INTEGER NOT NULL DEFAULT 0, -- pre-existing drift, unused in src

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    PRIMARY KEY (order_perm_id, order_id)
);
CREATE INDEX open_option_orders_expiry ON trading.open_option_orders(expiry);

-- Stock Transactions
-- ON DELETE RESTRICT: executed trades are an audit trail, deleting a strategy
-- must not silently erase them.
CREATE TABLE trading.stock_transactions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE RESTRICT,
    execution_id TEXT NOT NULL PRIMARY KEY,
    order_perm_id INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    price DOUBLE PRECISION NOT NULL,
    fees NUMERIC(12, 6) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL
);
CREATE INDEX transactions_stock ON trading.stock_transactions(stock, time);
CREATE INDEX transactions_strategy ON trading.stock_transactions(strategy, time);

-- Option Transactions
-- ON DELETE RESTRICT: executed trades are an audit trail, deleting a strategy
-- must not silently erase them.
CREATE TABLE trading.option_transactions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE RESTRICT,
    execution_id TEXT NOT NULL PRIMARY KEY,
    order_perm_id INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    price DOUBLE PRECISION NOT NULL,
    fees NUMERIC(12, 6) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL
);
-- Removed: (order_perm_id, time) is not unique in practice — a single order can
-- produce several partial fills sharing the order timestamp.
CREATE UNIQUE INDEX option_transactions_unique
    ON trading.option_transactions(stock, strategy, time, expiry, strike, multiplier, option_type);

-- Staged Commissions table - to handle race conditions of commissions coming in
CREATE TABLE trading.staged_commissions (
    execution_id TEXT NOT NULL PRIMARY KEY,
    fees NUMERIC(12, 6) NOT NULL,

    -- created_at exposes a commission that never matched a transaction;
    -- last_updated is bumped by trg_staged_commissions_last_updated below.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
------------------------------------- TRADING -------------------------------------

------------------------------------- MARKET_DATA -------------------------------------
-- Historical Data
-- 5 Min
CREATE TABLE market_data.historical_data (
    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume NUMERIC(30, 6) NOT NULL,
    PRIMARY KEY (stock, primary_exchange, currency, time)
);
-- Removed: redundant, (stock) is a prefix of historical_data_stock_time (stock, time)
-- CREATE INDEX historical_data_stock ON market_data.historical_data(stock);
-- Removed: historical_data_stock_time (stock, time) — superseded by the PK and by
-- idx_historical_data_stock_exchange_time; it was an old artefact from before
-- primary_exchange/currency were part of the key.
CREATE INDEX idx_historical_data_stock_exchange_time
ON market_data.historical_data (stock, primary_exchange, time);
-- Removed: duplicate of idx_historical_data_stock_exchange_time; btree scans backwards for DESC
-- CREATE INDEX idx_historical_data_stock_exchange_time_desc
-- ON market_data.historical_data (stock, primary_exchange, time DESC);

-- Pre-existing on the live cluster but never captured in a migration; kept for time-range scans.
CREATE INDEX IF NOT EXISTS historical_data_time_idx
ON market_data.historical_data (time DESC);

-- Historical Options Data
CREATE TABLE market_data.historical_options_data (
    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    time TIMESTAMPTZ NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume NUMERIC(30, 6) NOT NULL,

    PRIMARY KEY (stock, primary_exchange, currency, expiry, strike, multiplier, option_type, time)
);
CREATE INDEX historical_options_data_stock ON market_data.historical_options_data(stock);
CREATE INDEX historical_options_data_stock_time ON market_data.historical_options_data(stock, time);

-- Historical Forex Data
CREATE TABLE market_data.historical_forex_data (
    pair VARCHAR(30) NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    bid_open DOUBLE PRECISION,
    bid_high DOUBLE PRECISION,
    bid_low DOUBLE PRECISION,
    bid_close DOUBLE PRECISION,
    ask_open DOUBLE PRECISION,
    ask_high DOUBLE PRECISION,
    ask_low DOUBLE PRECISION,
    ask_close DOUBLE PRECISION,

    -- is_complete BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (pair, time)
);
------------------------------------- MARKET_DATA -------------------------------------

------------------------------------- LOGS -------------------------------------

-- id exists so two log lines written in the same microsecond with the same
-- level/name cannot collide; logging must never fail on a unique violation.
CREATE TABLE logs.logs (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    time TIMESTAMPTZ NOT NULL,
    level VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    PRIMARY KEY (time, id)
);

------------------------------------- LOGS -------------------------------------

------------------------------------- MISCELLANEOUS (strategy specific) -------------------------------------
CREATE TABLE logs.cancelled_orders (
    time TIMESTAMPTZ NOT NULL,
    order_perm_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,

    -- ON DELETE RESTRICT: cancelled orders are part of the audit trail.
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE RESTRICT,
    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL,

    quantity DOUBLE PRECISION NOT NULL,
    filled DOUBLE PRECISION NOT NULL,
    executions TEXT[] NOT NULL DEFAULT '{}',

    reason VARCHAR NOT NULL,

    PRIMARY KEY (time, order_perm_id, order_id)
);
------------------------------------- MISCELLANEOUS (strategy specific) -------------------------------------

------------------------------------- TRIGGERS -------------------------------------

-- Position timestamps: bump last_updated on every UPDATE
CREATE OR REPLACE FUNCTION trading.set_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_current_stock_positions_last_updated
BEFORE UPDATE ON trading.current_stock_positions
FOR EACH ROW
EXECUTE FUNCTION trading.set_last_updated();

CREATE TRIGGER trg_current_option_positions_last_updated
BEFORE UPDATE ON trading.current_option_positions
FOR EACH ROW
EXECUTE FUNCTION trading.set_last_updated();

CREATE TRIGGER trg_staged_commissions_last_updated
BEFORE UPDATE ON trading.staged_commissions
FOR EACH ROW
EXECUTE FUNCTION trading.set_last_updated();

-- Staged commissions: apply late-arriving fees to already-recorded transactions
CREATE OR REPLACE FUNCTION trading.apply_staged_commission_stocks()
RETURNS TRIGGER AS $$
BEGIN
    -- Try to apply a matching staged commission
    UPDATE trading.stock_transactions
    SET fees = sc.fees
    FROM trading.staged_commissions sc
    WHERE trading.stock_transactions.execution_id = NEW.execution_id
        AND sc.execution_id = NEW.execution_id;

    -- Delete the staging row if matched
    DELETE FROM trading.staged_commissions
    WHERE execution_id = NEW.execution_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trading.apply_staged_commission_options()
RETURNS TRIGGER AS $$
BEGIN
    -- Try to apply a matching staged commission
    UPDATE trading.option_transactions
    SET fees = sc.fees
    FROM trading.staged_commissions sc
    WHERE trading.option_transactions.execution_id = NEW.execution_id
        AND sc.execution_id = NEW.execution_id;

    -- Delete the staging row if matched
    DELETE FROM trading.staged_commissions
    WHERE execution_id = NEW.execution_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trading.try_apply_commission_to_transaction()
RETURNS TRIGGER AS $$
BEGIN
    -- Attempt to apply commission if matching transaction exists
    UPDATE trading.stock_transactions
    SET fees = NEW.fees
    WHERE execution_id = NEW.execution_id
    AND trading.stock_transactions.fees = 0.0;

    -- If successful (i.e., a row was updated), delete from staging
    IF FOUND THEN
        RETURN NULL; -- Prevents insert into staged_commissions
    ELSE
        UPDATE trading.option_transactions
        SET fees = NEW.fees
        WHERE execution_id = NEW.execution_id
        AND trading.option_transactions.fees = 0.0;

        IF FOUND THEN
            RETURN NULL;
        ELSE
            RETURN NEW;  -- Keep the staging row
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_apply_staged_commission_stocks
AFTER INSERT ON trading.stock_transactions
FOR EACH ROW EXECUTE FUNCTION trading.apply_staged_commission_stocks();

CREATE TRIGGER trg_apply_staged_commission_options
AFTER INSERT ON trading.option_transactions
FOR EACH ROW EXECUTE FUNCTION trading.apply_staged_commission_options();

CREATE TRIGGER trg_try_apply_commission_stocks
BEFORE INSERT OR UPDATE ON trading.staged_commissions
FOR EACH ROW EXECUTE FUNCTION trading.try_apply_commission_to_transaction();

------------------------------------- TRIGGERS -------------------------------------

------------------------------------- HYPERTABLES -------------------------------------

SELECT create_hypertable('market_data.historical_data', 'time', if_not_exists => TRUE);
SELECT create_hypertable('market_data.historical_options_data', 'time', if_not_exists => TRUE);
SELECT create_hypertable('market_data.historical_forex_data', 'time', if_not_exists => TRUE);

SELECT create_hypertable('logs.logs', 'time', chunk_time_interval => INTERVAL '1 day');
SELECT add_retention_policy('logs.logs', INTERVAL '3 days');

-- Compression: chunks older than the policy threshold are rewritten columnar.
-- segmentby = identity of one series; orderby = sort order within each series.
ALTER TABLE market_data.historical_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'stock, primary_exchange, currency',
    timescaledb.compress_orderby = 'time DESC'
);

ALTER TABLE market_data.historical_options_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'stock, primary_exchange, currency, expiry, strike, multiplier, option_type',
    timescaledb.compress_orderby = 'time DESC'
);

ALTER TABLE market_data.historical_forex_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pair',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('market_data.historical_data', INTERVAL '30 days');
SELECT add_compression_policy('market_data.historical_options_data', INTERVAL '30 days');
SELECT add_compression_policy('market_data.historical_forex_data', INTERVAL '30 days');

------------------------------------- HYPERTABLES -------------------------------------

------------------------------------- OPTIONS -------------------------------------
-- Per-table storage/autovacuum tuning.
--
-- open_stock_orders / open_option_orders are the only high-churn tables in the
-- schema: rows are inserted when an order is placed and deleted when it fills or
-- is cancelled. That leaves almost no live rows but a steady stream of dead
-- tuples, which the default autovacuum threshold (20% of a tiny table) never
-- triggers on. Tighten the scale factor so dead tuples are reclaimed promptly
-- instead of bloating the relation and its indexes.
ALTER TABLE trading.open_stock_orders SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50
);

ALTER TABLE trading.open_option_orders SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50
);

-- Resource profile (selected in the ENVIRONMENT section at the top of this file).
--
-- Only session-level parameters can be applied from a migration. Parameters
-- whose context is postmaster (shared_buffers, max_connections, ...) or that
-- cannot be set per database (autovacuum_work_mem) are rendered as ALTER SYSTEM
-- statements in a WARNING: run them once, then reload/restart the server.
DO $opt$
DECLARE
    env  text := coalesce(nullif(current_setting('trading_app.env', true), ''), 'local');
    cfg  jsonb;
    k    text;
    v    text;
    todo text := '';
    per_db CONSTANT text[] := ARRAY[
        'effective_cache_size', 'maintenance_work_mem', 'work_mem',
        'temp_buffers', 'max_parallel_workers'
    ];
BEGIN
    cfg := CASE env
        -- 4 GB RAM server
        WHEN 'small' THEN '{
            "shared_buffers":"1GB","effective_cache_size":"3GB",
            "maintenance_work_mem":"256MB","autovacuum_work_mem":"128MB",
            "autovacuum_max_workers":2,"work_mem":"8MB","max_connections":30,
            "max_worker_processes":12,"max_parallel_workers":8,
            "wal_buffers":"16MB","timescaledb.max_background_workers":4}'::jsonb
        -- 8 GB RAM server
        WHEN 'medium' THEN '{
            "shared_buffers":"2GB","effective_cache_size":"6GB",
            "maintenance_work_mem":"512MB","autovacuum_work_mem":"256MB",
            "autovacuum_max_workers":3,"work_mem":"8MB","max_connections":40,
            "max_worker_processes":16,"max_parallel_workers":8,
            "wal_buffers":"16MB","timescaledb.max_background_workers":6}'::jsonb
        -- 16 GB RAM server
        WHEN 'large' THEN '{
            "shared_buffers":"3GB","effective_cache_size":"10GB",
            "maintenance_work_mem":"1GB","autovacuum_work_mem":"256MB",
            "autovacuum_max_workers":3,"work_mem":"16MB","max_connections":50,
            "max_worker_processes":16,"max_parallel_workers":8,
            "wal_buffers":"16MB","timescaledb.max_background_workers":8}'::jsonb
        -- local / default: 16 GB development machine (also the fallback for typos)
        ELSE '{
            "shared_buffers":"1GB","effective_cache_size":"8GB",
            "maintenance_work_mem":"512MB","autovacuum_work_mem":"256MB",
            "autovacuum_max_workers":3,"work_mem":"8MB","max_connections":50,
            "max_worker_processes":16,"max_parallel_workers":8,
            "wal_buffers":"16MB","timescaledb.max_background_workers":6}'::jsonb
    END;

    -- Applied now, per database; effective for new connections.
    FOREACH k IN ARRAY per_db LOOP
        IF cfg ? k THEN
            v := cfg->>k;
            EXECUTE format('ALTER DATABASE %I SET %I = %L', current_database(), k, v);
        END IF;
    END LOOP;

    -- Everything else cannot be applied from inside a migration.
    FOR k, v IN
        SELECT key, value FROM jsonb_each_text(cfg) WHERE key <> ALL (per_db)
    LOOP
        todo := todo || format(E'\n  ALTER SYSTEM SET %I = %L;', k, v);
    END LOOP;

    RAISE WARNING E'Resource profile "%" applied (per-database settings).\nRun these outside a transaction, then reload/restart:%', env, todo;
END
$opt$;
------------------------------------- OPTIONS -------------------------------------

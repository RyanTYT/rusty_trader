------------------------------------- TIMESCALE DB INIT -------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;
ALTER DATABASE trading_system SET timescaledb.enable_cagg_window_functions = TRUE;
------------------------------------- TIMESCALE DB INIT -------------------------------------

-- Create schema
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS logs;

------------------------------------- TRADING -------------------------------------

-- Enums
CREATE TYPE status AS ENUM ('active', 'stopping', 'inactive');
CREATE TYPE option_type AS ENUM ('C', 'P');

-- Notifications table
CREATE TABLE trading.notifications (
    title TEXT NOT NULL PRIMARY KEY,
    body TEXT NOT NULL,
    alert_type TEXT NOT NULL
);

-- Strategy table
CREATE TABLE trading.strategy (
    strategy VARCHAR(50) PRIMARY KEY,
    capital DOUBLE PRECISION NOT NULL,
    initial_capital DOUBLE PRECISION NOT NULL,
    status status NOT NULL
);

-- Current stock positions
CREATE TABLE trading.current_stock_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (strategy, stock, primary_exchange)
);
CREATE INDEX current_positions_strategy ON trading.current_stock_positions(strategy);

-- Current option positions: want this cos i want to be able to filter by the columns
CREATE TABLE trading.current_option_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    PRIMARY KEY (strategy, stock, primary_exchange, expiry, strike, multiplier, option_type)
);
CREATE INDEX current_option_positions_expiry ON trading.current_option_positions(expiry);

-- Target stock positions
CREATE TABLE trading.target_stock_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (stock, primary_exchange, strategy)
);
CREATE INDEX target_positions_strategy ON trading.target_stock_positions(strategy);

-- Target Option Positions
CREATE TABLE trading.target_option_positions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    PRIMARY KEY (stock, primary_exchange, strategy, expiry, strike, multiplier, option_type)
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

    quantity DOUBLE PRECISION NOT NULL,
    filled DOUBLE PRECISION NOT NULL,
    executions TEXT[] NOT NULL,

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

    quantity DOUBLE PRECISION NOT NULL,
    filled DOUBLE PRECISION NOT NULL,
    executions TEXT[] NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL,

    PRIMARY KEY (order_perm_id, order_id)
);
CREATE INDEX open_option_orders_expiry ON trading.open_option_orders(expiry);

-- Stock Transactions
CREATE TABLE trading.stock_transactions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,
    execution_id TEXT NOT NULL PRIMARY KEY,
    order_perm_id INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    fees NUMERIC(12, 6) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL
);
CREATE INDEX transactions_stock ON trading.stock_transactions(stock, time);
CREATE INDEX transactions_strategy ON trading.stock_transactions(strategy, time);

-- Option Transactions
CREATE TABLE trading.option_transactions (
    strategy VARCHAR(50) NOT NULL REFERENCES trading.strategy(strategy) ON DELETE CASCADE,
    execution_id TEXT NOT NULL PRIMARY KEY,
    order_perm_id INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    fees NUMERIC(12, 6) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,

    expiry VARCHAR(20) NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    multiplier VARCHAR(50) NOT NULL,
    option_type option_type NOT NULL
);
CREATE UNIQUE INDEX option_transactions_unique_order_perm_id
    ON trading.option_transactions(order_perm_id, time);
CREATE UNIQUE INDEX option_transactions_unique
    ON trading.option_transactions(stock, strategy, time, expiry, strike, multiplier, option_type);

CREATE TABLE trading.staged_commissions (
    execution_id TEXT NOT NULL PRIMARY KEY,
    fees NUMERIC(12, 6) NOT NULL
);
------------------------------------- TRADING -------------------------------------

------------------------------------- MARKET_DATA -------------------------------------
-- Historical Data
-- 5 Min
CREATE TABLE market_data.historical_data (
    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume NUMERIC(30, 6) NOT NULL,
    PRIMARY KEY (stock, primary_exchange, time)
);
CREATE INDEX historical_data_stock ON market_data.historical_data(stock);
CREATE INDEX historical_data_stock_time ON market_data.historical_data(stock, time);

-- 1 Day
CREATE TABLE market_data.daily_historical_data (
    stock VARCHAR(50) NOT NULL,
    time TIMESTAMPTZ NOT NULL,

    open NUMERIC(20, 15) NOT NULL,
    high NUMERIC(20, 15) NOT NULL,
    low NUMERIC(20, 15) NOT NULL,
    close NUMERIC(20, 15) NOT NULL,
    volume NUMERIC(30, 6) NOT NULL,
    PRIMARY KEY (stock, time)
);
CREATE INDEX daily_historical_data_stock ON market_data.daily_historical_data(stock);
CREATE INDEX daily_historical_data_stock_time ON market_data.daily_historical_data(stock, time);

CREATE TABLE market_data.historical_options_data (
    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
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

    PRIMARY KEY (stock, primary_exchange, expiry, strike, multiplier, option_type, time)
);
CREATE INDEX historical_options_data_stock ON market_data.historical_options_data(stock);
CREATE INDEX historical_options_data_stock_time ON market_data.historical_options_data(stock, time);
------------------------------------- MARKET_DATA -------------------------------------

------------------------------------- LOGS -------------------------------------
CREATE TABLE logs.logs (
    time TIMESTAMPTZ NOT NULL,
    level VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,

    PRIMARY KEY (time, level, name)
);
------------------------------------- LOGS -------------------------------------

------------------------------------- TIMESCALE DB -------------------------------------
SELECT create_hypertable('market_data.historical_data', 'time', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.daily_ohlcv
WITH (timescaledb.continuous) AS 
SELECT
    stock,
    primary_exchange,
    time_bucket('1 day', time) AS day,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume
FROM market_data.historical_data
GROUP BY stock, primary_exchange, day
WITH NO DATA;

CREATE OR REPLACE VIEW market_data.daily_volatility AS
SELECT
    stock,
    primary_exchange,
    day,
    stddev_samp(close / open) OVER (
        PARTITION BY stock
        ORDER BY day
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) AS rolling_volatility
FROM market_data.daily_ohlcv; -- Querying the continuous aggregate

SELECT add_continuous_aggregate_policy('market_data.daily_ohlcv',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);
------------------------------------- TIMESCALE DB -------------------------------------

-- ------------------------------------- VIEWS -------------------------------------
-- -- ============================
-- -- CURRENT OPTION POSITIONS VIEW
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.current_option_positions_view AS
-- SELECT
--     cp.*,
--     cop.expiry,
--     cop.strike,
--     cop.multiplier,
--     cop.option_type,
--     FALSE as do_update
-- FROM trading.current_positions cp
-- JOIN trading.current_option_positions cop ON cp.id = cop.id
-- WHERE cp.asset_type = 'option';
--
-- -- Trigger functions
-- CREATE OR REPLACE FUNCTION trading.current_option_positions_insert_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     IF NEW.do_update THEN
--         INSERT INTO trading.current_positions(strategy, asset_type, stock, avg_price, quantity)
--         VALUES (NEW.strategy, 'option', NEW.stock, NEW.avg_price, NEW.quantity)
--         RETURNING id INTO NEW.id;
--
--         INSERT INTO trading.current_option_positions(id, expiry, strike, multiplier, option_type)
--         VALUES (NEW.id, NEW.expiry, NEW.strike, NEW.multiplier, NEW.option_type);
--
--         RETURN NEW;
--     ELSE
--         INSERT INTO trading.current_positions(strategy, asset_type, stock, avg_price, quantity)
--         VALUES (NEW.strategy, 'option', NEW.stock, NEW.avg_price, NEW.quantity)
--         RETURNING id INTO NEW.id;
--
--         INSERT INTO trading.current_option_positions(id, expiry, strike, multiplier, option_type)
--         VALUES (NEW.id, NEW.expiry, NEW.strike, NEW.multiplier, NEW.option_type);
--
--         RETURN NEW;
--     END IF;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.current_option_positions_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE trading.current_positions
--     SET strategy = NEW.strategy,
--         stock = NEW.stock,
--         avg_price = NEW.avg_price,
--         quantity = NEW.quantity
--     WHERE id = OLD.id;
--
--     UPDATE trading.current_option_positions
--     SET expiry = NEW.expiry,
--         strike = NEW.strike,
--         multiplier = NEW.multiplier,
--         option_type = NEW.option_type
--     WHERE id = OLD.id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.current_option_positions_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.current_positions WHERE id = OLD.id;
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- -- Triggers on the view
-- CREATE TRIGGER trg_current_option_positions_insert
-- INSTEAD OF INSERT ON trading.current_option_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.current_option_positions_insert_trigger();
--
-- CREATE TRIGGER trg_current_option_positions_update
-- INSTEAD OF UPDATE ON trading.current_option_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.current_option_positions_update_trigger();
--
-- CREATE TRIGGER trg_current_option_positions_delete
-- INSTEAD OF DELETE ON trading.current_option_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.current_option_positions_delete_trigger();
--
-- -- ============================
-- -- TARGET OPTION POSITIONS VIEW
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.target_option_positions_view AS
-- SELECT
--     tp.*,
--     top.expiry,
--     top.strike,
--     top.multiplier,
--     top.option_type
-- FROM trading.target_positions tp
-- JOIN trading.target_option_positions top ON tp.id = top.id
-- WHERE tp.asset_type = 'option';
--
-- -- Trigger functions
-- CREATE OR REPLACE FUNCTION trading.target_option_positions_insert_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     INSERT INTO trading.target_positions(strategy, asset_type, stock, avg_price, quantity)
--     VALUES (NEW.strategy, 'option', NEW.stock, NEW.avg_price, NEW.quantity)
--     RETURNING id INTO NEW.id;
--
--     INSERT INTO trading.target_option_positions(id, expiry, strike, multiplier, option_type)
--     VALUES (NEW.id, NEW.expiry, NEW.strike, NEW.multiplier, NEW.option_type);
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.target_option_positions_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE trading.target_positions
--     SET strategy = NEW.strategy,
--         stock = NEW.stock,
--         avg_price = NEW.avg_price,
--         quantity = NEW.quantity
--     WHERE id = OLD.id;
--
--     UPDATE trading.target_option_positions
--     SET expiry = NEW.expiry,
--         strike = NEW.strike,
--         multiplier = NEW.multiplier,
--         option_type = NEW.option_type
--     WHERE id = OLD.id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.target_option_positions_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.target_positions WHERE id = OLD.id;
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- -- Triggers on the view
-- CREATE TRIGGER trg_target_option_positions_insert
-- INSTEAD OF INSERT ON trading.target_option_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.target_option_positions_insert_trigger();
--
-- CREATE TRIGGER trg_target_option_positions_update
-- INSTEAD OF UPDATE ON trading.target_option_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.target_option_positions_update_trigger();
--
-- CREATE TRIGGER trg_target_option_positions_delete
-- INSTEAD OF DELETE ON trading.target_option_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.target_option_positions_delete_trigger();
--
-- -- ============================
-- -- OPEN OPTION ORDERS VIEW
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.open_option_orders_view AS
-- SELECT
--     oo.*,
--     ooo.expiry,
--     ooo.strike,
--     ooo.multiplier,
--     ooo.option_type
-- FROM trading.open_orders oo
-- JOIN trading.open_option_orders ooo ON oo.id = ooo.id
-- WHERE oo.asset_type = 'option';
--
-- -- Trigger functions for INSERT, UPDATE, DELETE
--
-- CREATE OR REPLACE FUNCTION trading.open_option_orders_insert_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     INSERT INTO trading.open_orders(strategy, order_perm_id, order_id, time, asset_type, stock, quantity)
--     VALUES (NEW.strategy, NEW.order_perm_id, NEW.order_id, NEW.time, 'option', NEW.stock, NEW.quantity)
--     RETURNING id INTO NEW.id;
--
--     INSERT INTO trading.open_option_orders(id, expiry, strike, multiplier, option_type)
--     VALUES (NEW.id, NEW.expiry, NEW.strike, NEW.multiplier, NEW.option_type);
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.open_option_orders_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE trading.open_orders
--     SET strategy = NEW.strategy,
--         order_perm_id = NEW.order_perm_id,
--         order_id = NEW.order_perm_id,
--         time = NEW.time,
--         stock = NEW.stock,
--         quantity = NEW.quantity
--     WHERE id = OLD.id;
--
--     UPDATE trading.open_option_orders
--     SET expiry = NEW.expiry,
--         strike = NEW.strike,
--         multiplier = NEW.multiplier,
--         option_type = NEW.option_type
--     WHERE id = OLD.id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.open_option_orders_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.open_orders WHERE id = OLD.id;
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
--
-- -- Attach triggers to the view
-- CREATE TRIGGER trg_open_option_orders_insert
-- INSTEAD OF INSERT ON trading.open_option_orders_view
-- FOR EACH ROW EXECUTE FUNCTION trading.open_option_orders_insert_trigger();
--
-- CREATE TRIGGER trg_open_option_orders_update
-- INSTEAD OF UPDATE ON trading.open_option_orders_view
-- FOR EACH ROW EXECUTE FUNCTION trading.open_option_orders_update_trigger();
--
-- CREATE TRIGGER trg_open_option_orders_delete
-- INSTEAD OF DELETE ON trading.open_option_orders_view
-- FOR EACH ROW EXECUTE FUNCTION trading.open_option_orders_delete_trigger();
--
-- -- ============================
-- -- OPTION TRANSACTIONS VIEW
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.option_transactions_view AS
-- SELECT
--     t.*,
--     ot.expiry,
--     ot.strike,
--     ot.multiplier,
--     ot.option_type
-- FROM trading.transactions t
-- JOIN trading.option_transactions ot ON t.id = ot.id
-- WHERE t.asset_type = 'option';
--
-- -- Trigger functions for INSERT, UPDATE, DELETE
--
-- CREATE OR REPLACE FUNCTION trading.option_transactions_insert_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     INSERT INTO trading.transactions(strategy, time, asset_type, stock, price, fees, quantity)
--     VALUES (NEW.strategy, NEW.time, 'option', NEW.stock, NEW.price, NEW.fees, NEW.quantity)
--     RETURNING id INTO NEW.id;
--
--     INSERT INTO trading.option_transactions(id, expiry, strike, multiplier, option_type)
--     VALUES (NEW.id, NEW.expiry, NEW.strike, NEW.multiplier, NEW.option_type);
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.option_transactions_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE trading.transactions
--     SET strategy = NEW.strategy,
--         time = NEW.time,
--         stock = NEW.stock,
--         price = NEW.price,
--         fees = NEW.fees,
--         quantity = NEW.quantity
--     WHERE id = OLD.id;
--
--     UPDATE trading.option_transactions
--     SET expiry = NEW.expiry,
--         strike = NEW.strike,
--         multiplier = NEW.multiplier,
--         option_type = NEW.option_type
--     WHERE id = OLD.id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.option_transactions_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.transactions WHERE id = OLD.id;
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- -- Attach triggers to the view
-- CREATE TRIGGER trg_option_transactions_insert
-- INSTEAD OF INSERT ON trading.option_transactions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.option_transactions_insert_trigger();
--
-- CREATE TRIGGER trg_option_transactions_update
-- INSTEAD OF UPDATE ON trading.option_transactions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.option_transactions_update_trigger();
--
-- CREATE TRIGGER trg_option_transactions_delete
-- INSTEAD OF DELETE ON trading.option_transactions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.option_transactions_delete_trigger();
--
-- ============================
-- Staged Commissions table - to handle race conditions of commissions coming in
-- ============================

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
    WHERE trading.stock_transactions.execution_id = NEW.execution_id
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

-- -- ============================
-- -- CURRENT STOCK POSITIONS
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.current_stock_positions_view AS
-- SELECT strategy, asset_type, stock, avg_price, quantity FROM trading.current_positions
-- WHERE asset_type = 'stock';
--
-- CREATE OR REPLACE FUNCTION trading.current_stock_positions_insert_trigger()
-- RETURNS TRIGGER AS $$
-- DECLARE
--     existing_id INTEGER;
-- BEGIN
--     INSERT INTO trading.current_positions(strategy, asset_type, stock, avg_price, quantity)
--     VALUES (NEW.strategy, 'stock', NEW.stock, NEW.avg_price, NEW.quantity)
--     ON CONFLICT (asset_type, stock, strategy)
--     DO UPDATE SET
--         avg_price = EXCLUDED.avg_price,
--         quantity = EXCLUDED.quantity
--     RETURNING id INTO existing_id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.current_stock_positions_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     IF NEW.asset_type IS DISTINCT FROM 'stock' THEN
--         RAISE EXCEPTION 'Modifying asset_type is not allowed from this view';
--     END IF;
--
--     UPDATE trading.current_positions
--     SET
--         avg_price = NEW.avg_price,
--         quantity = NEW.quantity
--     WHERE asset_type = 'stock'
--       AND stock = OLD.stock
--       AND strategy = OLD.strategy;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.current_stock_positions_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.current_positions
--     WHERE asset_type = 'stock'
--       AND stock = OLD.stock
--       AND strategy = OLD.strategy;
--
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE TRIGGER trg_insert_current_stock_positions
-- INSTEAD OF INSERT ON trading.current_stock_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.current_stock_positions_insert_trigger();
--
-- CREATE TRIGGER trg_update_current_stock_positions
-- INSTEAD OF UPDATE ON trading.current_stock_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.current_stock_positions_update_trigger();
--
-- CREATE TRIGGER trg_delete_current_stock_positions
-- INSTEAD OF DELETE ON trading.current_stock_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.current_stock_positions_delete_trigger();
--
-- -- ============================
-- -- TARGET STOCK POSITIONS
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.target_stock_positions_view AS
-- SELECT strategy, asset_type, stock, avg_price, quantity FROM trading.target_positions
-- WHERE asset_type = 'stock';
--
-- CREATE OR REPLACE FUNCTION trading.target_stock_positions_insert_trigger()
-- RETURNS TRIGGER AS $$
-- DECLARE
--     existing_id INTEGER;
-- BEGIN
--     INSERT INTO trading.target_positions(strategy, asset_type, stock, avg_price, quantity)
--     VALUES (NEW.strategy, 'stock', NEW.stock, NEW.avg_price, NEW.quantity)
--     ON CONFLICT (asset_type, stock, strategy)
--     DO UPDATE SET
--         avg_price = EXCLUDED.avg_price,
--         quantity = EXCLUDED.quantity
--     RETURNING id INTO existing_id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.target_stock_positions_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     IF NEW.asset_type IS DISTINCT FROM 'stock' THEN
--         RAISE EXCEPTION 'Modifying asset_type is not allowed from this view';
--     END IF;
--
--     UPDATE trading.target_positions
--     SET
--         avg_price = NEW.avg_price,
--         quantity = NEW.quantity
--     WHERE asset_type = 'stock'
--       AND stock = OLD.stock
--       AND strategy = OLD.strategy;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.target_stock_positions_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.current_positions
--     WHERE asset_type = 'stock'
--       AND stock = OLD.stock
--       AND strategy = OLD.strategy;
--
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE TRIGGER trg_insert_target_stock_positions
-- INSTEAD OF INSERT ON trading.target_stock_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.target_stock_positions_insert_trigger();
--
-- CREATE TRIGGER trg_update_target_stock_positions
-- INSTEAD OF UPDATE ON trading.target_stock_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.target_stock_positions_update_trigger();
--
-- CREATE TRIGGER trg_delete_target_stock_positions
-- INSTEAD OF DELETE ON trading.target_stock_positions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.target_stock_positions_delete_trigger();
--
-- -- ============================
-- -- OPEN STOCK ORDERS
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.open_stock_orders_view AS
-- SELECT strategy, order_perm_id, order_id, time, asset_type, stock, quantity, filled, executions FROM trading.open_orders
-- WHERE asset_type = 'stock';
--
-- CREATE OR REPLACE FUNCTION trading.open_stock_orders_insert_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     INSERT INTO trading.open_orders (
--         strategy, order_perm_id, order_id, time,
--         asset_type, stock, quantity, filled, executions
--     )
--     VALUES (
--         NEW.strategy, NEW.order_perm_id, NEW.order_id, NEW.time,
--         'stock', NEW.stock, NEW.quantity, NEW.filled, NEW.executions
--     );
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.open_stock_orders_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE trading.open_orders
--     SET
--         order_perm_id = NEW.order_perm_id,
--         order_id = NEW.order_id,
--         time = NEW.time,
--         stock = NEW.stock,
--         quantity = NEW.quantity,
--         filled = NEW.filled,
--         executions = NEW.executions
--     WHERE strategy = OLD.strategy
--       AND asset_type = 'stock'
--       AND stock = OLD.stock
--       AND order_perm_id = OLD.order_perm_id
--       AND order_id = OLD.order_id
--       AND time = OLD.time;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.open_stock_orders_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.open_orders
--     WHERE strategy = OLD.strategy
--       AND asset_type = 'stock'
--       AND stock = OLD.stock
--       AND order_perm_id = OLD.order_perm_id
--       AND order_id = OLD.order_id
--       AND time = OLD.time;
--
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE TRIGGER trg_insert_open_stock_orders
-- INSTEAD OF INSERT ON trading.open_stock_orders_view
-- FOR EACH ROW EXECUTE FUNCTION trading.open_stock_orders_insert_trigger();
--
-- CREATE TRIGGER trg_update_open_stock_orders
-- INSTEAD OF UPDATE ON trading.open_stock_orders_view
-- FOR EACH ROW EXECUTE FUNCTION trading.open_stock_orders_update_trigger();
--
-- CREATE TRIGGER trg_delete_open_stock_orders
-- INSTEAD OF DELETE ON trading.open_stock_orders_view
-- FOR EACH ROW EXECUTE FUNCTION trading.open_stock_orders_delete_trigger();
--
-- -- ============================
-- -- STOCK TRANSACTIONS
-- -- ============================
--
-- CREATE OR REPLACE VIEW trading.stock_transactions_view AS
-- SELECT strategy, order_perm_id, time, asset_type, stock, price, fees, quantity FROM trading.transactions
-- WHERE asset_type = 'stock';
--
-- CREATE OR REPLACE FUNCTION trading.stock_transactions_insert_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     INSERT INTO trading.transactions (
--         strategy, order_perm_id, time,
--         asset_type, stock, price, fees, quantity
--     )
--     VALUES (
--         NEW.strategy, NEW.order_perm_id, NEW.time,
--         'stock', NEW.stock, NEW.price, NEW.fees, NEW.quantity
--     )
--     ON CONFLICT (asset_type, stock, strategy, time)
--     DO UPDATE SET
--         price = EXCLUDED.price,
--         fees = EXCLUDED.fees,
--         quantity = EXCLUDED.quantity,
--         order_perm_id = EXCLUDED.order_perm_id;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.stock_transactions_update_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE trading.transactions
--     SET
--         price = NEW.price,
--         fees = NEW.fees,
--         quantity = NEW.quantity,
--         order_perm_id = NEW.order_perm_id
--     WHERE strategy = OLD.strategy
--       AND asset_type = 'stock'
--       AND stock = OLD.stock
--       AND time = OLD.time;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE FUNCTION trading.stock_transactions_delete_trigger()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     DELETE FROM trading.transactions
--     WHERE strategy = OLD.strategy
--       AND asset_type = 'stock'
--       AND stock = OLD.stock
--       AND time = OLD.time;
--
--     RETURN OLD;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE TRIGGER trg_insert_stock_transactions
-- INSTEAD OF INSERT ON trading.stock_transactions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.stock_transactions_insert_trigger();
--
-- CREATE TRIGGER trg_update_stock_transactions
-- INSTEAD OF UPDATE ON trading.stock_transactions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.stock_transactions_update_trigger();
--
-- CREATE TRIGGER trg_delete_stock_transactions
-- INSTEAD OF DELETE ON trading.stock_transactions_view
-- FOR EACH ROW EXECUTE FUNCTION trading.stock_transactions_delete_trigger();

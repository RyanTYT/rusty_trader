ALTER TABLE trading.current_stock_positions
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.target_stock_positions
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.current_option_positions
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.target_option_positions
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.open_stock_orders
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.open_option_orders
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE market_data.historical_data
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE market_data.historical_options_data
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.stock_transactions
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

ALTER TABLE trading.option_transactions
ADD COLUMN currency VARCHAR(10) NOT NULL DEFAULT 'USD';

-- current_stock_positions
-- existing PK is likely (stock, primary_exchange, strategy)
ALTER TABLE trading.current_stock_positions
    DROP CONSTRAINT current_stock_positions_pkey;
ALTER TABLE trading.current_stock_positions
    ADD CONSTRAINT current_stock_positions_pkey
    PRIMARY KEY (stock, primary_exchange, currency, strategy);

-- target_stock_positions
ALTER TABLE trading.target_stock_positions
    DROP CONSTRAINT target_stock_positions_pkey;
ALTER TABLE trading.target_stock_positions
    ADD CONSTRAINT target_stock_positions_pkey
    PRIMARY KEY (strategy, primary_exchange, currency, stock);

-- current_option_positions
-- existing PK likely (stock, primary_exchange, strategy, expiry, strike, multiplier, option_type)
ALTER TABLE trading.current_option_positions
    DROP CONSTRAINT current_option_positions_pkey;
ALTER TABLE trading.current_option_positions
    ADD CONSTRAINT current_option_positions_pkey
    PRIMARY KEY (stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type);

-- target_option_positions
ALTER TABLE trading.target_option_positions
    DROP CONSTRAINT target_option_positions_pkey;
ALTER TABLE trading.target_option_positions
    ADD CONSTRAINT target_option_positions_pkey
    PRIMARY KEY (strategy, stock, primary_exchange, currency, expiry, strike, multiplier, option_type);

-- open_stock_orders
-- existing PK is (order_perm_id)
ALTER TABLE trading.open_stock_orders
    DROP CONSTRAINT open_stock_orders_pkey;
ALTER TABLE trading.open_stock_orders
    ADD CONSTRAINT open_stock_orders_pkey
    PRIMARY KEY (order_perm_id);
-- NOTE: currency is Optional<String> here, so it likely shouldn't be added to the PK

-- open_option_orders
ALTER TABLE trading.open_option_orders
    DROP CONSTRAINT open_option_orders_pkey;
ALTER TABLE trading.open_option_orders
    ADD CONSTRAINT open_option_orders_pkey
    PRIMARY KEY (order_perm_id);
-- NOTE: same as above, currency is Optional<String>, unsuitable as PK

-- historical_data
-- existing PK likely (stock, primary_exchange, time)
ALTER TABLE market_data.historical_data
    DROP CONSTRAINT historical_data_pkey;
ALTER TABLE market_data.historical_data
    ADD CONSTRAINT historical_data_pkey
    PRIMARY KEY (stock, primary_exchange, currency, time);

-- historical_options_data
-- existing PK likely (stock, primary_exchange, expiry, strike, multiplier, option_type, time)
ALTER TABLE market_data.historical_options_data
    DROP CONSTRAINT historical_options_data_pkey;
ALTER TABLE market_data.historical_options_data
    ADD CONSTRAINT historical_options_data_pkey
    PRIMARY KEY (stock, primary_exchange, currency, expiry, strike, multiplier, option_type, time);

-- stock_transactions
-- existing PK is (execution_id)
ALTER TABLE trading.stock_transactions
    DROP CONSTRAINT stock_transactions_pkey;
ALTER TABLE trading.stock_transactions
    ADD CONSTRAINT stock_transactions_pkey
    PRIMARY KEY (execution_id);
-- NOTE: currency is Optional<String> here, unsuitable as PK

-- option_transactions
ALTER TABLE trading.option_transactions
    DROP CONSTRAINT option_transactions_pkey;
ALTER TABLE trading.option_transactions
    ADD CONSTRAINT option_transactions_pkey
    PRIMARY KEY (execution_id);
-- NOTE: same as above

ALTER TABLE trading.strategy 
    DROP COLUMN capital,
    DROP COLUMN initial_capital;

ALTER TABLE trading.open_stock_orders
    ADD parent_order_id INT NOT NULL DEFAULT 0;

ALTER TABLE trading.open_option_orders
    ADD parent_order_id INT NOT NULL DEFAULT 0;

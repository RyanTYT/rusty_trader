-- Add migration script here

CREATE TABLE trading.fractional_momentum_weekly_positions (
    stock VARCHAR(50) NOT NULL,
    primary_exchange VARCHAR(50) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (stock, primary_exchange)
);

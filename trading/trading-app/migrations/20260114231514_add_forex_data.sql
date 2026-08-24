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

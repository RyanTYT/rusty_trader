-- Add migration script here

CREATE INDEX idx_historical_data_stock_exchange_time_desc
ON market_data.historical_data (stock, primary_exchange, time DESC);

CREATE INDEX idx_historical_data_stock_exchange_time
ON market_data.historical_data (stock, primary_exchange, time);

-- to update on UPDATE as well
CREATE OR REPLACE FUNCTION trading.set_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Stocks
ALTER TABLE trading.current_stock_positions
ADD COLUMN last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE TRIGGER trg_current_stock_positions_last_updated
BEFORE UPDATE ON trading.current_stock_positions
FOR EACH ROW
EXECUTE FUNCTION trading.set_last_updated();

-- Options
ALTER TABLE trading.current_option_positions
ADD COLUMN last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE TRIGGER trg_current_option_positions_last_updated
BEFORE UPDATE ON trading.current_option_positions
FOR EACH ROW
EXECUTE FUNCTION trading.set_last_updated();

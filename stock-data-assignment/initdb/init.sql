-- Create the stock_prices table
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    current_price DECIMAL(10,2),
    volume BIGINT,
    trading_date DATE,
    previous_close DECIMAL(10,2),
    price_change DECIMAL(10,2),
    change_percent DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trading_date)
);

-- Create an index for better query performance
CREATE INDEX IF NOT EXISTS idx_stock_symbol_date ON stock_prices(symbol, trading_date);
CREATE INDEX IF NOT EXISTS idx_stock_created_at ON stock_prices(created_at);

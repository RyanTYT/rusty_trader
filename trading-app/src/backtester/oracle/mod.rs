//! Backtest price/data surface — the point-in-time price oracle
//! ([`BacktestPriceSupplier`]) + the market data loader
//! ([`load_market_data`], IBKR/Alpaca → DB).

pub mod data_loader;
pub mod price_supplier;

pub use price_supplier::BacktestPriceSupplier;

//! Ports through which market data and contract resolution are injected.
//!
//! The rest of the crate depends on these traits rather than on `ibapi::Client`
//! or [`crate::market_data::consolidator::Consolidator`] directly. That is what
//! lets tests substitute fakes and the backtester replay data:
//!
//! - [`current_price::PriceSupplier`] — current price of a contract.
//! - [`contracts::ContractBroker`] — resolve and validate a raw contract.
//! - [`strategy_value::GetStrategyValue`] — SGD value of a strategy's positions.
//!
//! `available_funds.rs` is **disabled** (its `pub mod` line is commented out):
//! the `AccountTracker`/available-funds listener is not part of the compiled
//! crate today, so do not rely on it.

// pub mod available_funds;
pub mod current_price;
pub mod strategy_value;
pub mod contracts;

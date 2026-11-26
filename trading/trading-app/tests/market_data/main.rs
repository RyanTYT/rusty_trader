//! Tests for `src/market_data` (trait layer + consolidator + producer/consumer).
//!
//! Scope: pure, deterministic logic only — no IB client, no DB, no network.
//!
//! Not covered (see `traits/current_price.rs` for the reason):
//!   - `Consolidator::_extract_price` (src/market_data/traits/current_price.rs:418)
//!
//! Producer/consumer modules each open with the blockers that stop coverage
//! (spawned threads behind a live `ibapi::Client`, `PgPool`, or private
//! functions), with `path:line` references.

mod consolidator;
mod consumer;
mod handler;
mod producer;
mod traits;

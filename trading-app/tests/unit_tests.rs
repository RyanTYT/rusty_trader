//! Unit tests for trading-app.
//!
//! These tests cover pure-logic functions that require NO external dependencies
//! (no Postgres, no IBKR connection). Run with:
//!
//!     cargo test --test unit_tests
//!
//! The directory structure under `unit_tests/` mirrors `src/` for easy navigation:
//! a test for `src/strategy/helpers/rolling_fn.rs` lives at
//! `unit_tests/strategy/helpers/test_rolling_fn.rs`.

#[path = "unit_tests/strategy/mod.rs"]
mod strategy;
#[path = "unit_tests/helpers/mod.rs"]
mod helpers;
#[path = "unit_tests/database/mod.rs"]
mod database;
#[path = "unit_tests/market_data/mod.rs"]
mod market_data;
#[path = "unit_tests/execution/mod.rs"]
mod execution;
#[path = "unit_tests/schedule/mod.rs"]
mod schedule;
#[path = "unit_tests/logger/mod.rs"]
mod logger;

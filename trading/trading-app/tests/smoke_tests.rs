//! Smoke tests for trading-app.
//!
//! These tests require a live IB Gateway instance (on `127.0.0.1:4002`) AND a
//! live Postgres database. They are marked `#[ignore]` so they are skipped by
//! default. Run them explicitly with:
//!
//!     DATABASE_URL=... cargo test --test smoke_tests -- --ignored
//!
//! Each test file under `live/` exercises a real IBKR flow (validate_contract,
//! get_current_price, populate_historical_data, sync_executions, etc.).

#[path ="common/mod.rs"]
mod common;

#[path = "smoke_tests/live/mod.rs"]
mod live;

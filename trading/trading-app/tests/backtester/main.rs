//! Backtester integration tests.
//!
//! Mirrors the `src/backtester` layout: one module per source subfolder
//! (`setup`, `oracle`, `execution`, `output`, `methods`), plus `support` with
//! shared pure fixtures (contracts, fixed bars, a fake `PriceSupplier`, float
//! assertions).
//!
//! Nothing here touches IBKR or the network. `BacktestPriceSupplier` is built
//! with a lazy `PgPool` (no connection is ever opened); every other test is
//! pure logic. No database is used — see `blocked` notes in each module and the
//! final report for the client/DB-bound surface that is intentionally not
//! exercised.
//!
//! Run with:
//!   cargo test --test backtester --features backtest

#![cfg(feature = "backtest")]
#![allow(dead_code)]

mod execution;
mod methods;
mod oracle;
mod output;
mod setup;
mod support;

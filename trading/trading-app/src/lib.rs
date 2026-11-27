//! Automated IBKR trading system: live execution, historical replay and
//! backtesting.
//!
//! This is an application crate with a testable library core. The binary in
//! `src/main.rs` wires the live process; the reusable pieces live in the
//! modules below. It is **not** a published library API yet — see
//! `docs/future-api.md` for the parked plan to
//! expose a narrow facade.
//!
//! # Architecture
//!
//! ```text
//!   IBKR gateway
//!        │
//!        ▼
//!   market_data ──▶ bars, prices, ring buffers
//!        │
//!        ▼
//!   strategy ─────▶ BarUpdateOutcome (orders or a DB query)
//!        │
//!        ▼
//!   execution ────▶ IBKR orders, fills, position/transaction writes
//!        │
//!        ▼
//!   database ─────▶ TimescaleDB (positions, transactions, open orders, …)
//! ```
//!
//! - [`database`] — the persistence layer: models, the [`Exec`](crate::database::exec::Exec)
//!   executor wrapper, [`DbTxn`](crate::database::db::DbTxn) /
//!   [`DbDirect`](crate::database::db::DbDirect) sessions, the CRUD macros and
//!   the [`DbTables`](crate::database::tables::DbTables) accessors.
//! - [`execution`] — order placement and cancellation, the order-update stream,
//!   the execution/commission/open-order handlers, and startup/reconnect sync.
//! - [`strategy`] — strategy definitions, the intent/outcome types, and the
//!   `blended5` fund (sleeves, gates, ledger, monthly reallocation).
//! - [`strategy_wrappers`] — the gate/chain composition around a strategy.
//! - [`market_data`] — consolidation of IBKR bars, price providers, and the
//!   SPMC ring buffers that fan data out to strategies.
//! - `backtester` — historical replay in `InMemory` or `Db` mode (feature
//!   `backtest`).
//! - [`server`] — the axum read-only HTTP API (prices, FX, strategy values,
//!   health).
//! - [`init_app`] — live bootstrap: configuration, strategy construction,
//!   controllers and shutdown.
//!
//! [`helpers`], [`ibc`], [`logger`] and [`schedule`] are support modules.
//!
//! # Key concepts
//!
//! - **Strategy** — code that consumes bars and returns a
//!   [`BarUpdateOutcome`](crate::strategy::strategy::BarUpdateOutcome):
//!   `EmitOrders` (place/cancel now), `PendingDbQuery` (let the engine read
//!   target/current positions and decide), or `NoAction`.
//! - **Target vs current positions** — `trading.target_*_positions` is what a
//!   strategy wants; `trading.current_*_positions` is what it holds. The
//!   engine trades the difference.
//! - **Transactions** — one row per execution, with price, quantity, fees and
//!   signed side. Positions are updated additively from executions.
//! - **Open orders** — working orders mirrored from IBKR. Rows with
//!   `order_perm_id = -1` are *optimistic* placeholders written before IBKR
//!   acknowledges the order.
//! - **Parked / backed-up orders** — orders stored in the redb-backed
//!   [`OrderStore`](crate::execution::fx_backed_up_order::OrderStore) until
//!   cash/FX conditions allow placement.
//! - **Fund and sleeves** — the `blended5` fund runs four sleeves against one
//!   capital pool. Its [`PoolState`](crate::strategy_wrappers::gates::impls::fund_gate::PoolState)
//!   is **per run** (construction-time TLS + captured `Arc`), not process
//!   global; the ledger lives in the run.
//!
//! # Persistence and transactions
//!
//! All model access goes through [`database`]. A [`DbTxn`](crate::database::db::DbTxn)
//! owns one `sqlx::Transaction` and auto-rolls back when dropped, so a
//! multi-write handler must `commit()` explicitly. [`DbDirect`](crate::database::db::DbDirect)
//! is a pooled connection without `BEGIN`; use it for single statements and
//! sequences that do not need atomicity. Model handles borrow the session and
//! are exclusive: run one operation at a time, then drop the handle.
//!
//! The execution handler deliberately writes one transaction per execution
//! (open-order update/delete + transaction + position/cash updates) and only
//! forwards the strategy hook and places backed-up orders **after** commit.
//!
//! # Feature flags
//!
//! - `backtest` — compiles `backtester` and excludes [`init_app`]; replay
//!   code is `#[cfg(feature = "backtest")]`.
//! - `test-utils` — exposes `test_internals` so integration tests can reach
//!   `pub(crate)` helpers without widening production visibility.
//!
//! # Testing
//!
//! `tests/` mirrors `src/` (`tests/database`, `tests/execution`,
//! `tests/market_data`, …). Database tests are `#[ignore]` by default and
//! require `TEST_DATABASE_URL`; they truncate the target database on setup and
//! teardown. Complex execution flows are table-driven JSON scenario cases
//! stored next to their runners. See `docs/test_plan.md` and
//! `docs/scenarios.md`.
//!
//! # Documentation conventions
//!
//! Documentation here is written for maintainers of the current code, not for
//! an external API consumer. Every module and every item — private included —
//! should state its contract: units and encodings, side effects, error and
//! panic conditions, transaction/atomicity behaviour, and concurrency or
//! lifetime constraints. Prefer *why* and *invariants* over restating the
//! signature. Use `# Errors`, `# Panics`, `# Safety` and `# Invariants`
//! sections where they apply; private helpers may use plain prose.
//!
//! Related documents:
//! `docs/architecture.md`,
//! `docs/glossary.md`,
//! `docs/future-api.md`,
//! `docs/known_bugs.md`.

/// Historical replay and backtesting (feature `backtest`).
#[cfg(feature = "backtest")]
pub mod backtester;
/// TimescaleDB models, executors, CRUD and table accessors.
pub mod database;
/// Order placement, fills, position updates, and IBKR synchronisation.
pub mod execution;
/// Small shared helpers (contracts, hashing, timeouts, …).
pub mod helpers;
/// IB Gateway process lifecycle.
pub mod ibc;
/// Live application bootstrap: config, strategies, controllers, shutdown.
#[cfg(not(feature = "backtest"))]
pub mod init_app;
/// Logging setup and market-hours helpers.
pub mod logger;
/// Bar consolidation, price providers, and SPMC ring distribution.
pub mod market_data;
/// Trading-session and scheduling helpers.
pub mod schedule;
/// Read-only axum HTTP API (prices, FX, strategy values, health).
pub mod server;
/// Strategy definitions, intents/outcomes, and the `blended5` fund.
pub mod strategy;
/// Gate/chain composition around strategies.
pub mod strategy_wrappers;

/// Test-only re-exports of internal items that are otherwise `pub(crate)` or private.
/// This module is gated on `test-utils` feature (or `test` cfg for unit tests).
/// Production builds (without `test-utils`) are untouched.
/// External test files (under `tests/`) import via `trading_app::test_internals::...`
/// and the test binaries are built with `--features test-utils`.
#[cfg(all(any(test, feature = "test-utils"), not(feature = "backtest")))]
pub mod test_internals {
    // ─── Types (already pub in source — safe to pub use) ───────────────────
    pub use crate::helpers::contract::{HashContract, LocalContractTypes};
    pub use crate::helpers::sync_timeout::TimeoutError;
    pub use crate::ibc::IBGateway;
    pub use crate::market_data::consumer::strategy_consumer::{IbkrBarConsumer, IbkrBarType};
    pub use crate::market_data::handler::DataSubscription;
    pub use crate::market_data::memoise::{AnyMemoized, Memoized};

    // ─── Functions (pub(crate) in source — wrapped as pub here) ────────────
    // The source functions stay pub(crate); these pub wrappers delegate to them,
    // so prod visibility is unchanged but tests can call them.

    // ibc
    pub use crate::ibc::{with_gateway, with_gateway_retry};

    // helpers
    pub fn get_local_symbol(contract: &ibapi::contracts::Contract) -> String {
        crate::helpers::contract::get_local_symbol(contract)
    }
    pub fn build_contract_from_stock(
        stock: &String,
        primary_exchange: &String,
        currency: &String,
    ) -> ibapi::contracts::Contract {
        crate::helpers::contract::build_contract_from_stock(stock, primary_exchange, currency)
    }
    pub fn get_contract_from(pos_diff: &LocalContractTypes) -> ibapi::contracts::Contract {
        crate::helpers::contract::get_contract_from(pos_diff)
    }
    pub fn timeout<F, O, E>(duration: std::time::Duration, func: F) -> Result<O, TimeoutError<E>>
    where
        F: FnOnce() -> Result<O, E> + Send + 'static,
        O: Send + 'static,
        E: Send + 'static,
    {
        crate::helpers::sync_timeout::timeout(duration, func)
    }

    // database
    pub fn map_to_placeholder(key: usize, column_name: &str) -> String {
        crate::database::crud::map_to_placeholder(key, column_name)
    }

    // market_data (consolidator)
    pub fn last_bar_available_time_forex(
        now: chrono::DateTime<chrono_tz::Tz>,
    ) -> chrono::DateTime<chrono_tz::Tz> {
        crate::market_data::consolidator::last_bar_available_time_forex(now)
    }
    pub fn last_bar_available(
        now: chrono::DateTime<chrono_tz::Tz>,
        asset_type: &crate::database::models::AssetType,
    ) -> Option<chrono::DateTime<chrono_tz::Tz>> {
        crate::market_data::consolidator::last_bar_available(now, asset_type)
    }
    pub fn is_fx_trading_datetime(dt: &chrono::DateTime<chrono_tz::Tz>) -> bool {
        crate::market_data::consolidator::is_fx_trading_datetime(dt)
    }
    pub fn fx_trading_day_start(
        date: &chrono::NaiveDate,
        tz: &chrono_tz::Tz,
    ) -> chrono::DateTime<chrono_tz::Tz> {
        crate::market_data::consolidator::fx_trading_day_start(date, tz)
    }

    // market_data (consumer helper)
    pub use crate::market_data::consumer::helper::aggregate_bars; // already pub
    pub fn next_boundary(
        from: std::time::SystemTime,
        interval: std::time::Duration,
    ) -> std::time::SystemTime {
        crate::market_data::consumer::helper::next_boundary(from, interval)
    }

    // logger
    pub fn is_stock_open_hard(dt: &chrono::DateTime<chrono_tz::Tz>) -> bool {
        crate::logger::is_stock_open_hard(dt)
    }
    pub fn is_autorestart(dt: chrono::DateTime<chrono::Utc>) -> bool {
        crate::logger::is_autorestart(dt)
    }
    pub fn is_apac_reset_now(now_utc: &chrono::DateTime<chrono::Utc>) -> bool {
        crate::logger::is_apac_reset_now(now_utc)
    }
    pub fn is_any_open(dt: &chrono::DateTime<chrono_tz::Tz>) -> bool {
        crate::logger::is_any_open(dt)
    }

    // ─── CRUD constructors (pub(crate) in source — wrapped as pub here) ────
    // Re-exports the CRUD types + pub wrapper constructors.
    pub use crate::database::models_crud::current_positions::current_option_positions::CurrentOptionPositionsCRUD;
    pub use crate::database::models_crud::current_positions::current_stock_positions::CurrentStockPositionsCRUD;
    pub use crate::database::models_crud::historical_data::historical_forex_data::HistoricalForexDataCRUD;
    pub use crate::database::models_crud::historical_data::historical_options_data::HistoricalOptionsDataCRUD;
    pub use crate::database::models_crud::historical_data::historical_stock_data::HistoricalStockDataCRUD;
    pub use crate::database::models_crud::logs::LogsCRUD;
    pub use crate::database::models_crud::notification::NotificationCRUD;
    pub use crate::database::models_crud::open_orders::open_option_orders::OpenOptionOrdersCRUD;
    pub use crate::database::models_crud::open_orders::open_stock_orders::OpenStockOrdersCRUD;
    pub use crate::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsCRUD;
    pub use crate::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsCRUD;
    pub use crate::database::models_crud::transactions::option_transactions::OptionTransactionsCRUD;
    pub use crate::database::models_crud::transactions::stock_transactions::StockTransactionsCRUD;

    pub fn current_stock_positions_crud(pool: &sqlx::PgPool) -> CurrentStockPositionsCRUD<'_> {
        CurrentStockPositionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn current_option_positions_crud(pool: &sqlx::PgPool) -> CurrentOptionPositionsCRUD<'_> {
        CurrentOptionPositionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn open_stock_orders_crud(pool: &sqlx::PgPool) -> OpenStockOrdersCRUD<'_> {
        OpenStockOrdersCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn open_option_orders_crud(pool: &sqlx::PgPool) -> OpenOptionOrdersCRUD<'_> {
        OpenOptionOrdersCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn target_stock_positions_crud(pool: &sqlx::PgPool) -> TargetStockPositionsCRUD<'_> {
        TargetStockPositionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn target_option_positions_crud(pool: &sqlx::PgPool) -> TargetOptionPositionsCRUD<'_> {
        TargetOptionPositionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn stock_transactions_crud(pool: &sqlx::PgPool) -> StockTransactionsCRUD<'_> {
        StockTransactionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn option_transactions_crud(pool: &sqlx::PgPool) -> OptionTransactionsCRUD<'_> {
        OptionTransactionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn historical_stock_data_crud(pool: &sqlx::PgPool) -> HistoricalStockDataCRUD<'_> {
        HistoricalStockDataCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn historical_options_data_crud(pool: &sqlx::PgPool) -> HistoricalOptionsDataCRUD<'_> {
        HistoricalOptionsDataCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn historical_forex_data_crud(pool: &sqlx::PgPool) -> HistoricalForexDataCRUD<'_> {
        HistoricalForexDataCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn logs_crud(pool: &sqlx::PgPool) -> LogsCRUD<'_> {
        LogsCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub fn notification_crud(pool: &sqlx::PgPool) -> NotificationCRUD<'_> {
        NotificationCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub use crate::database::models_crud::cancelled_orders::CancelledOrdersCRUD;
    pub fn cancelled_orders_crud(pool: &sqlx::PgPool) -> CancelledOrdersCRUD<'_> {
        CancelledOrdersCRUD::new(crate::database::db::Exec::Pool(pool))
    }
    pub use crate::database::models_crud::staged_commissions::StagedCommissionsCRUD;
    pub fn staged_commissions_crud(pool: &sqlx::PgPool) -> StagedCommissionsCRUD<'_> {
        StagedCommissionsCRUD::new(crate::database::db::Exec::Pool(pool))
    }

    // ─── StrategyParameters constructor (fields are pub(crate) — wrapped here) ──
    pub use crate::init_app::StrategyParameters;
    pub fn strategy_parameters(
        strategy: crate::strategy::strategy::StrategyEnum,
        subscribed_contracts: Vec<crate::market_data::handler::DataSubscription>,
    ) -> StrategyParameters {
        StrategyParameters {
            strategy,
            subscribed_contracts,
        }
    }
}

#[macro_export]
macro_rules! arc_drop_async {
    ($app_state:ident) => {
        let mut app_state =
            std::sync::Arc::into_inner($app_state).expect("Expected only 1 strong reference");
        app_state.async_drop().await;
    };
}

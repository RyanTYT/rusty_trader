use async_trait::async_trait;
use sqlx::{Postgres, postgres::PgArguments, query::QueryAs};
pub mod database;
pub mod execution;
pub mod helpers;
pub mod ibc;
pub mod logger;
pub mod market_data;
pub mod strategy;
pub mod init_app;
pub mod schedule;
pub mod server;

/// Test-only re-exports of internal items that are otherwise `pub(crate)` or private.
/// This module is gated on `test-utils` feature (or `test` cfg for unit tests).
/// Production builds (without `test-utils`) are untouched.
/// External test files (under `tests/`) import via `trading_app::test_internals::...`
/// and the test binaries are built with `--features test-utils`.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_internals {
    // ─── Types (already pub in source — safe to pub use) ───────────────────
    pub use crate::ibc::IBGateway;
    pub use crate::helpers::contract::{HashContract, LocalContractTypes};
    pub use crate::helpers::sync_timeout::TimeoutError;
    pub use crate::market_data::consumer::strategy_consumer::{IbkrBarConsumer, IbkrBarType};
    pub use crate::market_data::handler::DataSubscription;
    pub use crate::market_data::memoise::{AnyMemoized, Memoized};

    // ─── Functions (pub(crate) in source — wrapped as pub here) ────────────
    // The source functions stay pub(crate); these pub wrappers delegate to them,
    // so prod visibility is unchanged but tests can call them.

    // ibc
    pub async fn init_ibc_with_retry(
        ibc_logs_file: &'static str,
        retry_times: u32,
    ) -> Result<IBGateway, String> {
        crate::ibc::init_ibc_with_retry(ibc_logs_file, retry_times).await
    }

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
    pub fn timeout<F, O, E>(
        duration: std::time::Duration,
        func: F,
    ) -> Result<O, TimeoutError<E>>
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
    pub fn last_bar_available_time_forex(now: chrono::DateTime<chrono_tz::Tz>) -> chrono::DateTime<chrono_tz::Tz> {
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
    pub use crate::market_data::consumer::helper::aggregate_bars;  // already pub
    pub fn next_boundary(from: std::time::SystemTime, interval: std::time::Duration) -> std::time::SystemTime {
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
    pub use crate::database::models_crud::current_positions::current_stock_positions::CurrentStockPositionsCRUD;
    pub use crate::database::models_crud::current_positions::current_option_positions::CurrentOptionPositionsCRUD;
    pub use crate::database::models_crud::open_orders::open_stock_orders::OpenStockOrdersCRUD;
    pub use crate::database::models_crud::open_orders::open_option_orders::OpenOptionOrdersCRUD;
    pub use crate::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsCRUD;
    pub use crate::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsCRUD;
    pub use crate::database::models_crud::transactions::stock_transactions::StockTransactionsCRUD;
    pub use crate::database::models_crud::transactions::option_transactions::OptionTransactionsCRUD;
    pub use crate::database::models_crud::historical_data::historical_stock_data::HistoricalStockDataCRUD;
    pub use crate::database::models_crud::historical_data::historical_options_data::HistoricalOptionsDataCRUD;
    pub use crate::database::models_crud::historical_data::historical_forex_data::HistoricalForexDataCRUD;
    pub use crate::database::models_crud::historical_data::daily_historical_data::DailyHistoricalStockDataCRUD;
    pub use crate::database::models_crud::logs::LogsCRUD;
    pub use crate::database::models_crud::notification::NotificationCRUD;

    pub fn current_stock_positions_crud(pool: sqlx::PgPool) -> CurrentStockPositionsCRUD {
        CurrentStockPositionsCRUD::new(pool)
    }
    pub fn current_option_positions_crud(pool: sqlx::PgPool) -> CurrentOptionPositionsCRUD {
        CurrentOptionPositionsCRUD::new(pool)
    }
    pub fn open_stock_orders_crud(pool: sqlx::PgPool) -> OpenStockOrdersCRUD {
        OpenStockOrdersCRUD::new(pool)
    }
    pub fn open_option_orders_crud(pool: sqlx::PgPool) -> OpenOptionOrdersCRUD {
        OpenOptionOrdersCRUD::new(pool)
    }
    pub fn target_stock_positions_crud(pool: sqlx::PgPool) -> TargetStockPositionsCRUD {
        TargetStockPositionsCRUD::new(pool)
    }
    pub fn target_option_positions_crud(pool: sqlx::PgPool) -> TargetOptionPositionsCRUD {
        TargetOptionPositionsCRUD::new(pool)
    }
    pub fn stock_transactions_crud(pool: sqlx::PgPool) -> StockTransactionsCRUD {
        StockTransactionsCRUD::new(pool)
    }
    pub fn option_transactions_crud(pool: sqlx::PgPool) -> OptionTransactionsCRUD {
        OptionTransactionsCRUD::new(pool)
    }
    pub fn historical_stock_data_crud(pool: sqlx::PgPool) -> HistoricalStockDataCRUD {
        HistoricalStockDataCRUD::new(pool)
    }
    pub fn historical_options_data_crud(pool: sqlx::PgPool) -> HistoricalOptionsDataCRUD {
        HistoricalOptionsDataCRUD::new(pool)
    }
    pub fn historical_forex_data_crud(pool: sqlx::PgPool) -> HistoricalForexDataCRUD {
        HistoricalForexDataCRUD::new(pool)
    }
    pub fn daily_historical_stock_data_crud(pool: sqlx::PgPool) -> DailyHistoricalStockDataCRUD {
        DailyHistoricalStockDataCRUD::new(pool)
    }
    pub fn logs_crud(pool: sqlx::PgPool) -> LogsCRUD {
        LogsCRUD::new(pool)
    }
    pub fn notification_crud(pool: sqlx::PgPool) -> NotificationCRUD {
        NotificationCRUD::new(pool)
    }
    pub use crate::database::models_crud::cancelled_orders::CancelledOrdersCRUD;
    pub fn cancelled_orders_crud(pool: sqlx::PgPool) -> CancelledOrdersCRUD {
        CancelledOrdersCRUD::new(pool)
    }
    pub use crate::database::models_crud::staged_commissions::StagedCommissionsCRUD;
    pub fn staged_commissions_crud(pool: sqlx::PgPool) -> StagedCommissionsCRUD {
        StagedCommissionsCRUD::new(pool)
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

#[async_trait]
pub trait Insertable {
    fn table_name() -> &'static str;
    fn pri_column_names(&self) -> Vec<&'static str>;
    fn opt_column_names(&self) -> Vec<&'static str>;
    fn bind_pri<'q>(&'q self, sql: &'q str) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_pri_to_query<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_pri_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
    fn bind_opt<'q>(&'q self, sql: &'q str) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_opt_to_query<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_opt_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
}

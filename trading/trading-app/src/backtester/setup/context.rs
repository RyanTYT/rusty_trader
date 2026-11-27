//! Shared execution surface for all backtest methods. Two shapes:
//! - [`LightContext`] — clock + price supplier + consolidator. The pieces
//!   every method needs. Used by the InMemory path (single + sweep) —
//!   avoids constructing the broker/order_engine/order_store (which would
//!   open the `OrderStore` file + contend across parallel sweep backtests).
//! - [`BacktestContext`] — the full execution surface (LightContext + broker,
//!   order_engine, order_store, strategy). Used by the DB-backed
//!   `HistoricalReplay` method only.
//!
//! `BacktestContext::build` reuses `build_light_context` so the
//! clock/prices/consolidator wiring isn't duplicated.

use std::sync::Arc;

use sqlx::PgPool;

use crate::execution::fx_backed_up_order::OrderStore;
use crate::execution::order_engine::OrderEngine;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::handler::MarketDataHandler;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::{StrategyDetails, StrategyEnum, StrategyExecutor};

use crate::backtester::execution::broker::BacktestBroker;
use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;
use crate::backtester::setup::clock::BacktestClock;
use crate::backtester::setup::config::BacktestConfig;

/// The light execution surface — clock + price supplier + consolidator. The
/// pieces every backtest method needs; constructed without the broker /
/// order_engine / order_store (so no `OrderStore::open()` file contention).
/// Used by the InMemory path (single route + sweep).
pub struct LightContext {
    /// The replay clock; advanced to the current bar before each tick.
    pub clock: Arc<BacktestClock>,
    /// Point-in-time price oracle over the replayed bar history.
    pub prices: Arc<BacktestPriceSupplier>,
    /// Read facade wired to the oracle, with memoisation disabled.
    pub consolidator: Arc<Consolidator>,
}

/// Build the light execution surface (clock + prices + consolidator) from a
/// pool + config. Shared by the InMemory single route, the sweep
/// (`run_one_backtest`), + `BacktestContext::build` (so the wiring isn't
/// duplicated).
///
/// The consolidator is built with `new_for_backtest`, so no lookup is
/// memoised: every price/value read reaches the oracle (and therefore the
/// replayed history) instead of a cache that could hold a stale bar.
pub fn build_light_context(pool: &PgPool, config: &BacktestConfig) -> LightContext {
    let clock = Arc::new(BacktestClock::new());
    let prices = Arc::new(BacktestPriceSupplier::new(
        clock.clone(),
        pool.clone(),
        &config.subscribed_contracts,
    ));
    let market_data_handler = MarketDataHandler::new(pool.clone());
    let consolidator = Arc::new(Consolidator::new_for_backtest(
        pool.clone(),
        prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
        market_data_handler,
    ));
    LightContext {
        clock,
        prices,
        consolidator,
    }
}

/// The full execution surface — `LightContext` + broker + order_engine +
/// order_store + strategy. Used by the DB-backed `HistoricalReplay` method
/// (which needs the broker/order_engine/order_store for real reconciliation).
pub struct BacktestContext {
    /// Database pool used by the broker, engine and handlers.
    pub pool: PgPool,
    /// Handle of the owning runtime; used for `block_on` and spawned writes.
    pub handle: tokio::runtime::Handle,
    /// The run's configuration (bar intervals, fees, period, lag).
    pub config: BacktestConfig,
    /// The replay clock shared with the oracle.
    pub clock: Arc<BacktestClock>,
    /// Point-in-time price oracle.
    pub prices: Arc<BacktestPriceSupplier>,
    /// The simulated broker that materializes fills.
    pub broker: Arc<BacktestBroker>,
    /// Read facade over the oracle.
    pub consolidator: Arc<Consolidator>,
    /// Order engine used for placement/cancellation.
    pub order_engine: OrderEngine,
    /// Parked-order store (opened from `ORDERS_FILE_PATH`).
    pub order_store: Arc<OrderStore>,
    /// The strategy under test.
    pub strategy: StrategyEnum,
    /// The strategy's metadata, resolved once for the run.
    pub strategy_details: StrategyDetails,
}

impl BacktestContext {
    /// Construct the full execution surface. Reuses `build_light_context` for
    /// the clock/prices/consolidator, then adds the broker/order_engine/order_store.
    ///
    /// # Panics
    /// Panics when the parked-order store cannot be opened — either
    /// `ORDERS_FILE_PATH` is unset or the redb file cannot be created
    /// (permissions, contention, corrupt file).
    pub fn build(
        config: BacktestConfig,
        pool: PgPool,
        handle: tokio::runtime::Handle,
        strategy: StrategyEnum,
    ) -> Self {
        let light = build_light_context(&pool, &config);
        let broker = Arc::new(BacktestBroker::new(
            pool.clone(),
            handle.clone(),
            config.clone(),
            light.prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
        ));
        let order_engine = OrderEngine::new(pool.clone(), handle.clone());
        let order_store =
            Arc::new(OrderStore::open().expect("Expected to open OrderStore for backtest"));
        let strategy_details = strategy.get_strategy_details();
        Self {
            pool,
            handle,
            config,
            clock: light.clock,
            prices: light.prices,
            broker,
            consolidator: light.consolidator,
            order_engine,
            order_store,
            strategy,
            strategy_details,
        }
    }
}

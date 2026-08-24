//! Shared execution surface for all backtest methods — the broker, price
//! oracle, consolidator, order engine, order store, + strategy. Constructed
//! once by `BacktestContext::build` + passed to each `BacktestMethod::run`.
//! This is the seam that lets a second backtest method reuse the same
//! execution setup without duplicating the broker/prices/consolidator wiring.

use std::sync::Arc;

use sqlx::PgPool;

use crate::execution::fx_backed_up_order::OrderStore;
use crate::execution::order_engine::OrderEngine;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::handler::MarketDataHandler;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::StrategyEnum;

use crate::backtester::execution::broker::BacktestBroker;
use crate::backtester::clock::BacktestClock;
use crate::backtester::config::BacktestConfig;
use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;

pub struct BacktestContext {
    pub pool: PgPool,
    pub handle: tokio::runtime::Handle,
    pub config: BacktestConfig,
    pub clock: Arc<BacktestClock>,
    pub prices: Arc<BacktestPriceSupplier>,
    pub broker: Arc<BacktestBroker>,
    pub consolidator: Arc<Consolidator>,
    pub order_engine: OrderEngine,
    pub order_store: Arc<OrderStore>,
    pub strategy: StrategyEnum,
}

impl BacktestContext {
    /// Construct the full execution surface from a config + pool + handle +
    /// strategy. This wires the broker, price supplier, consolidator, order
    /// engine, + order store — shared by all backtest methods.
    pub fn build(
        config: BacktestConfig,
        pool: PgPool,
        handle: tokio::runtime::Handle,
        strategy: StrategyEnum,
    ) -> Self {
        let clock = Arc::new(BacktestClock::new());
        let prices = Arc::new(BacktestPriceSupplier::new(
            clock.clone(),
            pool.clone(),
            &config.subscribed_contracts,
        ));
        let broker = Arc::new(BacktestBroker::new(
            pool.clone(),
            handle.clone(),
            config.clone(),
            prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
        ));
        let market_data_handler = MarketDataHandler::new(pool.clone());
        let consolidator = Arc::new(Consolidator::new_for_backtest(
            pool.clone(),
            prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
            market_data_handler,
        ));
        let order_engine = OrderEngine::new(pool.clone(), handle.clone());
        let order_store = Arc::new(
            OrderStore::open().expect("Expected to open OrderStore for backtest"),
        );
        Self {
            pool,
            handle,
            config,
            clock,
            prices,
            broker,
            consolidator,
            order_engine,
            order_store,
            strategy,
        }
    }
}

//! Backtest replayer — the core loop. Uses REAL prod code:
//!   - `Consolidator::new_for_backtest` (backtest-constructible, no `Client`)
//!   - `strategy.on_bar_update(contract, bar, &Arc<Consolidator>)` (real signal logic)
//!   - `order_engine.handle_bar_update_outcome(&broker, &prices, outcome, &strategy, &order_store)` (real reconciliation)
//!
//! Per tick: advance clock → publish close to oracle → set broker bar →
//! real on_bar_update → real handle_bar_update_outcome → equity snapshot.
//! Runs on `spawn_blocking` so `handle.block_on` calls are legal.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use ibapi::contracts::Contract;
use sqlx::FromRow;

use crate::database::crud::CRUDTrait;
use crate::database::models::{AssetType, HistoricalStockDataFullKeys, Status, StrategyFullKeys};
use crate::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys, CurrentPositionsOps,
};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::database::models_crud::strategy::StrategyCRUD;
use crate::execution::order_engine::OrderEngine;
use crate::execution::fx_backed_up_order::OrderStore;
use crate::helpers::contract::{get_contract_from, get_local_symbol, LocalContractTypes};
use crate::market_data::consolidator::Consolidator;
use crate::market_data::handler::MarketDataHandler;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::backtester::broker::BacktestBroker;
use crate::backtester::clock::BacktestClock;
use crate::backtester::config::BacktestConfig;
use crate::backtester::equity::{EquityCurve, EquitySnapshot};
use crate::backtester::price_supplier::BacktestPriceSupplier;

pub struct Replayer {
    config: BacktestConfig,
    pool: sqlx::PgPool,
    handle: tokio::runtime::Handle,
    clock: Arc<BacktestClock>,
    prices: Arc<BacktestPriceSupplier>,
    broker: Arc<BacktestBroker>,
    consolidator: Arc<Consolidator>,
    order_engine: OrderEngine,
    order_store: Arc<OrderStore>,
    strategy: StrategyEnum,
}

impl Replayer {
    pub fn new(
        config: BacktestConfig,
        pool: sqlx::PgPool,
        handle: tokio::runtime::Handle,
        strategy: StrategyEnum,
    ) -> Self {
        let clock = Arc::new(BacktestClock::new());
        let prices = Arc::new(BacktestPriceSupplier::new(clock.clone(), pool.clone()));
        let broker = Arc::new(BacktestBroker::new(
            pool.clone(),
            handle.clone(),
            config.clone(),
        ));
        let market_data_handler = MarketDataHandler::new(pool.clone());
        let consolidator = Arc::new(Consolidator::new_for_backtest(
            pool.clone(),
            prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
            market_data_handler,
        ));
        let order_engine = OrderEngine::new(pool.clone(), handle.clone());
        let order_store = Arc::new(
            OrderStore::open()
                .expect("Expected to open OrderStore for backtest"),
        );

        Self {
            config,
            pool,
            handle,
            clock,
            prices,
            broker,
            consolidator,
            order_engine,
            order_store,
            strategy,
        }
    }

    pub fn broker(&self) -> &Arc<BacktestBroker> {
        &self.broker
    }

    pub fn prices(&self) -> &Arc<BacktestPriceSupplier> {
        &self.prices
    }

    /// Run the backtest over the configured period. Returns the equity curve.
    pub fn run(&self) -> Result<EquityCurve, String> {
        // 1. Ensure market data is loaded (v1: pre-populated, no-op).
        // 2. Ensure the strategy row exists (FK target for transactions/positions).
        let pool = self.pool.clone();
        let strat_name = self.strategy.get_name();
        self.handle.block_on(async move {
            let crud = StrategyCRUD::new(pool);
            if let Err(e) = crud
                .create_or_ignore(&StrategyFullKeys {
                    strategy: strat_name,
                    status: Status::Active,
                })
                .await
            {
                tracing::error!("backtest: create strategy row failed: {e:?}");
            }
        });

        // 3. Load the bar stream (v1: single stock contract).
        let bars = self.handle.block_on(self.load_bars())?;
        tracing::info!(
            "Backtest: {} bars over [{}, {}]",
            bars.len(),
            self.config.start,
            self.config.end
        );

        // 4. Replay.
        let contract = self
            .config
            .subscribed_contracts
            .first()
            .cloned()
            .expect("BacktestConfig.subscribed_contracts must be non-empty");
        let mut equity = EquityCurve::new();
        for bar in bars {
            let time = bar.get_time();
            self.clock.set(time);
            let close = bar.get_price();
            self.prices.publish_close(&contract, close);
            self.broker.set_current_bar(bar.clone());

            // --- REAL prod on_bar_update (the strategy signal logic) ---
            let outcome = self
                .strategy
                .on_bar_update(&contract, &bar, &self.consolidator)
                .unwrap_or_else(|e| {
                    tracing::error!("on_bar_update error: {e:?}");
                    crate::strategy::strategy::BarUpdateOutcome::NoAction
                });

            // --- REAL prod handle_bar_update_outcome (the reconciliation) ---
            // Under `--features backtest` this takes `&dyn OrderSubmitter` +
            // `&dyn PriceSupplier` — the SAME body as prod, just routed through
            // BacktestBroker + BacktestPriceSupplier instead of client/consolidator.
            self.order_engine.handle_bar_update_outcome(
                &*self.broker,
                &*self.prices,
                outcome,
                &self.strategy,
                &self.order_store,
            );

            // 5. Equity snapshot.
            let snap = self.handle.block_on(self.snapshot(time, &contract, close));
            equity.push(snap);
        }
        Ok(equity)
    }

    async fn load_bars(&self) -> Result<Vec<HistoricalDataFullKeys>, String> {
        let c = self
            .config
            .subscribed_contracts
            .first()
            .expect("subscribed_contracts non-empty");
        let stock = get_local_symbol(c);
        let pe = c.primary_exchange.to_string();
        let currency = c.currency.to_string();

        #[derive(FromRow)]
        struct BarRow {
            stock: String,
            primary_exchange: String,
            currency: String,
            time: DateTime<Utc>,
            open: f64,
            high: f64,
            low: f64,
            close: f64,
            volume: rust_decimal::Decimal,
        }

        let rows: Vec<BarRow> = sqlx::query_as(
            r#"SELECT stock, primary_exchange, currency, time, open, high, low, close, volume
               FROM market_data.historical_data
               WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                 AND time >= $4 AND time <= $5
               ORDER BY time ASC"#,
        )
        .bind(stock)
        .bind(pe)
        .bind(currency)
        .bind(self.config.start)
        .bind(self.config.end)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("load_bars: {e:?}"))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                HistoricalDataFullKeys::Stock(HistoricalStockDataFullKeys {
                    stock: r.stock,
                    primary_exchange: r.primary_exchange,
                    currency: r.currency,
                    time: r.time,
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                })
            })
            .collect())
    }

    async fn snapshot(&self, time: DateTime<Utc>, _contract: &Contract, fallback_close: f64) -> EquitySnapshot {
        let cash = self.broker.cash();
        let cp_crud = CurrentPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
        let positions = match cp_crud.get_pos_by_strat(self.strategy.get_name().as_str()).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("snapshot: get_pos_by_strat failed: {e:?}");
                Vec::new()
            }
        };
        let mut positions_value = 0.0_f64;
        for pos in positions {
            let qty = match &pos {
                CurrentPositionsFullKeys::Stock(v) => v.quantity,
                _ => continue,
            };
            if qty.abs() < 1e-9 {
                continue;
            }
            let pcontract = get_contract_from(&LocalContractTypes::CurrentPosFk(pos));
            let price = self
                .prices
                .get_current_price(pcontract, false, &[])
                .unwrap_or(fallback_close);
            positions_value += qty * price;
        }
        let equity = cash + positions_value;
        EquitySnapshot {
            time,
            cash,
            positions_value,
            equity,
        }
    }
}

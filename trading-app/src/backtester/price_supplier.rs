//! Point-in-time price oracle for the backtester.
//!
//! Implements the existing prod [`PriceSupplier`] trait so the seamed
//! `handle_bar_update_outcome` variant (and `Consolidator::new_for_backtest`'s
//! memoisers) can fetch "current" prices without touching IBKR/yfinance.
//!
//! `get_current_price` returns the most recent bar close at or before the
//! backtest clock — i.e. the close of the bar the replayer is currently
//! replaying. The replayer publishes each tick's close via [`publish_close`],
//! which seeds an in-memory cache (fast path). FX pairs not in the bar stream
//! fall back to a point-in-time DB lookup (slow path — TODO).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ibapi::contracts::Contract;

use crate::helpers::contract::HashContract;
use crate::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};

pub struct BacktestPriceSupplier {
    clock: Arc<crate::backtester::clock::BacktestClock>,
    /// HashContract -> latest published close. Populated by the replayer.
    cache: Mutex<HashMap<HashContract, f64>>,
    #[allow(dead_code)]
    pool: sqlx::PgPool,
}

impl BacktestPriceSupplier {
    pub fn new(
        clock: Arc<crate::backtester::clock::BacktestClock>,
        pool: sqlx::PgPool,
    ) -> Self {
        Self {
            clock,
            cache: Mutex::new(HashMap::new()),
            pool,
        }
    }

    /// Called by the replayer each tick to publish the current bar's close for
    /// a contract, so `get_current_price` returns it without a DB hit.
    pub fn publish_close(&self, contract: &Contract, close: f64) {
        let key = HashContract {
            contract: contract.clone(),
        };
        self.cache
            .lock()
            .expect("BacktestPriceSupplier cache poisoned")
            .insert(key, close);
    }
}

#[async_trait::async_trait]
impl PriceSupplier for BacktestPriceSupplier {
    fn get_current_price(
        &self,
        contract: Contract,
        _vwap: bool,
        _generic_ticks: &[&str],
    ) -> Result<f64, String> {
        let key = HashContract {
            contract: contract.clone(),
        };
        if let Some(&close) = self
            .cache
            .lock()
            .expect("BacktestPriceSupplier cache poisoned")
            .get(&key)
        {
            return Ok(close);
        }
        // Slow path: point-in-time DB lookup at the backtest clock.
        // TODO: per-asset-type as-of close from historical_data /
        // historical_forex_data (covers FX pairs not in the bar stream).
        let _ = self.clock.now();
        Err(format!(
            "BacktestPriceSupplier: no published close for {} (slow path not yet implemented)",
            contract.symbol
        ))
    }

    #[cfg(not(feature = "backtest"))]
    async fn populate_historical_data(
        &self,
        _contract: &Contract,
        _config: &HistoricalDataConfig,
    ) -> Result<(), String> {
        // Backtest pre-loads market_data via the data-loader phase; no-op here.
        Ok(())
    }
}

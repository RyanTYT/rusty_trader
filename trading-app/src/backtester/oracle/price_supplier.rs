//! Point-in-time price oracle for the backtester.
//!
//! Implements the existing prod [`PriceSupplier`] trait so the seamed
//! `handle_bar_update_outcome` variant (and `Consolidator::new_for_backtest`'s
//! memoisers) can fetch "current" prices without touching IBKR/yfinance.
//!
//! `get_current_price` returns the most recent bar close at or before the
//! backtest clock — i.e. the close of the bar the replayer is currently
//! replaying. The replayer publishes each tick's close via [`publish_close`],
//! which writes a **fixed-size lock-free cache** (a `Box<[AtomicU64]>` indexed
//! by a pre-assigned slot — one slot per contract, fixed at init since the
//! strategy tracks a known, fixed set of contracts). FX pairs not in the bar
//! stream fall back to a fixed FX-rate map (so strategies that ignore FX don't
//! die — they just use a constant rate). The slow-path point-in-time DB lookup
//! is a TODO for when accurate FX is needed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;

use crate::helpers::contract::HashContract;
use crate::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};

pub struct BacktestPriceSupplier {
    clock: Arc<crate::backtester::clock::BacktestClock>,
    /// Fixed-size lock-free cache: slot `i` holds the latest published close
    /// for the contract assigned to slot `i`, packed via `f64::to_bits`.
    /// `0` = not yet published. Indexed by `slot_map`; size is fixed at init
    /// (= the number of contracts the strategy tracks).
    cache: Box<[AtomicU64]>,
    /// `HashContract` → slot index. Immutable after `new` — concurrent reads
    /// are safe (no mutation, so `&HashMap` from multiple threads is sound).
    slot_map: HashMap<HashContract, usize>,
    /// Fixed FX rates for pairs not in the bar stream. Keyed by
    /// `(base_symbol, quote_currency)` — e.g. `("USD", "SGD")` -> 1.35 means
    /// 1 USD = 1.35 SGD. The reverse pair is resolved by inverting.
    fx_map: HashMap<(String, String), f64>,
    #[allow(dead_code)]
    pool: sqlx::PgPool,
}

impl BacktestPriceSupplier {
    /// Build the supplier with a FIXED set of contracts (one cache slot each).
    /// The replayer passes `config.subscribed_contracts` here — the strategy
    /// tracks a known, fixed set, so the cache size is fixed + the slot map is
    /// immutable after init.
    pub fn new(
        clock: Arc<crate::backtester::clock::BacktestClock>,
        pool: sqlx::PgPool,
        contracts: &[Contract],
    ) -> Self {
        let n = contracts.len().max(1); // at least 1 slot
        let cache = (0..n).map(|_| AtomicU64::new(0)).collect::<Box<[_]>>();
        let slot_map = contracts
            .iter()
            .enumerate()
            .map(|(i, c)| (HashContract { contract: c.clone() }, i))
            .collect();
        let mut fx_map = HashMap::new();
        // Default fixed rates (strategies that ignore FX just use these).
        fx_map.insert(("USD".to_string(), "SGD".to_string()), 1.35);

        Self {
            clock,
            cache,
            slot_map,
            fx_map,
            pool,
        }
    }

    /// Called by the replayer each tick to publish the current bar's close for
    /// a contract. Lock-free: an atomic store into the contract's pre-assigned
    /// slot. If the contract isn't in the fixed set, it's logged + ignored
    /// (the strategy shouldn't publish closes for contracts it doesn't track).
    pub fn publish_close(&self, contract: &Contract, close: f64) {
        let key = HashContract {
            contract: contract.clone(),
        };
        match self.slot_map.get(&key) {
            Some(&slot) => {
                self.cache[slot].store(close.to_bits(), Ordering::Release);
            }
            None => {
                tracing::warn!(
                    "BacktestPriceSupplier: contract {} not in the fixed cache set; ignoring publish_close",
                    contract.symbol
                );
            }
        }
    }

    /// Look up a fixed FX rate for `contract` (a ForexPair). Checks both the
    /// pair + its reverse. Returns `Some(rate)` where `rate` is in the same
    /// direction as the contract (1 base symbol = rate quote currency).
    fn fx_map_lookup(&self, contract: &Contract) -> Option<f64> {
        if contract.security_type != SecurityType::ForexPair {
            return None;
        }
        let base = contract.symbol.to_string();
        let quote = contract.currency.to_string();
        let key = (base.clone(), quote.clone());
        if let Some(&rate) = self.fx_map.get(&key) {
            return Some(rate);
        }
        let reverse = (quote, base);
        if let Some(&rate) = self.fx_map.get(&reverse) {
            if rate != 0.0 {
                return Some(1.0 / rate);
            }
        }
        None
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
        // 1. Lock-free cache: atomic read from the contract's slot.
        let key = HashContract {
            contract: contract.clone(),
        };
        if let Some(&slot) = self.slot_map.get(&key) {
            let bits = self.cache[slot].load(Ordering::Acquire);
            if bits != 0 {
                return Ok(f64::from_bits(bits));
            }
        }
        // 2. FX map fallback: fixed rate for FX pairs not in the bar stream.
        if let Some(rate) = self.fx_map_lookup(&contract) {
            tracing::debug!(
                "BacktestPriceSupplier: FX map fallback for {} -> {} = {}",
                contract.symbol,
                contract.currency,
                rate
            );
            return Ok(rate);
        }
        // 3. Slow path: point-in-time DB lookup at the backtest clock.
        // TODO: per-asset-type as-of close from historical_data /
        // historical_forex_data (covers arbitrary contracts not in the cache
        // + not in the FX map).
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

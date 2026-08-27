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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;

use crate::helpers::contract::HashContract;
use crate::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};

pub struct BacktestPriceSupplier {
    clock: Arc<crate::backtester::setup::clock::BacktestClock>,
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
    /// Btargetsuild the supplier with a FIXED set of contracts (one cache slot each).
    /// The replayer passes `config.subscribed_contracts` here — the strategy
    /// tracks a known, fixed set, so the cache size is fixed + the slot map is
    /// immutable after init.
    pub fn new(
        clock: Arc<crate::backtester::setup::clock::BacktestClock>,
        pool: sqlx::PgPool,
        contracts: &[Contract],
    ) -> Self {
        let n = contracts.len().max(1); // at least 1 slot
        let cache = (0..n).map(|_| AtomicU64::new(0)).collect::<Box<[_]>>();
        let slot_map = contracts
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    HashContract {
                        contract: c.clone(),
                    },
                    i,
                )
            })
            .collect();

        // Default fixed rates (strategies that ignore FX just use these).
        let fx_map = default_fx_map();

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

        let own_contract = self.slot_map.keys().find(|_| true).unwrap();
        println!("============");
        println!(
            "({}, {}, {})",
            contract.symbol, contract.primary_exchange, contract.currency
        );
        println!(
            "({}, {}, {})",
            own_contract.contract.symbol,
            own_contract.contract.primary_exchange,
            own_contract.contract.currency
        );
        println!("============");
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

/// Snapshot FX rates to SGD, ~2026-08-26 (source: exchange-rates.org).
/// Key: (base_symbol, "SGD"), Value: units of SGD per 1 unit of base.
/// Reverse pairs (SGD -> base) are resolved by inverting, per existing convention.
fn default_fx_map() -> HashMap<(String, String), f64> {
    let mut fx_map = HashMap::new();
    let sgd = "SGD".to_string();

    let rates: &[(&str, f64)] = &[
        // Majors
        ("USD", 1.2713),
        ("EUR", 1.4819),
        ("GBP", 1.7287),
        ("JPY", 0.007983),
        ("CNY", 0.1891),
        ("HKD", 0.1622),
        ("AUD", 0.9132),
        ("NZD", 0.7564),
        ("CAD", 0.9163),
        ("CHF", 1.5794),
        // Europe
        ("SEK", 0.1335),
        ("NOK", 0.1360),
        ("DKK", 0.1982),
        ("PLN", 0.3437),
        ("CZK", 0.06142),
        ("HUF", 0.004092),
        ("RON", 0.2819),
        ("BGN", 0.7577),
        ("RUB", 0.01506),
        ("TRY", 0.02641),
        // Middle East
        ("ILS", 0.4267),
        ("AED", 0.3462),
        ("SAR", 0.3383),
        ("QAR", 0.3488),
        ("KWD", 4.1182),
        ("BHD", 3.3724),
        ("OMR", 3.3066),
        ("JOD", 1.7932),
        // Africa
        ("EGP", 0.02532),
        ("ZAR", 0.07975),
        ("NGN", 0.0009374),
        ("KES", 0.009823),
        ("GHS", 0.1136),
        ("MAD", 0.1375),
        ("DZD", 0.009554),
        ("TND", 0.4386),
        // Asia
        ("INR", 0.01332),
        ("IDR", 0.00007154),
        ("MYR", 0.3158),
        ("THB", 0.03877),
        ("PHP", 0.02063),
        ("VND", 0.00004870),
        ("KRW", 0.0009185),
        ("TWD", 0.03995),
        ("PKR", 0.004579),
        ("BDT", 0.01035),
        ("LKR", 0.003870),
        ("NPR", 0.008333),
        ("KHR", 0.0003143),
        ("MMK", 0.0006055),
        ("BND", 1.0011),
        ("MOP", 0.1575),
        ("SGD", 1.0),
        // Americas
        ("MXN", 0.07502),
        ("BRL", 0.2469),
        ("ARS", 0.0008396),
        ("CLP", 0.001382),
        ("COP", 0.0004109),
        ("PEN", 0.3793),
        ("UYU", 0.03163),
    ];

    for (base, rate) in rates {
        fx_map.insert((base.to_string(), sgd.clone()), *rate);
    }

    fx_map
}

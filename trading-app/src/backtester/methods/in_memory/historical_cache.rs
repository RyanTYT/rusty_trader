//! In-memory cache for the strategy's 4 historical-data queries (the
//! `NoiseOps` + `read_last_vwap` methods). Pre-computed ONCE before the sweep
//! (runs the existing SQL per bar), then read by cfg-gated branches in the
//! CRUD methods during the in-memory backtest — eliminating the per-bar DB
//! queries (the bottleneck for the in-memory sweep).
//!
//! The cache is keyed by `bar_time` (the backtest clock at each bar). The
//! strategy's `on_bar_update` calls the 4 CRUD methods with `now = bar_time`;
//! the cfg-gated branch looks up the pre-computed value for that `bar_time`.
//!
//! Pre-computation cost: ~N_bars × 4 SQL queries (buffered, concurrent). For
//! 100k bars that's ~100-400s one-time — amortized over 1000s of sweep
//! backtests (each backtest then takes ~1-2s instead of ~400s).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::{AssetType, HistoricalStockDataPrimaryKeysWoTime};
use crate::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime, NoiseOps, VwapBarValue,
};
use crate::helpers::contract::get_local_symbol;
use std::cell::RefCell;

/// The 4 pre-computed historical-query values for one bar time.
#[derive(Debug, Clone, Default, Copy)]
pub struct HistoricalQueryValues {
    pub avg_move_since_open: f64,
    pub most_recent_daily_open: f64,
    pub daily_vol: f64,
    pub vwap: f64,
}

/// The pre-computed cache: `bar_time → (avg_move, daily_open, daily_vol, vwap)`.
/// Built once by [`precompute_historical_cache`], shared across all sweep
/// backtests via a thread-local (set per-backtest in `run_with_bars`).
pub struct InMemoryHistoricalCache {
    pub values: HashMap<DateTime<Utc>, HistoricalQueryValues>,
}

impl InMemoryHistoricalCache {
    /// Look up the 4 values for `bar_time`. Returns `None` if not pre-computed
    /// (the cfg-gated CRUD branch falls back to the DB in that case).
    pub fn get(&self, bar_time: &DateTime<Utc>) -> Option<HistoricalQueryValues> {
        self.values.get(bar_time).copied()
    }
}

thread_local! {
    static HISTORICAL_CACHE: RefCell<Option<Arc<InMemoryHistoricalCache>>> =
        const { RefCell::new(None) };
}

/// Set the thread-local historical cache. Called by `InMemoryReplay::run_with_bars`
/// (per-backtest, on the rayon worker thread) so the cfg-gated CRUD branches
/// can look up the pre-computed values.
pub fn set(cache: Arc<InMemoryHistoricalCache>) {
    HISTORICAL_CACHE.with(|c| *c.borrow_mut() = Some(cache));
}

/// Get the thread-local historical cache, if set. Called by the cfg-gated
/// CRUD branches.
pub fn current() -> Option<Arc<InMemoryHistoricalCache>> {
    HISTORICAL_CACHE.with(|c| c.borrow().clone())
}

/// Clear the thread-local cache. Called by `run_with_bars` after the loop.
pub fn clear() {
    HISTORICAL_CACHE.with(|c| *c.borrow_mut() = None);
}

/// Pre-compute the 4 historical-query values for every bar in `bars`, by
/// running the existing SQL queries once per bar (concurrent, buffered to
/// avoid overwhelming Postgres). The result is shared across all sweep
/// backtests (the bars are the same for every backtest).
///
/// `bars_contract` is the contract the bars are for (e.g. QQQ/NASDAQ/USD) —
/// the 4 queries are keyed by its stock/primary_exchange/currency.
pub async fn precompute_historical_cache(
    pool: &PgPool,
    bars: &[crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys],
    bars_contract: &ibapi::contracts::Contract,
) -> Arc<InMemoryHistoricalCache> {
    let stock = get_local_symbol(bars_contract);
    let pe = bars_contract.primary_exchange.to_string();
    let currency = bars_contract.currency.to_string();
    let crud = HistoricalDataCRUD::from(&AssetType::Stock, pool.clone());

    // Buffer the bars — run 8 concurrently (each running 4 queries via tokio::join!
    // → ~32 concurrent queries, a reasonable Postgres load).
    let results: Vec<(DateTime<Utc>, HistoricalQueryValues)> = stream::iter(bars.iter())
        .map(|bar| {
            let crud = crud.clone();
            let stock = stock.clone();
            let pe = pe.clone();
            let currency = currency.clone();
            let bar_time = bar.get_time();
            async move {
                let pk = HistoricalStockDataPrimaryKeysWoTime {
                    stock: stock.clone(),
                    primary_exchange: pe.clone(),
                    currency: currency.clone(),
                };
                let vwap_pk =
                    HistoricalDataPrimaryKeysWoTime::Stock(pk.clone());
                let (avg, open, vol, vwap) = tokio::join!(
                    crud.get_avg_move_since_open(pk.clone(), bar_time),
                    crud.get_most_recent_daily_open(pk.clone(), bar_time),
                    crud.get_daily_vol(pk.clone(), bar_time),
                    crud.read_last_vwap(
                        vwap_pk,
                        Some("US/Eastern".to_string()),
                        VwapBarValue::Close,
                        bar_time,
                    ),
                );
                (
                    bar_time,
                    HistoricalQueryValues {
                        avg_move_since_open: avg.unwrap_or_else(|e| {
                            tracing::warn!("precompute avg_move_since_open: {e:?}");
                            0.0
                        }),
                        most_recent_daily_open: open.unwrap_or_else(|e| {
                            tracing::warn!("precompute most_recent_daily_open: {e:?}");
                            0.0
                        }),
                        daily_vol: vol.unwrap_or_else(|e| {
                            tracing::warn!("precompute daily_vol: {e:?}");
                            0.0
                        }),
                        vwap: vwap.ok().flatten().unwrap_or_else(|| {
                            tracing::warn!("precompute vwap: none");
                            0.0
                        }),
                    },
                )
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;

    let mut values = HashMap::with_capacity(results.len());
    for (bar_time, vals) in results {
        values.insert(bar_time, vals);
    }
    tracing::info!("Pre-computed historical cache: {} bars", values.len());
    Arc::new(InMemoryHistoricalCache { values })
}

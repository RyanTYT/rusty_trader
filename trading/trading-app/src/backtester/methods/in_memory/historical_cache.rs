//! In-memory cache for the strategy's historical-data queries. Pre-computed
//! ONCE before the sweep (runs the SQL per bar), then read by cfg-gated
//! branches in the CRUD methods during the in-memory backtest — eliminating
//! the per-bar DB queries (the bottleneck for the in-memory sweep).
//!
//! **Strategy-driven** (not hardcoded to the Noise strategy): the strategy
//! declares its queries via [`CacheQuery`] (a trait — the strategy implements
//! `run()` which calls its own SQL functions). The
//! `precompute_historical_cache` runs each query's `run()` per bar + stores
//! the results in a `HashMap<String, f64>` per bar (keyed by `name()`). The
//! CRUD methods look up the cache by name. The cache module does NOT know
//! which SQL functions the strategy calls — the strategy owns that mapping.
//!
//! The cache is keyed by `bar_time` (the backtest clock at each bar). The
//! strategy's `on_bar_update` calls the CRUD methods with `now = bar_time`;
//! the cfg-gated branch looks up the pre-computed value for that `bar_time`.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::{AssetType, HistoricalStockDataPrimaryKeysWoTime};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataCRUD;
use crate::helpers::contract::get_local_symbol;
use std::cell::RefCell;

/// A SQL query a strategy wants pre-computed per bar (the strategy-driven
/// cache). The strategy implements this trait for each of its queries — the
/// `run()` method calls the strategy's own SQL function (the cache module
/// doesn't know which functions exist). The `precompute_historical_cache`
/// runs each query's `run()` per bar + stores the result in a
/// `HashMap<String, f64>` keyed by `name()`.
///
/// The strategy declares its queries via `StrategyExecutor::cache_queries()`
/// (returns `Vec<Arc<dyn CacheQuery>>`). The lookback params are read from
/// the strategy's `backtest_params` (fixed per-sweep) — the cache + the
/// strategy's `on_bar_update` use the SAME values.
#[async_trait::async_trait]
pub trait CacheQuery: Send + Sync {
    /// The name string (the cache key). The CRUD methods look up the cache by
    /// this name.
    fn name(&self) -> &'static str;
    /// Run the SQL query for one bar. `crud` is the HistoricalDataCRUD (the
    /// SQL functions); `pk` is the stock/pe/currency; `bar_time` is the bar's
    /// time (the `now` for look-ahead-free queries).
    async fn run(
        &self,
        crud: &HistoricalDataCRUD,
        pk: &HistoricalStockDataPrimaryKeysWoTime,
        bar_time: DateTime<Utc>,
    ) -> Result<f64, String>;
}

/// The pre-computed cache: `bar_time → (name → value)`. Built once by
/// [`precompute_historical_cache`], shared across all sweep backtests via a
/// thread-local (set per-backtest in `run_with_bars`).
pub struct InMemoryHistoricalCache {
    pub values: HashMap<DateTime<Utc>, HashMap<String, f64>>,
}

impl InMemoryHistoricalCache {
    /// Look up the values for `bar_time`. Returns `None` if not pre-computed
    /// (the cfg-gated CRUD branch falls back to the DB in that case).
    pub fn get(&self, bar_time: &DateTime<Utc>) -> Option<&HashMap<String, f64>> {
        self.values.get(bar_time)
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

/// Pre-compute the strategy's declared queries for every bar in `bars`, by
/// running each query's `run()` once per bar (concurrent, buffered to avoid
/// overwhelming Postgres). The result is shared across all sweep backtests
/// (the bars are the same for every backtest).
///
/// `bars_contract` is the contract the bars are for (e.g. QQQ/NASDAQ/USD) —
/// the queries are keyed by its stock/primary_exchange/currency.
/// `cache_queries` is the strategy's declared queries (from
/// `StrategyExecutor::cache_queries()`). The cache module does NOT interpret
/// the queries — it just calls `q.run()` per bar.
pub async fn precompute_historical_cache(
    pool: &PgPool,
    bars: &[crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys],
    bars_contract: &ibapi::contracts::Contract,
    cache_queries: &[Arc<dyn CacheQuery>],
) -> Arc<InMemoryHistoricalCache> {
    let stock = get_local_symbol(bars_contract);
    let pe = bars_contract.primary_exchange.to_string();
    let currency = bars_contract.currency.to_string();
    let crud = HistoricalDataCRUD::from(&AssetType::Stock, pool.clone());

    // Buffer the bars — run 8 concurrently (each running the declared queries
    // sequentially via a for loop → ~8 × N_queries concurrent queries).
    let results: Vec<(DateTime<Utc>, HashMap<String, f64>)> = stream::iter(bars.iter())
        .map(|bar| {
            let crud = crud.clone();
            let stock = stock.clone();
            let pe = pe.clone();
            let currency = currency.clone();
            let cache_queries = cache_queries.to_vec();
            let bar_time = bar.get_time();
            async move {
                let pk = HistoricalStockDataPrimaryKeysWoTime {
                    stock: stock.clone(),
                    primary_exchange: pe.clone(),
                    currency: currency.clone(),
                };
                let mut values = HashMap::new();
                for q in &cache_queries {
                    let name = q.name();
                    let val = q.run(&crud, &pk, bar_time).await.unwrap_or_else(|e| {
                        tracing::warn!("precompute {name}: {e:?}");
                        0.0
                    });
                    values.insert(name.to_string(), val);
                }
                (bar_time, values)
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;

    let mut values_map = HashMap::with_capacity(results.len());
    for (bar_time, vals) in results {
        values_map.insert(bar_time, vals);
    }
    tracing::info!("Pre-computed historical cache: {} bars", values_map.len());
    Arc::new(InMemoryHistoricalCache { values: values_map })
}

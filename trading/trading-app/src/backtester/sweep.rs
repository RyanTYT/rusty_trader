//! `run_one_backtest` — run a single in-memory backtest for a given param
//! set. Shared with the optimizer crate (the research/optimization layer
//! builds a param grid + runs each via this + scores the results).
//!
//! The backtester binary itself no longer runs param sweeps — it runs one
//! backtest per active strategy in `backtest.json` (see `run_backtest` in
//! the parent module). Sweeps / grids / TPE are the optimizer's job.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;

use crate::backtester::methods::in_memory::replay::InMemoryReplay;
use crate::backtester::output::results::BacktestResults;
use crate::backtester::setup::config::BacktestConfig;
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

/// One sweep result: the params that produced it + the computed metrics.
#[derive(Debug, Serialize)]
pub struct SweepResult {
    pub params: HashMap<String, f64>,
    pub results: BacktestResults,
}

/// Run a single in-memory backtest (one sweep entry). Constructs the
/// per-backtest pieces (strategy with params, clock, prices, consolidator),
/// runs `InMemoryReplay::run_with_bars`, + computes the results in-memory.
/// No `BacktestContext` (avoids `OrderStore::open()` file contention across
/// parallel backtests — the order_store is unused in-memory).
///
/// `pub` so the `optimizer` crate (the research/optimization layer) can call
/// it directly — the optimizer builds a param grid, runs each via this, +
/// scores the results.
///
/// Bars should be in a 2D shape of e.g.
///
/// Subscribed to AAPL, QQQ
///
/// [[AAPL_0, QQQ_0], [AAPL_1, QQQ_1], ...]
///
/// for cache friendliness
pub fn run_one_backtest(
    name: &str,
    pool: &sqlx::PgPool,
    config: &BacktestConfig,
    params: &HashMap<String, f64>,
    bars: Arc<Vec<Vec<Option<HistoricalDataFullKeys>>>>,
    handle: &tokio::runtime::Handle,
) -> Result<SweepResult, String> {
    // Build a minimal StrategySpec from the param map — no per-strategy
    // contracts / benchmark → relative uses its hardcoded defaults (the
    // optimizer doesn't override the instrument set). The params ARE applied
    // via construct_strategy (which now calls with_backtest_params for
    // relative too — fixing the latent relative-params-drop bug).
    let spec = crate::strategy::StrategySpec {
        active: true,
        params: params.clone(),
        contracts: Vec::new(),
        benchmark: None,
    };
    let strategy = crate::strategy::construct_strategy(name, &spec, pool.clone(), handle.clone())
        .ok_or_else(|| format!("Unknown strategy '{name}'"))?;
    // Build the light execution surface (clock/prices/consolidator) — shared
    // with BacktestContext::build, avoids the OrderStore::open() file contention.
    let light = crate::backtester::setup::context::build_light_context(pool, config);
    // Set the bar cache, warm up, trim to post-warm-up, run_with_bars, clear
    // — all in one helper (shared with the single route).
    let (equity, state) =
        InMemoryReplay.run_with_warm_up(strategy, bars, config, handle, &light)?;
    let results = BacktestResults::compute_in_memory(
        &equity,
        &state,
        config.starting_capital_sgd,
        config.stock_bar_interval,
    );
    Ok(SweepResult {
        params: params.clone(),
        results,
    })
}

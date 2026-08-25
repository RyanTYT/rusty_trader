//! Backtester — replay historical bars through the REAL prod strategy +
//! reconciliation against test-db, with a simulated broker + point-in-time
//! price oracle. Feature-gated on `backtest`.
//!
//! # Architecture
//! - [`BacktestConfig`] — the user-facing interface layer (bar granularity,
//!   lookback period, capital, fees, commission model, mode). Built via the
//!   fluent builder or [`BacktestConfig::from_env`].
//! - [`LightContext`] / [`BacktestContext`] — the execution surface. The light
//!   context (clock/prices/consolidator) is used by the InMemory path; the
//!   full context (+ broker/order_engine/order_store) by the Db path.
//! - [`BacktestMethod`] trait — the Db-backed `HistoricalReplay` method. The
//!   InMemory path uses [`InMemoryReplay::run_with_warm_up`] directly.
//! - [`run_backtest`] — thin dispatch: scan for `*.json` sweep files →
//!   [`run_sweep_route`]; else [`run_single_route`].
//!
//! # Layout
//! - `setup/` — the once-constructed types (config, context, clock, seed).
//! - `output/` — the equity curve + results.
//! - `execution/` — the order/fill surface (broker, fill model, OrderSubmitter).
//! - `oracle/` — the price/data surface (price supplier, data loader).
//! - `methods/` — the backtest methods (`HistoricalReplay`, `InMemoryReplay`).
//!
//! Build & run:
//!   cargo run --bin backtest --features backtest

pub mod execution;
pub mod methods;
pub mod oracle;
pub mod output;
pub mod setup;
pub mod sweep;

pub use setup::clock::BacktestClock;
pub use setup::config::{BacktestConfig, BacktestMode, BacktestPeriod};
pub use setup::context::{BacktestContext, LightContext, build_light_context};
pub use output::equity::EquityCurve;
pub use output::results::BacktestResults;
pub use execution::{BacktestBroker, CommissionModel, OrderSubmitter};
pub use methods::{BacktestMethod, HistoricalReplay, InMemoryReplay};
pub use oracle::BacktestPriceSupplier;

use std::sync::Arc;

use sqlx::PgPool;

use crate::strategy::strategy::StrategyExecutor;

/// Entry point. Dispatches: scan the working dir for `*.json` sweep files →
/// [`run_sweep_route`]; else [`run_single_route`] (env-var params).
pub async fn run_backtest(pool: PgPool, config: BacktestConfig) -> Result<(), String> {
    let handle = tokio::runtime::Handle::current();

    // Scan the working dir for `*.json` sweep files (each filename names a
    // strategy; the file is the param grid).
    let sweep_names: Vec<String> = std::fs::read_dir(".")
        .map_err(|e| format!("read working dir: {e}"))?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();
    if !sweep_names.is_empty() {
        return run_sweep_route(pool, config, handle, sweep_names).await;
    }
    run_single_route(pool, config, handle).await
}

/// Sweep route: for each `*.json` sweep file (named after a strategy), run a
/// parallel sweep (rayon, core-sized pool). `construct_strategies` filters to
/// recognised strategies (only the chosen run). Results stream to an I/O
/// thread (JSONL output for live `tail -f` updates). The bars are loaded
/// once + shared across all strategies' sweeps via `Arc`.
async fn run_sweep_route(
    pool: PgPool,
    config: BacktestConfig,
    handle: tokio::runtime::Handle,
    sweep_names: Vec<String>,
) -> Result<(), String> {
    // Discovery: construct_strategies filters to recognised strategies.
    let strategies = crate::strategy::construct_strategies(
        sweep_names,
        pool.clone(),
        handle.clone(),
    );
    if strategies.is_empty() {
        return Err("No recognised strategies found in *.json sweep files".into());
    }
    // Load the bars once (shared across all strategies' sweeps via Arc).
    let bars = crate::backtester::methods::load_bars(&config, &pool).await?;
    let bars_arc = Arc::new(bars);
    for strategy in strategies {
        let name = strategy.get_name();
        let sweep_file = std::path::PathBuf::from(format!("{name}.json"));
        if !sweep_file.exists() {
            continue;
        }
        let param_grid = crate::backtester::sweep::parse_sweep_file(&sweep_file);
        tracing::info!("Sweep: {} backtests from {name}.json", param_grid.len());
        let pool_clone = pool.clone();
        let handle_clone = handle.clone();
        let config_clone = config.clone();
        let bars_clone = bars_arc.clone();
        let name_clone = name.clone();
        tokio::task::spawn_blocking(move || {
            crate::backtester::sweep::run_backtest_sweep(
                &name_clone,
                pool_clone,
                &config_clone,
                &param_grid,
                bars_clone,
                &handle_clone,
            )
        })
        .await
        .map_err(|e| format!("sweep join: {e:?}"))??;
    }
    Ok(())
}

/// Single-backtest route (no `*.json` sweep files): construct the chosen
/// strategies (from `config.strategy_params`) with their env-var params + run
/// each. InMemory mode uses the light context + `run_with_warm_up` (no
/// `OrderStore::open()`); Db mode uses the full `BacktestContext` +
/// `HistoricalReplay`.
async fn run_single_route(
    pool: PgPool,
    config: BacktestConfig,
    handle: tokio::runtime::Handle,
) -> Result<(), String> {
    // 1. Load market data (only for TimeRange; NumBars assumes bars are in the DB).
    if let BacktestPeriod::TimeRange { start, end } = &config.period {
        crate::backtester::oracle::data_loader::load_market_data(
            &config.subscribed_contracts,
            *start,
            *end,
            &pool,
            &handle,
        )
        .await?;
    }

    let starting_capital = config.starting_capital_sgd;
    let mode = config.mode;
    for (name, params) in &config.strategy_params {
        let strategy = crate::strategy::construct_strategy(
            name,
            pool.clone(),
            handle.clone(),
            Some(params.clone()),
        )
        .ok_or_else(|| format!("Unknown strategy '{name}'"))?;

        // 2. Seed the initial capital (Db mode only). For InMemory mode the
        //    `InMemoryReplay` seeds the `InMemoryState` internally.
        if mode == BacktestMode::Db {
            crate::backtester::setup::seed::seed_initial_capital(
                &pool,
                &strategy.get_name(),
                starting_capital,
            )
            .await?;
        }

        // 3. Run on a blocking thread (sync strategy fns + `handle.block_on`
        //    need a non-tokio thread). InMemory uses the light context; Db
        //    uses the full BacktestContext.
        let results = match mode {
            BacktestMode::InMemory => {
                let bars = crate::backtester::methods::load_bars(&config, &pool).await?;
                let bars_arc = Arc::new(bars);
                let config_clone = config.clone();
                let pool_clone = pool.clone();
                let handle_clone = handle.clone();
                let (equity, state) = tokio::task::spawn_blocking(move || {
                    let light = build_light_context(&pool_clone, &config_clone);
                    InMemoryReplay.run_with_warm_up(
                        strategy,
                        bars_arc,
                        &config_clone,
                        &handle_clone,
                        &light,
                    )
                })
                .await
                .map_err(|e| format!("replayer join: {e:?}"))??;
                BacktestResults::compute_in_memory(&equity, &state, starting_capital)
            }
            BacktestMode::Db => {
                let ctx = BacktestContext::build(
                    config.clone(),
                    pool.clone(),
                    handle.clone(),
                    strategy,
                );
                let equity = tokio::task::spawn_blocking(move || HistoricalReplay.run(ctx))
                    .await
                    .map_err(|e| format!("replayer join: {e:?}"))??;
                BacktestResults::compute(&pool, &equity, starting_capital).await?
            }
        };

        // 4. Results.
        let output_path = format!("backtest_results_{name}.json");
        results.write_json(&output_path)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&results).map_err(|e| format!("serialize: {e}"))?
        );
    }
    Ok(())
}

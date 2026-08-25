//! Backtester — replay historical bars through the REAL prod strategy +
//! reconciliation against test-db, with a simulated broker + point-in-time
//! price oracle. Feature-gated on `backtest`.
//!
//! # Architecture
//! - [`BacktestConfig`] — the user-facing interface layer (bar granularity,
//!   lookback period, capital, fees, commission model, mode). Built via the
//!   fluent builder or [`BacktestConfig::from_env`].
//! - [`BacktestContext`] — the shared execution surface (broker, price oracle,
//!   consolidator, order engine, order store, strategy). Built once.
//! - [`BacktestMethod`] trait — each method (e.g., [`HistoricalReplay`]) takes
//!   a `&BacktestContext` + produces an [`EquityCurve`].
//! - [`run_backtest`] — thin orchestration: data load → seed → context →
//!   method → results.
//!
//! # Layout
//! - `execution/` — the order/fill surface (broker, fill model, OrderSubmitter).
//! - `oracle/` — the price/data surface (price supplier, data loader).
//! - `methods/` — the backtest methods (`HistoricalReplay`, `InMemoryReplay`).
//!
//! Build & run:
//!   cargo run --bin backtest --features backtest

pub mod clock;
pub mod config;
pub mod context;
pub mod equity;
pub mod execution;
pub mod methods;
pub mod oracle;
pub mod results;
pub mod seed;
pub mod sweep;

pub use clock::BacktestClock;
pub use config::{BacktestConfig, BacktestMode, BacktestPeriod};
pub use context::BacktestContext;
pub use equity::EquityCurve;
pub use execution::{BacktestBroker, CommissionModel, OrderSubmitter};
pub use methods::{BacktestMethod, HistoricalReplay, InMemoryReplay};
pub use oracle::BacktestPriceSupplier;
pub use results::BacktestResults;

use sqlx::PgPool;
use std::sync::Arc;

use crate::strategy::strategy::StrategyExecutor;

/// Entry point. Accepts a fully-built [`BacktestConfig`] (the interface layer).
/// Thin orchestration: data load → seed → context → method → results.
pub async fn run_backtest(pool: PgPool, config: BacktestConfig) -> Result<(), String> {
    let handle = tokio::runtime::Handle::current();

    // Sweep route: scan the working dir for `*.json` sweep files. Each
    // filename (e.g. `noise.json`) names a strategy; the file is the param
    // grid (a JSON array of param objects). `construct_strategies` filters
    // to recognised strategies (only the chosen run). For each, run a
    // parallel sweep (rayon, core-sized pool). Results stream to an I/O
    // thread (JSONL output for live `tail -f` updates).
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
        // Discovery: construct_strategies filters to valid strategies.
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
        return Ok(());
    }

    // ── Single-backtest route (no *.json sweep files) ─────────────────────
    // 1. Load market data (only for TimeRange; NumBars assumes bars are
    //    already in the DB from a prior load).
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

    // 2. Construct the chosen strategies (from `config.strategy_params`) with
    //    their env-var params + run each. `construct_strategy` is the single
    //    name→variant mapping (no hardcoded Noise::new()).
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

        // 3. Seed the initial capital (Db mode only). For InMemory mode the
        //    `InMemoryReplay` seeds the `InMemoryState` internally.
        if mode == BacktestMode::Db {
            crate::backtester::seed::seed_initial_capital(
                &pool,
                &strategy.get_name(),
                starting_capital,
            )
            .await?;
        }

        // 4. Build the shared execution surface + run the method on a blocking
        //    thread (sync strategy fns + `handle.block_on` need a non-tokio thread).
        let ctx = BacktestContext::build(config.clone(), pool.clone(), handle.clone(), strategy);
        let equity = tokio::task::spawn_blocking(move || match mode {
            BacktestMode::InMemory => InMemoryReplay.run(ctx),
            BacktestMode::Db => HistoricalReplay.run(ctx),
        })
        .await
        .map_err(|e| format!("replayer join: {e:?}"))??;

        // 5. Results.
        let results = BacktestResults::compute(&pool, &equity, starting_capital).await?;
        let output_path = format!("backtest_results_{name}.json");
        results.write_json(&output_path)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&results).map_err(|e| format!("serialize: {e}"))?
        );
    }
    Ok(())
}

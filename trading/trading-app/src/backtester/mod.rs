//! Backtester — replay historical bars through the REAL prod strategy +
//! reconciliation against test-db, with a simulated broker + point-in-time
//! price oracle. Feature-gated on `backtest`.
//!
//! # Architecture
//! - [`BacktestConfig`] — the user-facing interface layer (bar granularity,
//!   lookback period, capital, fees, commission model, mode, + the
//!   per-strategy specs). Built via the fluent builder from `backtest.json`.
//! - [`LightContext`] / [`BacktestContext`] — the execution surface. The light
//!   context (clock/prices/consolidator) is used by the InMemory path; the
//!   full context (+ broker/order_engine/order_store) by the Db path.
//! - [`BacktestMethod`] trait — the Db-backed `HistoricalReplay` method. The
//!   InMemory path uses [`InMemoryReplay::run_with_warm_up`] directly.
//! - [`run_backtest`] — runs each active strategy in `config.strategies`
//!   (one backtest per strategy, with the directly-defined params from
//!   `backtest.json`).
//!
//! # Layout
//! - `setup/` — the once-constructed types (config, context, clock, seed).
//! - `output/` — the equity curve + results.
//! - `execution/` — the order/fill surface (broker, fill model, OrderSubmitter).
//! - `oracle/` — the price/data surface (price supplier, data loader).
//! - `methods/` — the backtest methods (`HistoricalReplay`, `InMemoryReplay`).
//! - `sweep/` — `run_one_backtest` (shared with the optimizer crate).
//!
//! Build & run:
//!   cargo run --bin backtest --features backtest

pub mod execution;
pub mod methods;
pub mod oracle;
pub mod output;
pub mod setup;
pub mod sweep;

pub use execution::{BacktestBroker, CommissionModel, OrderSubmitter};
pub use methods::{BacktestMethod, HistoricalReplay, InMemoryReplay};
pub use oracle::BacktestPriceSupplier;
pub use output::equity::EquityCurve;
pub use output::results::BacktestResults;
pub use setup::clock::BacktestClock;
pub use setup::config::{BacktestConfig, BacktestMode, BacktestPeriod};
pub use setup::context::{BacktestContext, LightContext, build_light_context};

use std::sync::Arc;

use sqlx::PgPool;

use crate::strategy::strategy::StrategyExecutor;

/// Entry point. Runs each active strategy in `config.strategies` (one
/// backtest per strategy, with the directly-defined params from
/// `backtest.json`). The `config.subscribed_contracts` (the union of all
/// active strategies' contracts + benchmarks, built by the binary) is
/// loaded once for the period.
pub async fn run_backtest(pool: PgPool, config: BacktestConfig) -> Result<(), String> {
    let handle = tokio::runtime::Handle::current();
    run_single_route(pool, config, handle).await
}

/// Run each active strategy in `config.strategies`. InMemory mode uses the
/// light context + `run_with_warm_up` (no `OrderStore::open()`); Db mode
/// uses the full `BacktestContext` + `HistoricalReplay`. Results are
/// written to `backtest_results_{name}.json` per strategy.
async fn run_single_route(
    pool: PgPool,
    config: BacktestConfig,
    handle: tokio::runtime::Handle,
) -> Result<(), String> {
    // 1. Load market data for the union of all active strategies' contracts
    //    (only for TimeRange; NumBars assumes bars are in the DB).
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

    // Iterate strategies in a stable order (sorted by name) for deterministic
    // output ordering. Inactive strategies are logged + skipped.
    let mut entries: Vec<(&String, &crate::strategy::StrategySpec)> =
        config.strategies.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for (name, spec) in entries {
        if !spec.active {
            tracing::info!("Strategy '{name}' inactive (active=false) — skipping");
            continue;
        }
        let name_str = name.as_str();
        let strategy = crate::strategy::construct_strategy(name_str, spec, pool.clone(), handle.clone())
            .ok_or_else(|| format!("Unknown strategy '{name_str}'"))?;

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
                let bars = crate::backtester::methods::load_bars(
                    &config,
                    &pool,
                    strategy.warmup_bars_required(),
                )
                .await?;
                let bars_arc = Arc::new(crate::backtester::methods::transpose(bars));
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
                BacktestResults::compute_in_memory(
                    &equity,
                    &state,
                    starting_capital,
                    config.stock_bar_interval,
                )
            }
            BacktestMode::Db => {
                let ctx =
                    BacktestContext::build(config.clone(), pool.clone(), handle.clone(), strategy);
                let equity = tokio::task::spawn_blocking(move || HistoricalReplay.run(ctx))
                    .await
                    .map_err(|e| format!("replayer join: {e:?}"))??;
                BacktestResults::compute(
                    &pool,
                    &equity,
                    starting_capital,
                    config.stock_bar_interval,
                )
                .await?
            }
        };

        // 4. Results.
        let output_path = format!("backtest_results_{name_str}.json");
        results.write_json(&output_path)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&results).map_err(|e| format!("serialize: {e}"))?
        );
    }
    Ok(())
}

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

use crate::strategy::noise::Noise;
use crate::strategy::strategy::{StrategyEnum, StrategyExecutor};

/// Entry point. Accepts a fully-built [`BacktestConfig`] (the interface layer).
/// Thin orchestration: data load → seed → context → method → results.
pub async fn run_backtest(pool: PgPool, config: BacktestConfig) -> Result<(), String> {
    let handle = tokio::runtime::Handle::current();

    // Sweep route: if a `<strategy>.json` file (e.g. `noise.json`) is found in
    // the working directory, parse it as a param grid + run N backtests in
    // parallel (rayon, core-sized pool). Results stream to an I/O thread
    // (JSONL output for live `tail -f` updates). Else, fall through to the
    // normal single-backtest route (env-var params).
    let sweep_file = std::path::Path::new("noise.json");
    if sweep_file.exists() {
        let param_grid = crate::backtester::sweep::parse_sweep_file(sweep_file);
        tracing::info!("Sweep: {} backtests from noise.json", param_grid.len());
        // Load the bars once (shared across all sweep backtests — no per-backtest DB query).
        let bars = crate::backtester::methods::load_bars(&config, &pool).await?;
        // Pre-compute the 4 historical-query values per bar ONCE (the strategy's
        // `NoiseOps` + `read_last_vwap` queries), shared across all sweep
        // backtests via a thread-local. Eliminates the per-bar DB queries.
        let bars_contract = config
            .subscribed_contracts
            .first()
            .cloned()
            .expect("subscribed_contracts non-empty");
        let historical_cache =
            crate::backtester::methods::in_memory::historical_cache::precompute_historical_cache(
                &pool,
                &bars,
                &bars_contract,
                &[],
            )
            .await;
        // Run the sweep (sync, CPU-bound) on a blocking thread.
        let pool_clone = pool.clone();
        let handle_clone = handle.clone();
        tokio::task::spawn_blocking(move || {
            crate::backtester::sweep::run_backtest_sweep(
                pool_clone,
                &config,
                &param_grid,
                &bars,
                &historical_cache,
                &handle_clone,
            )
        })
        .await
        .map_err(|e| format!("sweep join: {e:?}"))??;
        return Ok(());
    }

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

    // 2. Real prod strategy — the SAME Noise that runs in production. Under
    //    backtest, inject the generic strategy params (from NOISE_<VAR> env
    //    vars) so the strategy's cfg-gated `on_bar_update` branches use them.
    let mut noise = Noise::new(pool.clone(), handle.clone());
    #[cfg(feature = "backtest")]
    {
        if let Some(noise_params) = config.strategy_params.get("noise") {
            noise = noise.with_backtest_params(noise_params.clone());
        }
    }
    let strategy = StrategyEnum::Noise(noise);

    // 3. Seed the initial capital. For Db mode, seed CASH:SGD in the DB
    //    (the strategy's `get_strategy_sgd_value` reads this as available
    //    cash). For InMemory mode, the `InMemoryReplay` seeds the
    //    `InMemoryState` internally (no DB seed needed).
    if config.mode == BacktestMode::Db {
        crate::backtester::seed::seed_initial_capital(
            &pool,
            &strategy.get_name(),
            config.starting_capital_sgd,
        )
        .await?;
    }

    // 4. Build the shared execution surface + run the method on a blocking
    //    thread (sync strategy fns + `handle.block_on` need a non-tokio thread).
    let starting_capital = config.starting_capital_sgd;
    let output_path = config.output_path.clone();
    let mode = config.mode;
    let ctx = BacktestContext::build(config, pool.clone(), handle.clone(), strategy);
    let equity = tokio::task::spawn_blocking(move || match mode {
        BacktestMode::InMemory => InMemoryReplay.run(ctx),
        BacktestMode::Db => HistoricalReplay.run(ctx),
    })
    .await
    .map_err(|e| format!("replayer join: {e:?}"))??;

    // 5. Results.
    let results = BacktestResults::compute(&pool, &equity, starting_capital).await?;
    results.write_json(&output_path)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&results).map_err(|e| format!("serialize: {e}"))?
    );
    Ok(())
}

//! Sweep runner — runs N in-memory backtests in parallel (a parameter sweep)
//! using a rayon pool sized to the core count. Results stream to an I/O
//! thread via a channel (JSONL output for live `tail -f` updates).
//!
//! Triggered when a `<strategy>.json` file (e.g. `noise.json`) is found in
//! the working directory — parsed as a `Vec<HashMap<String, f64>>` (a JSON
//! array of param objects), each entry becomes one backtest. If no file is
//! found, `run_backtest` runs a single backtest (env-var params).

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use rayon::scope;
use serde::Serialize;

use crate::market_data::consolidator::Consolidator;
use crate::market_data::handler::MarketDataHandler;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::noise::Noise;
use crate::strategy::strategy::StrategyEnum;

use crate::backtester::clock::BacktestClock;
use crate::backtester::config::BacktestConfig;
use crate::backtester::methods::in_memory::historical_cache::{self, InMemoryHistoricalCache};
use crate::backtester::methods::in_memory::replay::{InMemoryReplay, InMemoryRunContext};
use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;
use crate::backtester::results::BacktestResults;
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

/// One sweep result: the params that produced it + the computed metrics.
#[derive(Debug, Serialize)]
pub struct SweepResult {
    pub params: HashMap<String, f64>,
    pub results: BacktestResults,
}

/// Parse a sweep JSON file as `Vec<HashMap<String, f64>>` (a JSON array of
/// param objects). Panics if the file can't be read or the structure doesn't
/// match (the user asked for "assumed structure — panic if not").
pub fn parse_sweep_file(path: &Path) -> Vec<HashMap<String, f64>> {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read sweep file {path:?}: {e}"));
    serde_json::from_str::<Vec<HashMap<String, f64>>>(&content).unwrap_or_else(|e| {
        panic!(
            "sweep file {path:?} must be a JSON array of objects (string keys + numeric values): {e}"
        )
    })
}

/// Run N in-memory backtests in parallel (rayon scope, core-sized pool). Each
/// backtest's result streams to an I/O thread via a channel (JSONL output for
/// live updates). Blocks until all backtests finish + the I/O thread drains.
pub fn run_backtest_sweep(
    pool: sqlx::PgPool,
    config: &BacktestConfig,
    param_grid: &[HashMap<String, f64>],
    bars: &[HistoricalDataFullKeys],
    historical_cache: &Arc<InMemoryHistoricalCache>,
    handle: &tokio::runtime::Handle,
) -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel::<SweepResult>();
    let output_path = config.output_path.clone();

    // I/O thread — writes each result as a JSONL line as it arrives (flush
    // after each write so `tail -f` shows live updates).
    let io_thread = std::thread::spawn(move || -> Result<(), String> {
        let mut file =
            std::fs::File::create(&output_path).map_err(|e| format!("create output file: {e}"))?;
        for result in rx {
            let line = serde_json::to_string(&result)
                .map_err(|e| format!("serialize sweep result: {e}"))?;
            writeln!(file, "{line}").map_err(|e| format!("write output: {e}"))?;
            file.sync_all().ok();
        }
        Ok(())
    });

    // Sweep — each task owns a cloned Sender (mpsc::Sender is !Sync, so we
    // can't share a &Sender across rayon tasks; clone-per-task avoids a lock).
    scope(|s| {
        for params in param_grid {
            let task_tx = tx.clone();
            let task_pool = pool.clone();
            let task_handle = handle.clone();
            let task_params = params.clone();
            let task_cache = historical_cache.clone();
            s.spawn(move |_| {
                match run_one_backtest(
                    &task_pool,
                    config,
                    &task_params,
                    bars,
                    &task_cache,
                    &task_handle,
                ) {
                    Ok(result) => {
                        let _ = task_tx.send(result);
                    }
                    Err(e) => {
                        tracing::error!("sweep backtest failed (params {:?}): {e:?}", task_params)
                    }
                }
            });
        }
    });
    drop(tx); // close the channel → the I/O thread's rx iterator ends.

    io_thread
        .join()
        .map_err(|_| "I/O thread panicked".to_string())??;
    Ok(())
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
pub fn run_one_backtest(
    pool: &sqlx::PgPool,
    config: &BacktestConfig,
    params: &HashMap<String, f64>,
    bars: &[HistoricalDataFullKeys],
    historical_cache: &Arc<InMemoryHistoricalCache>,
    handle: &tokio::runtime::Handle,
) -> Result<SweepResult, String> {
    let noise = Noise::new(pool.clone(), handle.clone()).with_backtest_params(params.clone());
    let strategy = StrategyEnum::Noise(noise);
    let clock = Arc::new(BacktestClock::new());
    let prices = Arc::new(BacktestPriceSupplier::new(
        clock.clone(),
        pool.clone(),
        &config.subscribed_contracts,
    ));
    let market_data_handler = MarketDataHandler::new(pool.clone());
    let consolidator = Arc::new(Consolidator::new_for_backtest(
        pool.clone(),
        prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
        market_data_handler,
    ));
    let in_mem_ctx = InMemoryRunContext {
        config,
        strategy: strategy,
        handle,
        clock: &clock,
        prices: &prices,
        consolidator: &consolidator,
        historical_cache: Some(historical_cache.clone()),
    };
    let (equity, state) = InMemoryReplay.run_with_bars(in_mem_ctx, bars)?;
    let results = BacktestResults::compute_in_memory(&equity, &state, config.starting_capital_sgd);
    Ok(SweepResult {
        params: params.clone(),
        results,
    })
}

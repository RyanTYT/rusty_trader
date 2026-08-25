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

use crate::backtester::setup::config::BacktestConfig;
use crate::backtester::methods::in_memory::replay::InMemoryReplay;
use crate::backtester::output::results::BacktestResults;
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
    name: &str,
    pool: sqlx::PgPool,
    config: &BacktestConfig,
    param_grid: &[HashMap<String, f64>],
    bars: Arc<Vec<HistoricalDataFullKeys>>,
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
            let task_bars = bars.clone();
            let task_name = name.to_string();
            s.spawn(move |_| {
                match run_one_backtest(
                    &task_name,
                    &task_pool,
                    config,
                    &task_params,
                    task_bars,
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
    name: &str,
    pool: &sqlx::PgPool,
    config: &BacktestConfig,
    params: &HashMap<String, f64>,
    bars: Arc<Vec<HistoricalDataFullKeys>>,
    handle: &tokio::runtime::Handle,
) -> Result<SweepResult, String> {
    let strategy = crate::strategy::construct_strategy(
        name,
        pool.clone(),
        handle.clone(),
        Some(params.clone()),
    )
    .ok_or_else(|| format!("Unknown strategy '{name}'"))?;
    // Build the light execution surface (clock/prices/consolidator) — shared
    // with BacktestContext::build, avoids the OrderStore::open() file contention.
    let light = crate::backtester::setup::context::build_light_context(pool, config);
    // Set the bar cache, warm up, trim to post-warm-up, run_with_bars, clear
    // — all in one helper (shared with the single route).
    let (equity, state) = InMemoryReplay.run_with_warm_up(strategy, bars, config, handle, &light)?;
    let results = BacktestResults::compute_in_memory(&equity, &state, config.starting_capital_sgd);
    Ok(SweepResult {
        params: params.clone(),
        results,
    })
}

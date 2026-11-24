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

use crate::database::crud::CRUDTrait;
use crate::database::db::DbDirect;
use crate::database::tables::DbTables;

use crate::backtester::methods::load_bars;
use crate::backtester::oracle::data_loader::refresh_continuous_aggregate;
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
        let bars_per_contract = load_bars(&config, &pool, 0)
            .await
            .map_err(|e| format!("load_bars check: {e}"))?;
        let has_empty_bars = bars_per_contract.iter().any(|bars| bars.is_empty());
        if !has_empty_bars {
            tracing::info!(
                "Data already loaded for [{}, {}] — refreshing the continuous aggregate.",
                *start,
                *end
            );
            refresh_continuous_aggregate(&pool, *start, *end).await;
        } else {
            // OFFLINE MODE: no IBKR/Alpaca. Data must be pre-loaded into
            // market_data.historical_data (e.g. from raw_bars.parquet).
            tracing::info!(
                "Data not loaded for [{}, {}] — OFFLINE MODE: skipping IBKR/Alpaca load (pre-load data into market_data.historical_data).",
                *start,
                *end
            );
        }
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
        let strategy =
            crate::strategy::construct_strategy(name_str, spec, pool.clone(), handle.clone())
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
        // println!(
        //     "{}",
        //     serde_json::to_string_pretty(&results).map_err(|e| format!("serialize: {e}"))?
        // );
    }
    Ok(())
}

/// §8/P1b: run ALL active strategies in ONE replay pass — per-strategy
/// in-memory accounts (transfer-seeded), per-strategy results files + a
/// rollup curve (`backtest_results_blended5_multi.json`). InMemory mode only
/// (the multi state model is an in-memory construct).
///
/// The strategies see one shared market surface (clock/prices/consolidator)
/// and their own accounts; fills attribute to the owning strategy by
/// construction (`PositionKey.strategy` = the account's strategy name).
pub async fn run_multi_route(pool: PgPool, config: BacktestConfig) -> Result<(), String> {
    // Db lane: ONE shared pass over the union of contracts driving every
    // active sleeve on real DB state (`run_multi_route_db`). The ≥2 guard
    // stays on the InMemory lane only — solo Db runs are legitimate (the
    // sleeve IS the fund; the mpsc pool handles 1–4 active sleeves).
    if config.mode == BacktestMode::Db {
        return run_multi_route_db(pool, config).await;
    }
    let handle = tokio::runtime::Handle::current();

    // 1. Load market data for the union of all active strategies' contracts
    //    (same check as run_single_route).
    if let BacktestPeriod::TimeRange { start, end } = &config.period {
        let bars_per_contract = load_bars(&config, &pool, 0)
            .await
            .map_err(|e| format!("load_bars check: {e}"))?;
        let has_empty_bars = bars_per_contract.iter().any(|bars| bars.is_empty());
        if !has_empty_bars {
            tracing::info!(
                "Data already loaded for [{}, {}] — refreshing the continuous aggregate.",
                *start,
                *end
            );
            refresh_continuous_aggregate(&pool, *start, *end).await;
        } else {
            tracing::info!(
                "Data not loaded for [{}, {}] — OFFLINE MODE: skipping IBKR/Alpaca load.",
                *start,
                *end
            );
        }
    }

    // 2. Active strategies in deterministic (name-sorted) order.
    let mut entries: Vec<(&String, &crate::strategy::StrategySpec)> =
        config.strategies.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut strategies: Vec<crate::strategy::strategy::StrategyEnum> = Vec::new();
    for (name, spec) in &entries {
        if !spec.active {
            tracing::info!("Strategy '{name}' inactive (active=false) — skipping");
            continue;
        }
        let strategy =
            crate::strategy::construct_strategy(name, spec, pool.clone(), handle.clone())
                .ok_or_else(|| format!("Unknown strategy '{name}'"))?;
        strategies.push(strategy);
    }
    if strategies.is_empty() {
        return Err("run_multi_route: no active strategies".into());
    }

    // 3. Equal-split seeding: each participant account seeds
    //    starting_capital_sgd / n (generic multi-run semantics — any mode,
    //    any strategy set); the residual (≈ 0 for an even split) stays
    //    un-traded with the rollup.
    let seed = config.starting_capital_sgd / strategies.len() as f64;
    let seeds = vec![seed; strategies.len()];

    // 4. Load bars once (the deepest warmup hint), then one blocking pass.
    let max_warm = strategies
        .iter()
        .map(|s| s.warmup_bars_required())
        .max()
        .unwrap_or(0);
    let bars = load_bars(&config, &pool, max_warm).await?;
    let bars_arc = Arc::new(crate::backtester::methods::transpose(bars));

    let config_clone = config.clone();
    let pool_clone = pool.clone();
    let handle_clone = handle.clone();
    let seeds_clone = seeds.clone();
    let out = tokio::task::spawn_blocking(move || {
        let light = build_light_context(&pool_clone, &config_clone);
        crate::backtester::methods::InMemoryReplay.run_multi_with_warm_up(
            strategies,
            seeds_clone,
            bars_arc,
            &config_clone,
            &handle_clone,
            &light,
        )
    })
    .await
    .map_err(|e| format!("multi replayer join: {e:?}"))??;

    // 5. Per-strategy results (each account's own starting capital) + rollup.
    for (i, name) in out.names.iter().enumerate() {
        let results = BacktestResults::compute_in_memory(
            &out.curves[i],
            &out.states[i],
            seeds[i],
            config.stock_bar_interval,
        );
        let output_path = format!("backtest_results_{name}.json");
        results.write_json(&output_path)?;
        tracing::info!("multi pass: {name} results → {output_path}");
    }

    let mut per_final = serde_json::Map::new();
    for (i, name) in out.names.iter().enumerate() {
        per_final.insert(
            name.clone(),
            serde_json::json!(out.curves[i].last_equity().unwrap_or(f64::NAN)),
        );
    }
    let rollup_json = serde_json::json!({
        "mode": "multi_rollup",
        "starting_capital_sgd": config.starting_capital_sgd,
        "residual_sgd": out.residual,
        "per_strategy_final_equity_sgd": per_final,
        "rollup_equity_curve": out
            .rollup
            .snapshots
            .iter()
            .map(|s| serde_json::json!({
                "time_ms": s.time.timestamp_millis(),
                "cash": s.cash,
                "positions_value": s.positions_value,
                "equity": s.equity,
            }))
            .collect::<Vec<_>>(),
    });
    std::fs::write(
        "backtest_results_blended5_multi.json",
        serde_json::to_string_pretty(&rollup_json).map_err(|e| format!("serialize rollup: {e}"))?,
    )
    .map_err(|e| format!("write rollup: {e}"))?;
    tracing::info!("multi pass: rollup → backtest_results_blended5_multi.json");
    Ok(())
}

/// Db-lane multi runner: ONE `HistoricalReplay`-shaped pass over the union
/// of contracts, driving every active sleeve on REAL DB state. NO
/// InMemoryState is installed; equity snapshots read the DB per slot.
/// Accepts 1–4 active sleeves (no ≥2 guard here — solo = the sleeve IS the
/// fund). NOT pin-comparable with the InMemory lane (BacktestBroker fills
/// materialize at the current bar's close).
async fn run_multi_route_db(pool: PgPool, config: BacktestConfig) -> Result<(), String> {
    let handle = tokio::runtime::Handle::current();

    // 1. Data check / aggregate refresh (same as the InMemory arm).
    if let BacktestPeriod::TimeRange { start, end } = &config.period {
        let bars_per_contract = load_bars(&config, &pool, 0)
            .await
            .map_err(|e| format!("load_bars check: {e}"))?;
        let has_empty_bars = bars_per_contract.iter().any(|bars| bars.is_empty());
        if !has_empty_bars {
            tracing::info!(
                "Data already loaded for [{}, {}] — refreshing the continuous aggregate.",
                *start,
                *end
            );
            refresh_continuous_aggregate(&pool, *start, *end).await;
        } else {
            tracing::info!(
                "Data not loaded for [{}, {}] — OFFLINE MODE: skipping IBKR/Alpaca load.",
                *start,
                *end
            );
        }
    }

    // 2. Active strategies in deterministic (name-sorted) order.
    let mut entries: Vec<(&String, &crate::strategy::StrategySpec)> =
        config.strategies.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut strategies: Vec<crate::strategy::strategy::StrategyEnum> = Vec::new();
    for (name, spec) in &entries {
        if !spec.active {
            tracing::info!("Strategy '{name}' inactive (active=false) — skipping");
            continue;
        }
        let strategy =
            crate::strategy::construct_strategy(name, spec, pool.clone(), handle.clone())
                .ok_or_else(|| format!("Unknown strategy '{name}'"))?;
        strategies.push(strategy);
    }
    if strategies.is_empty() {
        return Err("run_multi_route (Db): no active strategies".into());
    }

    // 3. Strategy rows (FK targets for transactions/positions).
    for s in &strategies {
        let name = s.get_name();
        let mut db = DbDirect::connect(&pool);
        let mut crud = db.strategy();
        if let Err(e) = crud
            .create_or_ignore(&crate::database::models::StrategyFullKeys {
                strategy: name,
                status: crate::database::models::Status::Active,
            })
            .await
        {
            tracing::error!("backtest: create strategy row failed: {e:?}");
        }
    }

    // 4. Load bars once (deepest warmup hint) + warm each strategy at its
    //    OWN window start (mirrors run_multi_with_warm_up's per-strategy
    //    bar_times). warm_up_data reads the DB (read_last_n) in Db mode.
    let max_warm = strategies
        .iter()
        .map(|s| s.warmup_bars_required())
        .max()
        .unwrap_or(0);
    let bars = load_bars(&config, &pool, max_warm).await?;
    let bars_arc = Arc::new(crate::backtester::methods::transpose(bars));
    #[cfg(feature = "backtest")]
    let bar_times: Result<Vec<_>, String> = match &config.period {
        // Warm boundary = the config's window start (the pins lane's
        // warm_cut): warm_up_data feeds every DB bar <= start and returns
        // start, so the post-warm trim replays from the first bar >= start.
        // Positioning by the warmup-bars hint instead let the inclusive
        // read_last_n swallow replay bars into the warm feed and shifted
        // every sleeve's indicator state (the pins/Db divergence root).
        BacktestPeriod::TimeRange { start, .. } => strategies.iter().map(|_| Ok(*start)).collect(),
        BacktestPeriod::NumBars(_) => {
            Err("run_multi_route (Db): NumBars period is not supported with warmup_bars".into())
        }
    };
    #[cfg(not(feature = "backtest"))]
    let bar_times: Result<Vec<chrono::DateTime<chrono::Utc>>, String> =
        if let BacktestPeriod::NumBars(_) = &config.period {
            Err("run_multi_route (Db): NumBars period is not supported with warmup_bars".into())
        } else {
            Ok(Vec::new())
        };
    let bar_times = bar_times?;

    let light = build_light_context(&pool, &config);
    let mut warm_ends = Vec::with_capacity(strategies.len());
    for (i, s) in strategies.iter_mut().enumerate() {
        #[cfg(feature = "backtest")]
        let end = s
            .warm_up_data(&light.consolidator, bar_times[i])
            .await
            .map_err(|e| format!("warm_up_data: {e}"))?;
        #[cfg(not(feature = "backtest"))]
        let end = s
            .warm_up_data(&light.consolidator)
            .await
            .map_err(|e| format!("warm_up_data: {e}"))?;
        warm_ends.push(end);
    }

    // 5. Trim to post-warm-up (no shared bar cache in Db mode — trim at the
    //    deepest warmup_end, the analogue of cache.max_end_time).
    let trim_t = warm_ends
        .iter()
        .copied()
        .max()
        .expect("strategies non-empty");
    let split = bars_arc
        .iter()
        .position(|row| {
            row.iter()
                .any(|b| b.as_ref().is_some_and(|bar| bar.get_time() > trim_t))
        })
        .unwrap_or(0);

    // 6. Seed each participant account (equal split of the fund capital).
    let seed = config.starting_capital_sgd / strategies.len() as f64;
    for s in &strategies {
        crate::backtester::setup::seed::seed_initial_capital(&pool, &s.get_name(), seed).await?;
    }

    // 7. One blocking pass. The fund pool is initialised INSIDE the closure
    //    (FUND_LEDGER is a thread-local; non-blended5 runs have no pool and
    //    skip the init).
    let config_clone = config.clone();
    let pool_clone = pool.clone();
    let handle_clone = handle.clone();
    let out = tokio::task::spawn_blocking(move || {
        // if let Some(p) =
        //     crate::strategy_wrappers::gates::impls::fund_gate::PoolState::current()
        // {
        //     #[cfg(feature = "backtest")]
        //     p.init(
        //         config_clone.starting_capital_sgd,
        //         config_clone.starting_capital_sgd, // equal-split seeds: Σ = capital
        //         BacktestMode::Db,
        //     );
        //     #[cfg(not(feature = "backtest"))]
        //     p.init(config_clone.starting_capital_sgd, config_clone.starting_capital_sgd);
        // }

        // F2 session-calendar: same pre-scan as bin/local_backtest — TIMESTAMPS
        // ONLY (pure calendar metadata) so sleeves classify rows in the
        // session's close window. Without this install the Db lane's
        // close-aware cap basis never engages and de-gross orders fire at the
        // next session's first slot instead of at the close (the pins-parity
        // divergence). Env-gated: a no-op unless BLENDED5_CLOSE_AWARE=1.
        crate::strategy::blended5_fund::session_calendar::install_from_slots(&bars_arc[split..]);
        crate::backtester::methods::HistoricalReplay.run_multi(
            crate::backtester::methods::historical::DbMultiContext {
                config: config_clone,
                pool: pool_clone,
                handle: handle_clone,
                clock: light.clock,
                prices: light.prices,
                consolidator: light.consolidator,
                strategies,
            },
            &bars_arc[split..],
        )
    })
    .await
    .map_err(|e| format!("multi replayer join: {e:?}"))??;

    // 8. Per-strategy results (each account's own starting capital) + rollup.
    for (i, name) in out.names.iter().enumerate() {
        let results =
            BacktestResults::compute(&pool, &out.curves[i], seed, config.stock_bar_interval)
                .await?;
        let output_path = format!("backtest_results_{name}.json");
        results.write_json(&output_path)?;
        tracing::info!("multi pass (Db): {name} results → {output_path}");
    }

    let mut per_final = serde_json::Map::new();
    for (i, name) in out.names.iter().enumerate() {
        per_final.insert(
            name.clone(),
            serde_json::json!(out.curves[i].last_equity().unwrap_or(f64::NAN)),
        );
    }
    let rollup_json = serde_json::json!({
        "mode": "multi_rollup_db",
        "starting_capital_sgd": config.starting_capital_sgd,
        "residual_sgd": out.residual,
        "per_strategy_final_equity_sgd": per_final,
        "rollup_equity_curve": out
            .rollup
            .snapshots
            .iter()
            .map(|s| serde_json::json!({
                "time_ms": s.time.timestamp_millis(),
                "cash": s.cash,
                "positions_value": s.positions_value,
                "equity": s.equity,
            }))
            .collect::<Vec<_>>(),
    });
    std::fs::write(
        "backtest_results_blended5_multi_db.json",
        serde_json::to_string_pretty(&rollup_json).map_err(|e| format!("serialize rollup: {e}"))?,
    )
    .map_err(|e| format!("write rollup: {e}"))?;
    tracing::info!("multi pass (Db): rollup → backtest_results_blended5_multi_db.json");
    Ok(())
}

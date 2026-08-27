//! `optimize` — the CLI entry point for the optimization layer.
//!
//! Reads `optimiser_params.json` (working dir) for the full config:
//! ```json
//! {
//!   "config": {
//!     "start": "2022-01-01T00:00:00Z",
//!     "end":   "2025-01-01T00:00:00Z",
//!     "oos_fraction": 0.3,
//!     "grid_steps": 5,
//!     "top_k": 10,
//!     "starting_capital_sgd": 100000.0,
//!     "stock": "QQQ", "primary_exchange": "NASDAQ", "currency": "USD"
//!   },
//!   "noise": {
//!     "daily_vol_threshold":  [0.02, 0.08],
//!     "ideal_qty_high_vol":   [40,   100],
//!     "ideal_qty_low_vol":    [60,   140]
//!   }
//! }
//! ```
//! The `config` section holds the time period + the optimization settings +
//! the contract. The strategy key (`"noise"`) holds the param thresholds
//! (each `[min, max]`, array size 2).
//!
//! The historical data for `[start, end]` is populated via the SAME IBKR
//! loader the backtester uses (`load_market_data`) — so the DB is guaranteed
//! to have the bars for the full period before the optimization runs.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use optimizer::{
    config::param_spec::ParamSpec,
    config::validation::{Holdout, ValidationScheme, WalkForward},
    functions::objective::{Dispersion, RobustSharpe},
    functions::optimizer::{GridOptimizer, Optimizer, RandomOptimizer},
    functions::robustness::RobustnessEvaluator,
    functions::tpe::TpeOptimizer,
    report::RobustnessReport,
    runner::run::{OptConfig, OptResult, WalkForwardResult, run_optimization, run_walk_forward},
};
use trading_app::backtester::oracle::data_loader::{
    load_market_data, refresh_continuous_aggregate,
};
use trading_app::backtester::{BacktestConfig, BacktestMode, BacktestPeriod};

/// The `config` section of `optimiser_params.json`. `start` + `end` are
/// required (RFC3339); the rest have defaults.
#[derive(Deserialize)]
struct OptimiserConfig {
    start: String,
    end: String,
    oos_fraction: Option<f64>,
    grid_steps: Option<usize>,
    top_k: Option<usize>,
    starting_capital_sgd: Option<f64>,
    stock: Option<String>,
    primary_exchange: Option<String>,
    currency: Option<String>,
    /// "grid" (default), "random", or "tpe".
    optimizer: Option<String>,
    /// The batch size per iteration (default 8 — the core count).
    batch_size: Option<usize>,
    /// The total evaluation budget for "random"/"tpe" (ignored by "grid" —
    /// the grid size is the budget). Default 100.
    n_evaluations: Option<usize>,
    /// The RNG seed for "random"/"tpe". Default 42.
    seed: Option<u64>,
    /// "holdout" (default) or "walk_forward".
    validation: Option<String>,
    /// Required if `validation = "walk_forward"`.
    walk_forward: Option<WalkForwardJson>,
}

/// The `walk_forward` section of `optimiser_params.json`.
#[derive(Deserialize)]
struct WalkForwardJson {
    in_sample_days: i64,
    out_sample_days: i64,
}

/// The per-strategy config: the tunable params (the search space — each is a
/// `ParamSpec` with `value_type`, `distribution`, `special`, `build_upon`).
/// Replaces the old `strategy_params` (the `[min, max]` grid) + `cache_params`
/// (the fixed lookbacks) — all tunable params are now here, swept uniformly
/// (the pure-strategy architecture has no precomputed cache, so lookbacks can
/// be swept freely).
#[derive(Deserialize)]
struct StrategyConfig {
    params: Vec<ParamSpec>,
}

/// The top-level `optimiser_params.json` — the `config` section + the
/// per-strategy configs (flattened).
#[derive(Deserialize)]
struct OptimiserFile {
    config: OptimiserConfig,
    #[serde(flatten)]
    strategies: HashMap<String, StrategyConfig>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_string())?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let handle = tokio::runtime::Handle::current();

    // 0. Run migrations (so the schema exists — idempotent).
    tracing::info!("Running SQLx migrations…");
    sqlx::migrate!("../trading-app/migrations")
        .run(&pool)
        .await
        .map_err(|e| format!("migrate: {e}"))?;

    // 1. Read optimiser_params.json.
    let params_file = std::path::Path::new("optimiser_params.json");
    let content = std::fs::read_to_string(params_file)
        .map_err(|e| format!("read optimiser_params.json: {e} — expected in the working dir"))?;
    let file: OptimiserFile =
        serde_json::from_str(&content).map_err(|e| format!("parse optimiser_params.json: {e}"))?;
    let cfg_section = file.config;

    let start = DateTime::parse_from_rfc3339(&cfg_section.start)
        .map_err(|e| format!("config.start: {e}"))?
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(&cfg_section.end)
        .map_err(|e| format!("config.end: {e}"))?
        .with_timezone(&Utc);

    // Env overrides JSON (for quick experiments); else JSON; else defaults.
    let oos_fraction = env_or("BACKTEST_OOS_FRACTION", cfg_section.oos_fraction, 0.3);
    let grid_steps = env_or("BACKTEST_GRID_STEPS", cfg_section.grid_steps, 5);
    let top_k = env_or("BACKTEST_TOP_K", cfg_section.top_k, 10);
    let capital = env_or(
        "BACKTEST_CAPITAL",
        cfg_section.starting_capital_sgd,
        100_000.0,
    );
    let stock = std::env::var("BACKTEST_STOCK")
        .ok()
        .or(cfg_section.stock)
        .unwrap_or_else(|| "QQQ".to_string());
    let pe = std::env::var("BACKTEST_PRIMARY_EXCHANGE")
        .ok()
        .or(cfg_section.primary_exchange)
        .unwrap_or_else(|| "NASDAQ".to_string());
    let currency = std::env::var("BACKTEST_CURRENCY")
        .ok()
        .or(cfg_section.currency)
        .unwrap_or_else(|| "USD".to_string());
    let optimizer_kind = std::env::var("BACKTEST_OPTIMIZER")
        .ok()
        .or(cfg_section.optimizer)
        .unwrap_or_else(|| "grid".to_string());
    let batch_size = env_or(
        "BACKTEST_BATCH_SIZE",
        cfg_section.batch_size,
        num_cpus::get(),
    );
    let n_evaluations = env_or("BACKTEST_N_EVALUATIONS", cfg_section.n_evaluations, 100);
    let seed = env_or("BACKTEST_SEED", cfg_section.seed, 42u64);

    // 2. Build the base backtest config (full period; split later).
    let base_config = BacktestConfig::new(capital)
        .mode(BacktestMode::InMemory)
        .period(BacktestPeriod::TimeRange { start, end })
        .stock(stock, pe, currency);

    // 3. Populate the DB for the full period — IF NECESSARY. If the bars are
    //    already loaded, skip the IBKR/Alpaca fetch + just refresh the
    //    continuous aggregate (daily_ohlcv / daily_volatility). This makes
    //    re-runs fast (no re-fetching) while ensuring the aggregates are fresh.
    let bars_exist = !trading_app::backtester::methods::load_bars(&base_config, &pool)
        .await
        .map_err(|e| format!("load_bars check: {e}"))?
        .is_empty();
    if bars_exist {
        tracing::info!(
            "Data already loaded for [{}, {}] — refreshing the continuous aggregate.",
            start,
            end
        );
        refresh_continuous_aggregate(&pool, start, end).await;
    } else {
        tracing::info!(
            "Data not loaded for [{}, {}] — populating via IBKR/Alpaca (with_gateway_retry internally).",
            start,
            end
        );
        load_market_data(
            &base_config.subscribed_contracts,
            start,
            end,
            &pool,
            &handle,
        )
        .await?;
    }

    // 4. Build the validation scheme + split the period (GLOBAL — shared across all strategies).
    let validation_kind = std::env::var("BACKTEST_VALIDATION")
        .ok()
        .or(cfg_section.validation.clone())
        .unwrap_or_else(|| "holdout".to_string());
    let (validation, in_sample, _out_sample) = match validation_kind.as_str() {
        "holdout" => {
            let (is, os) = split_period(&base_config.period, oos_fraction);
            tracing::info!(
                "Holdout: IS {:?}, OS {:?} (fraction {oos_fraction})",
                is,
                os
            );
            (
                ValidationScheme::Holdout(Holdout {
                    in_sample: is.clone(),
                    out_sample: os.clone(),
                }),
                is,
                os,
            )
        }
        "walk_forward" => {
            let wf_json = cfg_section.walk_forward.as_ref()
                .ok_or("config.validation = 'walk_forward' requires a 'walk_forward' section (in_sample_days, out_sample_days)")?;
            let wf = WalkForward {
                in_sample: chrono::Duration::days(wf_json.in_sample_days),
                out_sample: chrono::Duration::days(wf_json.out_sample_days),
            };
            tracing::info!(
                "Walk-forward: IS {}d, OS {}d",
                wf_json.in_sample_days,
                wf_json.out_sample_days
            );
            let dummy = base_config.period.clone();
            (ValidationScheme::WalkForward(wf), dummy.clone(), dummy)
        }
        other => {
            return Err(format!(
                "unknown validation '{other}' (expected holdout/walk_forward)"
            ));
        }
    };

    // 5. The objective + robustness (GLOBAL).
    let objective = Arc::new(RobustSharpe::new(1.0, Dispersion::Mad));
    let robustness = RobustnessEvaluator::default();

    // 6. For each strategy config in the JSON, build the OptConfig + run.
    for (name, strategy_config) in &file.strategies {
        let specs = strategy_config.params.clone();
        tracing::info!("Loaded {} param specs for '{name}'", specs.len());

        let opt_cfg = OptConfig {
            base_config: BacktestConfig {
                period: in_sample.clone(),
                ..base_config.clone()
            },
            specs: specs.clone(),
            objective: objective.clone(),
            robustness: robustness.clone(),
            batch_size,
            top_k,
            validation: validation.clone(),
            strategy_name: name.clone(),
        };

        let report_path = std::env::var("BACKTEST_REPORT_PATH")
            .unwrap_or_else(|_| format!("robustness_report_{name}.html"));
        println!("\n=== Strategy: {name} ===");
        match &validation {
            ValidationScheme::WalkForward(_) => {
                let factory: Box<dyn Fn() -> Box<dyn Optimizer> + Send + Sync> = {
                    let specs = specs.clone();
                    Box::new(move || -> Box<dyn Optimizer> {
                        match optimizer_kind.as_str() {
                            "grid" => Box::new(GridOptimizer::new(&specs, grid_steps)),
                            "random" => Box::new(RandomOptimizer::new(&specs, n_evaluations, seed)),
                            "tpe" => Box::new(TpeOptimizer::new(&specs, n_evaluations, seed)),
                            _ => unreachable!(),
                        }
                    })
                };
                let wf_result = run_walk_forward(pool.clone(), factory, opt_cfg, &handle).await?;
                report_walk_forward(&wf_result);
                let report = RobustnessReport::from_walk_forward(&wf_result);
                let html = report.to_html(&format!("Walk-forward robustness report — {name}"));
                std::fs::write(&report_path, html).map_err(|e| format!("write report: {e}"))?;
                println!("[{name}] Robustness report written to: {report_path}");
            }
            _ => {
                let optimizer: Box<dyn Optimizer> = match optimizer_kind.as_str() {
                    "grid" => Box::new(GridOptimizer::new(&specs, grid_steps)),
                    "random" => Box::new(RandomOptimizer::new(&specs, n_evaluations, seed)),
                    "tpe" => Box::new(TpeOptimizer::new(&specs, n_evaluations, seed)),
                    other => {
                        return Err(format!(
                            "unknown optimizer '{other}' (expected grid/random/tpe)"
                        ));
                    }
                };
                let result = run_optimization(pool.clone(), optimizer, opt_cfg, &handle).await?;
                report_holdout(&result);
                let report = RobustnessReport::from_holdout(&result.all);
                let html = report.to_html(&format!("Holdout robustness report — {name}"));
                std::fs::write(&report_path, html).map_err(|e| format!("write report: {e}"))?;
                println!("[{name}] Robustness report written to: {report_path}");
            }
        }
    }
    Ok(())
}

/// `env > json > default`.
fn env_or<T: std::str::FromStr>(env: &str, json: Option<T>, default: T) -> T
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    if let Ok(v) = std::env::var(env) {
        if let Ok(parsed) = v.parse::<T>() {
            return parsed;
        }
    }
    json.unwrap_or(default)
}

/// Split a `BacktestPeriod` into in-sample (first `1 - oos_fraction`) +
/// out-of-sample (last `oos_fraction`). For `TimeRange`, splits the time;
/// for `NumBars`, splits the bar count.
fn split_period(period: &BacktestPeriod, oos_fraction: f64) -> (BacktestPeriod, BacktestPeriod) {
    let f = oos_fraction.clamp(0.0, 1.0);
    match period {
        BacktestPeriod::TimeRange { start, end } => {
            let total_secs = (*end - *start).num_seconds();
            let in_secs = (total_secs as f64 * (1.0 - f)) as i64;
            let split = *start + chrono::Duration::seconds(in_secs);
            (
                BacktestPeriod::TimeRange {
                    start: *start,
                    end: split,
                },
                BacktestPeriod::TimeRange {
                    start: split,
                    end: *end,
                },
            )
        }
        BacktestPeriod::NumBars(n) => {
            let in_n = ((*n as f64) * (1.0 - f)) as usize;
            let oos_n = *n - in_n;
            (
                BacktestPeriod::NumBars(in_n),
                BacktestPeriod::NumBars(oos_n),
            )
        }
    }
}

/// Print the holdout optimization result.
fn report_holdout(result: &OptResult) {
    println!("=== Optimization result (Holdout) ===");
    println!("Best params: {:?}", result.best.params);
    println!("Best score (RobustSharpe): {:.4}", result.best.score);
    println!(
        "In-sample:   Sharpe={:.4}  PnL={:.2}  MaxDD={:.2}%  trades={}",
        result.best.results.sharpe,
        result.best.results.total_pnl,
        result.best.results.max_drawdown_pct,
        result.best.results.num_trades,
    );
    if let Some(oos) = &result.out_of_sample {
        println!(
            "Out-of-sample: Sharpe={:.4}  PnL={:.2}  MaxDD={:.2}%  trades={}",
            oos.sharpe, oos.total_pnl, oos.max_drawdown_pct, oos.num_trades,
        );
        let ratio = if result.best.results.sharpe.abs() > 1e-9 {
            oos.sharpe / result.best.results.sharpe
        } else {
            0.0
        };
        println!(
            "OOS/IS Sharpe ratio: {:.2} (≥0.5 suggests the edge generalizes)",
            ratio
        );
    } else {
        println!("(no out-of-sample validation — set oos_fraction > 0)");
    }
}

/// Print the walk-forward result — the per-window results + the aggregated OOS.
fn report_walk_forward(result: &WalkForwardResult) {
    println!(
        "=== Walk-forward result ({} windows) ===",
        result.aggregated_oos.num_windows
    );
    for (i, w) in result.per_window.iter().enumerate() {
        let is_sharpe = w.best.results.sharpe;
        let os_sharpe = w.oos.as_ref().map(|o| o.sharpe).unwrap_or(0.0);
        let ratio = if is_sharpe.abs() > 1e-9 {
            os_sharpe / is_sharpe
        } else {
            0.0
        };
        println!(
            "  W{}: IS Sharpe={:.4}  OS Sharpe={:.4}  ratio={:.2}  params={:?}",
            i + 1,
            is_sharpe,
            os_sharpe,
            ratio,
            w.best.params,
        );
    }
    let a = &result.aggregated_oos;
    println!("--- Aggregated OOS ---");
    println!("  Sharpe={:.4}  Sortino={:.4}", a.sharpe, a.sortino);
    println!(
        "  PnL={:.2}  Return={:.2}%  MaxDD={:.2}%",
        a.total_pnl, a.total_return_pct, a.max_drawdown_pct
    );
    println!(
        "  Final equity: {:.2} (from {:.2})",
        a.final_equity, a.starting_capital
    );
}

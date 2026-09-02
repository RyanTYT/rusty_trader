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

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use ibapi::contracts::Contract;
use serde::Deserialize;

use optimizer::{
    config::opt_config::OptConfig,
    config::param_spec::ParamSpec,
    config::validation::{Holdout, ValidationScheme, WalkForward},
    functions::objective::{Dispersion, RobustSharpe},
    functions::optimizer::{GridOptimizer, Optimizer, RandomOptimizer},
    functions::robustness::RobustnessEvaluator,
    functions::tpe::TpeOptimizer,
    report::RobustnessReport,
    runner::run::{OptResult, WalkForwardResult, run_optimization, run_walk_forward},
};
use trading_app::{
    backtester::oracle::data_loader::{load_market_data, refresh_continuous_aggregate},
    database::models::AssetType,
};
use trading_app::{
    backtester::{BacktestConfig, BacktestMode, BacktestPeriod},
    helpers::contract::build_contract_from_stock,
};

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

    // stock: Option<String>,
    // primary_exchange: Option<String>,
    // currency: Option<String>,
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

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "asset_type", rename_all = "lowercase")]
enum JsonContract {
    Stock(StockContract),
    Option(OptionContract),
}

impl Hash for JsonContract {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Stock(v) => {
                v.stock.hash(state);
                v.primary_exchange.hash(state);
                v.currency.hash(state);
            }
            Self::Option(v) => {
                v.stock.hash(state);
                v.primary_exchange.hash(state);
                v.currency.hash(state);
            }
        };
        match self {
            Self::Stock(_) => "stock".to_string().hash(state),
            Self::Option(contract) => {
                "option".to_string().hash(state);
                contract.option_type.hash(state);
                contract.expiry.hash(state);
                ordered_float::OrderedFloat(contract.strike).hash(state);
                contract.multiplier.hash(state);
            }
        }
    }
}

impl PartialEq for JsonContract {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stock(v), Self::Stock(v_other)) => {
                v.primary_exchange.as_str().trim() == v_other.primary_exchange.as_str().trim()
                    && v.stock == v_other.stock
                    && v.currency == v_other.currency
            }
            (Self::Option(v), Self::Option(v_other)) => {
                v.primary_exchange.as_str().trim() == v_other.primary_exchange.as_str().trim()
                    && v.stock == v_other.stock
                    && v.currency == v_other.currency
                    && v.option_type == v_other.option_type
                    && v.expiry == v_other.expiry
                    && ordered_float::OrderedFloat(v.strike)
                        == ordered_float::OrderedFloat(v_other.strike)
                    && v.multiplier == v_other.multiplier
            }
            _ => false,
        }
    }
}

impl Eq for JsonContract {}

#[derive(Debug, Deserialize, Clone)]
struct StockContract {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
}

#[derive(Debug, Deserialize, Clone)]
struct OptionContract {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,

    // Additional fields for Option
    pub expiry: String,
    pub strike: f64,
    pub multiplier: u32,
    pub option_type: String, // Or create an enum for "call" | "put"
}

/// The per-strategy config: the tunable params (the search space — each is a
/// `ParamSpec` with `value_type`, `distribution`, `special`, `build_upon`).
/// Replaces the old `strategy_params` (the `[min, max]` grid) + `cache_params`
/// (the fixed lookbacks) — all tunable params are now here, swept uniformly
/// (the pure-strategy architecture has no precomputed cache, so lookbacks can
/// be swept freely).
#[derive(Deserialize, Debug)]
struct StrategyConfig {
    activated: bool,
    contracts: Vec<JsonContract>,
    params: Vec<ParamSpec>,
}

/// The top-level `optimiser_params.json` — the `config` section + the
/// per-strategy configs (flattened).
#[derive(Deserialize)]
struct OptimiserFile {
    config: OptimiserConfig,
    #[serde(flatten)]
    strategies: std::collections::HashMap<String, StrategyConfig>,
}

fn parse_into_contract(contract_data: &JsonContract) -> Contract {
    match contract_data {
        JsonContract::Stock(stock_contract) => build_contract_from_stock(
            &stock_contract.stock,
            &stock_contract.primary_exchange,
            &stock_contract.currency,
        ),
        JsonContract::Option(option_contract) => Contract::option(
            &option_contract.stock,
            &option_contract.expiry,
            option_contract.strike,
            &option_contract.option_type,
        ),
        // .primary_exchange(option_contract.primary_exchange)
        // .in_currency(option_contract.currency)
    }
    // let asset_type = AssetType::from_string(
    //     contract_data
    //         .get("asset_type")
    //         .expect("User must define asset type of contract"),
    // );
    // let stock = contract_data
    //     .get("stock")
    //     .expect("User must define stock of contract");
    // let primary_exchange = contract_data
    //     .get("primary_exchange")
    //     .expect("User must define primary_exchange of contract");
    // let currency = contract_data
    //     .get("currency")
    //     .expect("User must define currency of contract");
    // match asset_type {
    //     AssetType::Option => {
    //         let expiration = contract_data
    //             .get("expiry")
    //             .expect("User must define expiry of option contract");
    //         let strike = contract_data
    //             .get("strike")
    //             .expect("User must define strike of option contract");
    //         let right = contract_data
    //             .get("right")
    //             .expect("User must define right of option contract");
    //         Contract::option(stock, expiration, strike, right)
    //     }
    //     AssetType::Stock => Contract::stock(stock)
    //         .primary(primary_exchange)
    //         .in_currency(currency)
    //         .build(),
    //     AssetType::ForexPair => Contract {
    //         symbol: stock.into(),
    //         security_type: ibapi::prelude::SecurityType::ForexPair,
    //         exchange: "IDEALPRO".into(),
    //         currency: currency.into(),
    //         ..Default::default()
    //     },
    //     AssetType::Future => {
    //         let exchange = contract_data
    //             .get("exchange")
    //             .expect("User must define exchange of contract");
    //         Contract::continuous_futures(stock)
    //             .on_exchange(exchange)
    //             .in_currency(currency)
    //             .build()
    //     }
    //     _ => {
    //         panic!("Define in local contract style!!!");
    //     }
    // }
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
        .acquire_timeout(std::time::Duration::from_secs(90))
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
    let oos_fraction = cfg_section
        .oos_fraction
        .expect("User must define OOS fraction");
    let grid_steps = cfg_section.grid_steps.expect("User must define grid steps");
    let top_k = cfg_section.top_k.expect("User must define top k");
    let capital = cfg_section
        .starting_capital_sgd
        .expect("User must define top k");
    let optimizer_kind = cfg_section
        .optimizer
        .expect("User must define optimizer type: tpe/grid/...");
    let batch_size = cfg_section.batch_size.expect("User must define batch size");
    let n_evaluations = cfg_section
        .n_evaluations
        .expect("User must define N evalutations");
    let seed = cfg_section
        .seed
        .expect("User must define backtesting seed value");

    // let stock = std::env::var("BACKTEST_STOCK")
    //     .ok()
    //     .or(cfg_section.stock)
    //     .unwrap_or_else(|| "QQQ".to_string());
    // let pe = std::env::var("BACKTEST_PRIMARY_EXCHANGE")
    //     .ok()
    //     .or(cfg_section.primary_exchange)
    //     .unwrap_or_else(|| "NASDAQ".to_string());
    // let currency = std::env::var("BACKTEST_CURRENCY")
    //     .ok()
    //     .or(cfg_section.currency)
    //     .unwrap_or_else(|| "USD".to_string());

    // 2. Build the base backtest config (full period; split later).
    let mut all_contracts = HashSet::new();
    for strategy_config in file.strategies.values() {
        if strategy_config.activated {
            for contract in strategy_config.contracts.clone() {
                all_contracts.insert(contract);
            }
        }
    }
    let base_config = BacktestConfig::new(capital)
        .mode(BacktestMode::InMemory)
        .period(BacktestPeriod::TimeRange { start, end })
        .contracts(
            all_contracts
                .iter()
                .map(|json_contract| parse_into_contract(json_contract))
                .collect::<Vec<Contract>>(),
        );

    // 3. Populate the DB for the full period — IF NECESSARY. If the bars are
    //    already loaded, skip the IBKR/Alpaca fetch + just refresh the
    //    continuous aggregate (daily_ohlcv / daily_volatility). This makes
    //    re-runs fast (no re-fetching) while ensuring the aggregates are fresh.
    let bars_per_contract = trading_app::backtester::methods::load_bars(&base_config, &pool, 0)
        .await
        .map_err(|e| format!("load_bars check: {e}"))?;
    let has_empty_bars = bars_per_contract.iter().any(|bars| bars.is_empty());
    if !has_empty_bars {
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
    let objective = Arc::new(RobustSharpe::new(1.0, Dispersion::Mad, 3.0, 1.0));
    let robustness = RobustnessEvaluator::default();

    // 6. For each strategy config in the JSON, build the OptConfig + run.
    for (name, strategy_config) in &file.strategies {
        if !strategy_config.activated {
            continue;
        }
        println!("{name}: {:?}", strategy_config);
        let specs = strategy_config.params.clone();
        tracing::info!("Loaded {} param specs for '{name}'", specs.len());

        let base_config = BacktestConfig::new(capital)
            .mode(BacktestMode::InMemory)
            .period(BacktestPeriod::TimeRange { start, end })
            .contracts(
                strategy_config
                    .contracts
                    .iter()
                    .map(|contract| parse_into_contract(contract))
                    .collect::<Vec<Contract>>(),
            );

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
                    let optimizer_kind_str = optimizer_kind.clone();
                    let specs = specs.clone();
                    Box::new(move || -> Box<dyn Optimizer> {
                        match optimizer_kind_str.as_str() {
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
                let report = RobustnessReport::from_holdout(&result);
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

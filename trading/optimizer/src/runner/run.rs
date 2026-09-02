//! The optimization loop — sequentially pulls batches from the optimizer,
//! runs them in parallel (rayon), scores each, + feeds the results back.
//! After the optimizer is exhausted, picks the top-K, evaluates the
//! robustness neighborhood, + returns the best by `RobustSharpe`.
//!
//! # Flow
//! 1. Load bars (in-sample) — wrapped in `Arc` so the parallel sweep shares
//!    them (no pre-computed cache (the pure-strategy architecture warms up per-backtest from the bar cache).
//! 2. Sequential loop: `optimizer.next_batch(history, batch_size)` → run the
//!    batch in parallel (rayon) → score each by phase-1 (own metric, no
//!    neighborhood) → append to `history` → repeat until exhausted (`None`).
//! 3. Pick the top-K from `history` (by phase-1 score).
//! 4. For each top-K, run the robustness neighborhood (parallel) → re-score
//!    by `RobustSharpe` (phase-2: mean − α·MAD of the neighborhood).
//! 5. Pick the best (phase-2 score) + return.
//! 6. (If `Holdout`) validate the best on the out-of-sample period.

use chrono::Duration;
use trading_app::strategy::ContractEntry;
use std::sync::Arc;

use rayon::prelude::*;
use sqlx::PgPool;
use tokio::runtime::Handle;

use trading_app::backtester::methods::{load_bars, transpose};
use trading_app::backtester::sweep::{SweepResult, run_one_backtest};
use trading_app::backtester::{BacktestConfig, BacktestPeriod, BacktestResults};
use trading_app::strategy::strategy::StrategyExecutor;
use trading_app::helpers::contract::get_local_symbol;

use crate::config::opt_config::OptConfig;
use crate::config::param_spec::ParamSpec;
use crate::config::validation::{Holdout, ValidationScheme, WalkForward};
use crate::functions::objective::Objective;
use crate::functions::optimizer::{EvalResult, Optimizer};
use crate::functions::robustness::RobustnessEvaluator;

/// The optimization result: the best candidate + all evaluated candidates +
/// the out-of-sample validation (if `Holdout`).
#[derive(Debug)]
pub struct OptResult {
    /// The best candidate (by the objective's robust score).
    pub best: EvalResult,
    /// All evaluated candidates (phase-1 scored; the top-K also have
    /// phase-2 robust scores + neighborhoods).
    pub all: Vec<EvalResult>,
    /// The out-of-sample results for `best` (if `ValidationScheme::Holdout`).
    pub out_of_sample: Option<BacktestResults>,
}

/// Run the optimization loop. Async (loads bars); the parallel sweep is
/// CPU-bound (blocks the async thread).
pub async fn run_optimization(
    pool: PgPool,
    mut optimizer: Box<dyn Optimizer>,
    cfg: OptConfig,
    handle: &Handle,
) -> Result<OptResult, String> {
    // 1. Load bars (in-sample) — Arc so the parallel sweep shares them. Use
    // the MAX warmup_bars_required() across the param space: construct the
    // strategy with each param at its spec's max, then read
    // warmup_bars_required(). Every candidate's warmup is then served by the
    // shared prefix (the warmup reads the last N before bar_time regardless
    // of N vs prefix count).
    let max_params: std::collections::HashMap<String, f64> = cfg
        .specs
        .iter()
        .map(|s| (s.name.clone(), s.range().1))
        .collect();
    let max_spec = trading_app::strategy::StrategySpec {
        active: true,
        params: max_params,
        contracts: cfg.base_config.subscribed_contracts.iter().map(|contract| ContractEntry {
            stock: get_local_symbol(contract),
            primary_exchange: contract.primary_exchange.to_string(),
            currency: contract.currency.to_string()
        }).collect(),
        benchmark: None,
    };
    let max_strategy = trading_app::strategy::construct_strategy(
        &cfg.strategy_name,
        &max_spec,
        pool.clone(),
        handle.clone(),
    )
    .ok_or_else(|| format!("Unknown strategy '{}' for warmup sizing", cfg.strategy_name))?;
    let warmup_bars = max_strategy.warmup_bars_required();
    let bars = Arc::new(transpose(
        load_bars(&cfg.base_config, &pool, warmup_bars).await?,
    ));

    // 2. Sequential loop: pull batches, run in parallel, score by phase-1.
    let mut history: Vec<EvalResult> = Vec::new();
    let pool_ref = &pool;
    let cfg_ref = &cfg.base_config;
    let bars_ref = &bars;
    let handle_ref = handle;
    let strategy_name = &cfg.strategy_name;
    let objective = cfg.objective.as_ref();
    while let Some(batch) = optimizer.next_batch(&history, cfg.batch_size) {
        let results: Vec<SweepResult> = batch
            .par_iter()
            .map(|params| {
                run_one_backtest(
                    strategy_name,
                    pool_ref,
                    cfg_ref,
                    params,
                    bars_ref.clone(),
                    handle_ref,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        for s in results {
            let score = objective.score(&s.results, &[]);
            history.push(EvalResult {
                params: s.params,
                results: s.results,
                neighborhood: Vec::new(),
                score,
            });
        }
    }
    tracing::info!("Optimization: {} candidates evaluated", history.len());

    // 3. Pick the top-K (by phase-1 score).
    let mut scored = history.clone();
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let top_k: Vec<EvalResult> = scored.into_iter().take(cfg.top_k).collect();

    // 4. Robustness neighborhood for the top-K (phase-2: RobustSharpe).
    let mut eval_results: Vec<EvalResult> = Vec::new();
    for mut ev in top_k {
        let neighbors = cfg.robustness.neighborhood(&ev.params, &cfg.specs);
        let neighborhood: Vec<BacktestResults> = neighbors
            .par_iter()
            .filter_map(|p| {
                run_one_backtest(
                    strategy_name,
                    pool_ref,
                    cfg_ref,
                    p,
                    bars_ref.clone(),
                    handle_ref,
                )
                .ok()
                .map(|s| s.results)
            })
            .collect();
        ev.score = objective.score(&ev.results, &neighborhood);
        ev.neighborhood = neighborhood;
        eval_results.push(ev);
    }

    // 5. Pick the best (phase-2 robust score).
    eval_results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let best = eval_results
        .first()
        .cloned()
        .ok_or("optimization produced no results")?;

    tracing::info!("Running OOS Validation");
    // 6. Out-of-sample validation (if Holdout).
    let out_of_sample = match &cfg.validation {
        ValidationScheme::None => None,
        ValidationScheme::Holdout(h) => {
            let mut oos_config = cfg.base_config.clone();
            oos_config.period = h.out_sample.clone();
            // OOS: load with the best params' EXACT warmup need (the cache
            // is loaded once for the best params, not shared across
            // candidates) — so the warmup prefix matches the read limit
            // exactly → clean OOS (the walk-forward 0-trade fix).
            let best_spec = trading_app::strategy::StrategySpec {
                active: true,
                params: best.params.clone(),
                contracts: cfg.base_config.subscribed_contracts.iter().map(|contract| ContractEntry {
                    stock: get_local_symbol(contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string()
                }).collect(),
                benchmark: None,
            };
            let best_strategy = trading_app::strategy::construct_strategy(
                &cfg.strategy_name,
                &best_spec,
                pool.clone(),
                handle.clone(),
            )
            .ok_or_else(|| format!("Unknown strategy '{}' for OOS warmup", cfg.strategy_name))?;
            let oos_bars = Arc::new(
                transpose(
                    load_bars(&oos_config, &pool, best_strategy.warmup_bars_required()).await?,
                )
            );
            let mut oos_results_res = None;
            rayon::scope(|s| {
                s.spawn(|_| {
                    oos_results_res = Some(run_one_backtest(
                        &cfg.strategy_name,
                        &pool,
                        &oos_config,
                        &best.params,
                        oos_bars,
                        handle,
                    ))
                });
            });
            let oos_results = oos_results_res.unwrap()?;
            Some(oos_results.results)
        }
        // Walk-forward is handled by `run_walk_forward` (which calls
        // `run_optimization` per-window with a Holdout).
        ValidationScheme::WalkForward(_) => None,
    };

    Ok(OptResult {
        best,
        all: history,
        out_of_sample,
    })
}

// ─── Walk-forward (Phase 3) ───────────────────────────────────────────────

/// One window's walk-forward result: the best params (from the in-sample
/// optimization) + the out-of-sample validation.
#[derive(Debug)]
pub struct WindowResult {
    pub is_period: BacktestPeriod,
    pub os_period: BacktestPeriod,
    pub best: EvalResult,
    pub oos: Option<BacktestResults>,
}

/// The aggregated out-of-sample metrics (across all walk-forward windows).
/// Computed from the concatenated per-bar OOS returns (compounded).
#[derive(Debug)]
pub struct AggregatedMetrics {
    pub starting_capital: f64,
    pub final_equity: f64,
    pub total_pnl: f64,
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub bar_interval_minutes: i64,
    pub bars_per_year: f64,
    pub num_windows: usize,
}

/// The walk-forward result: the per-window results + the aggregated OOS.
#[derive(Debug)]
pub struct WalkForwardResult {
    pub per_window: Vec<WindowResult>,
    pub aggregated_oos: AggregatedMetrics,
}

/// Run the walk-forward validation. For each window: optimize on the
/// in-sample, validate the best params on the out-of-sample. The OOS windows
/// tile the period contiguously → the concatenated OOS returns are the "true"
/// performance. Each window gets a FRESH optimizer (via `optimizer_factory`).
///
/// Requires `cfg.validation = ValidationScheme::WalkForward(wf)`.
pub async fn run_walk_forward(
    pool: PgPool,
    optimizer_factory: Box<dyn Fn() -> Box<dyn Optimizer> + Send + Sync>,
    cfg: OptConfig,
    handle: &Handle,
) -> Result<WalkForwardResult, String> {
    let wf = match &cfg.validation {
        ValidationScheme::WalkForward(wf) => wf.clone(),
        _ => return Err("run_walk_forward requires ValidationScheme::WalkForward".into()),
    };
    let windows = wf.windows(&cfg.base_config.period);
    tracing::info!("Walk-forward: {} windows", windows.len());
    if windows.is_empty() {
        return Err(
            "walk-forward produced no windows (period too short for the IS+OS sizes)".into(),
        );
    }

    let mut per_window: Vec<WindowResult> = Vec::new();
    let mut oos_returns: Vec<f64> = Vec::new();
    for (is_period, os_period) in windows {
        tracing::info!(
            "Walk-forward window: IS {:?}, OS {:?}",
            is_period,
            os_period
        );
        let optimizer = optimizer_factory();
        let window_cfg = OptConfig {
            base_config: BacktestConfig {
                period: is_period.clone(),
                ..cfg.base_config.clone()
            },
            specs: cfg.specs.clone(),
            objective: cfg.objective.clone(),
            robustness: cfg.robustness.clone(),
            batch_size: cfg.batch_size,
            top_k: cfg.top_k,
            validation: ValidationScheme::Holdout(Holdout {
                in_sample: is_period.clone(),
                out_sample: os_period.clone(),
            }),
            strategy_name: cfg.strategy_name.clone(),
        };
        let result = run_optimization(pool.clone(), optimizer, window_cfg, handle).await?;
        // Collect the OOS per-bar returns (scale-invariant → can be
        // concatenated across windows without equity rescaling).
        if let Some(oos) = &result.out_of_sample {
            for w in oos.equity_curve.windows(2) {
                let prev = w[0].equity;
                let cur = w[1].equity;
                if prev.abs() > 1e-9 {
                    oos_returns.push((cur - prev) / prev);
                }
            }
        }
        per_window.push(WindowResult {
            is_period,
            os_period,
            best: result.best,
            oos: result.out_of_sample,
        });
    }

    let starting_capital = cfg.base_config.starting_capital_sgd;
    let aggregated = compute_aggregated_oos(
        &oos_returns,
        starting_capital,
        per_window.len(),
        cfg.base_config.stock_bar_interval,
    );
    Ok(WalkForwardResult {
        per_window,
        aggregated_oos: aggregated,
    })
}

/// Aggregate the OOS metrics from the concatenated per-bar returns
/// (compounded). The returns are scale-invariant → concatenating across
/// windows (without equity rescaling) gives the true compounded performance.
fn compute_aggregated_oos(
    returns: &[f64],
    starting_capital: f64,
    num_windows: usize,
    bar_interval: Duration,
) -> AggregatedMetrics {
    let mut equity = starting_capital;
    let mut peak = starting_capital;
    let mut max_dd = 0.0_f64;
    for &r in returns {
        equity *= 1.0 + r;
        if equity > peak {
            peak = equity;
        }
        if peak > 0.0 {
            let dd = (peak - equity) / peak * 100.0;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    let final_equity = equity;
    let total_pnl = final_equity - starting_capital;
    let total_return_pct = if starting_capital != 0.0 {
        total_pnl / starting_capital * 100.0
    } else {
        0.0
    };
    let (sharpe_per_bar, sortino_per_bar) = sharpe_sortino_from_returns(returns);
    // Annualize: same logic as BacktestResults::build
    let bar_interval_minutes = bar_interval.num_minutes().max(1);
    let bars_per_year = if bar_interval_minutes >= 1440 {
        252.0
    } else {
        (390.0 * 252.0) / bar_interval_minutes as f64
    };
    let annualization_factor = bars_per_year.sqrt();
    let sharpe = sharpe_per_bar * annualization_factor;
    let sortino = sortino_per_bar * annualization_factor;
    AggregatedMetrics {
        starting_capital,
        final_equity,
        total_pnl,
        total_return_pct,
        max_drawdown_pct: max_dd,
        sharpe,
        sortino,
        bar_interval_minutes,
        bars_per_year,
        num_windows,
    }
}

/// Sharpe + Sortino (per-bar, NOT annualized) from a slice of per-bar returns.
/// Annualization is applied by the caller ().
fn sharpe_sortino_from_returns(returns: &[f64]) -> (f64, f64) {
    if returns.len() < 2 {
        return (0.0, 0.0);
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    let std = var.sqrt();
    let downside_var: f64 = returns
        .iter()
        .filter(|r| **r < 0.0)
        .map(|r| r.powi(2))
        .sum::<f64>()
        / n;
    let downside_std = downside_var.sqrt();
    let sharpe = if std > 0.0 { mean / std } else { 0.0 };
    let sortino = if downside_std > 0.0 {
        mean / downside_std
    } else {
        0.0
    };
    (sharpe, sortino)
}

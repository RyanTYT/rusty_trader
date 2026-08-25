//! The optimization config.

use std::sync::Arc;

use trading_app::backtester::BacktestConfig;

use crate::config::param_spec::ParamSpec;
use crate::config::validation::ValidationScheme;
use crate::functions::objective::Objective;
use crate::functions::robustness::RobustnessEvaluator;

/// The optimization config.
pub struct OptConfig {
    /// The base backtest config (the in-sample period, capital, mode, contracts).
    pub base_config: BacktestConfig,
    /// The param specs (the search space — all tunable params, including the
    /// lookbacks; no more separate cache_params).
    pub specs: Vec<ParamSpec>,
    /// The objective (e.g. `RobustSharpe`).
    pub objective: Arc<dyn Objective>,
    /// The robustness evaluator (perturbation neighborhood).
    pub robustness: RobustnessEvaluator,
    /// The batch size per iteration (e.g. 8 — the core count).
    pub batch_size: usize,
    /// How many top candidates (by phase-1 score) to evaluate the robustness
    /// neighborhood for. E.g. 10.
    pub top_k: usize,
    /// The validation scheme (in-sample/out-of-sample split).
    pub validation: ValidationScheme,
    /// The strategy name — the runner constructs the strategy per-backtest
    /// via `construct_strategy(name, …, params)` (no base_strategy /
    /// cache_queries needed — the pure-strategy architecture has no
    /// precomputed cache).
    pub strategy_name: String,
}

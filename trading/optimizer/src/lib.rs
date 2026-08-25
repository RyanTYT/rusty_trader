//! Optimizer — the research/optimization layer for the backtester.
//!
//! Organized into:
//! - [`config`] — the config types (`OptConfig`, `ParamSpec`, `ValidationScheme`).
//! - [`functions`] — the optimization algorithms (grid/random/TPE) + the
//!   scoring (`Objective`/`RobustSharpe` + `RobustnessEvaluator`).
//! - [`runner`] — the `run_optimization`/`run_walk_forward` loop (relies on
//!   the trading-app backtester's `run_one_backtest`).
//! - [`report`] — the analysis layer (the Pareto front + the stability + the
//!   HTML report).

pub mod config;
pub mod functions;
pub mod report;
pub mod runner;

pub use config::param_spec::{Distribution, NormalMode, ParamSpec, SpecialParam, ValueType};
pub use config::validation::{Holdout, ValidationScheme, WalkForward};
pub use functions::objective::{Objective, RobustSharpe};
pub use functions::optimizer::{GridOptimizer, Optimizer, RandomOptimizer};
pub use functions::robustness::{RobustScore, RobustnessEvaluator};
pub use functions::tpe::TpeOptimizer;
pub use config::opt_config::OptConfig;
pub use runner::run::{run_optimization, run_walk_forward, AggregatedMetrics, OptResult, WalkForwardResult, WindowResult};
pub use report::RobustnessReport;

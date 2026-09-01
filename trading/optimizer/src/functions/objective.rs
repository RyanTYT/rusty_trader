//! The objective — what the optimizer maximizes.
//!
//! Per the design discussion:
//! - **Weighted combos** (`0.7·Sharpe − 0.3·MaxDD`) are fragile — the weights
//!   are arbitrary + the "balance" often doesn't exist cleanly.
//! - **Pareto fronts** are useful for *exploration* (seeing the trade-offs)
//!   but don't *select* — still subjective.
//! - **`RobustSharpe`** = `mean(neighborhood Sharpes) − α·dispersion(neighborhood
//!   Sharpes)` — a SINGLE objective with ONE knob (α). It's the empirical
//!   lower-confidence bound: a conservative estimate of the true performance.
//!   It directly encodes "real edge = stable plateau" — a spike (overfit) has
//!   high std → penalized; a plateau (real edge) has low std → not penalized.
//!
//! The dispersion metric is the **median absolute deviation (MAD)** by default
//! (robust to outliers in the small neighborhood; std is noisy for n=5-10).
//! `Std` is available as an alternative.

use trading_app::backtester::BacktestResults;

/// The objective: score a candidate's results + its robustness. Higher = better.
pub trait Objective: Send + Sync {
    /// `results` = the candidate's own results; `neighborhood` = the results of
    /// the perturbed neighbors (excluding the candidate). Returns the score.
    fn score(&self, results: &BacktestResults, neighborhood: &[BacktestResults]) -> f64;
}

/// The dispersion metric for the neighborhood.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispersion {
    /// Standard deviation (sensitive to outliers; noisy for small n).
    Std,
    /// Median absolute deviation (robust to outliers; default for small n).
    Mad,
}

impl Dispersion {
    pub fn compute(self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        match self {
            Dispersion::Std => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let var =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                var.sqrt()
            }
            Dispersion::Mad => {
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let median = if sorted.len() % 2 == 0 {
                    (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
                } else {
                    sorted[sorted.len() / 2]
                };
                let abs_devs: Vec<f64> = values.iter().map(|v| (v - median).abs()).collect();
                let mut abs_devs_sorted = abs_devs;
                abs_devs_sorted
                    .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                if abs_devs_sorted.len() % 2 == 0 {
                    (abs_devs_sorted[abs_devs_sorted.len() / 2 - 1]
                        + abs_devs_sorted[abs_devs_sorted.len() / 2])
                        / 2.0
                } else {
                    abs_devs_sorted[abs_devs_sorted.len() / 2]
                }
            }
        }
    }
}

/// `RobustSharpe`: `mean(neighborhood Sharpes) − α·dispersion(neighborhood
/// Sharpes)`. The empirical lower-confidence bound — prefers stable plateaus.
///
/// `alpha` controls the robustness penalty (default 1.0 — one dispersion unit;
/// tune up for more conservatism). `dispersion` defaults to `Mad` (robust for
/// small neighborhoods).
///
/// If the neighborhood is empty (robustness not evaluated), falls back to the
/// candidate's own Sharpe (no penalty).
#[derive(Debug, Clone)]
pub struct RobustSharpe {
    pub alpha: f64,
    pub dispersion: Dispersion,
    pub min_trades_per_month: f64,
    pub trade_penalty: f64,
}

impl Default for RobustSharpe {
    fn default() -> Self {
        Self {
            alpha: 1.0,
            dispersion: Dispersion::Mad,
            min_trades_per_month: 0.0,
            trade_penalty: 0.0,
        }
    }
}

impl RobustSharpe {
    pub fn new(
        alpha: f64,
        dispersion: Dispersion,
        min_trades_per_month: f64,
        trade_penalty: f64,
    ) -> Self {
        Self {
            alpha,
            dispersion,
            min_trades_per_month,
            trade_penalty,
        }
    }
}

impl Objective for RobustSharpe {
    fn score(&self, results: &BacktestResults, neighborhood: &[BacktestResults]) -> f64 {
        let own_sharpe = results.sharpe;
        let base = if neighborhood.is_empty() {
            return own_sharpe;
        } else {
            let mut sharpes: Vec<f64> = neighborhood.iter().map(|r| r.sharpe).collect();
            sharpes.push(own_sharpe);
            let mean = sharpes.iter().sum::<f64>() / sharpes.len() as f64;
            let disp = self.dispersion.compute(&sharpes);
            mean - self.alpha * disp
        };

        if self.trade_penalty > 0.0 && self.min_trades_per_month > 0.0 {
            let num_months = (results.equity_curve.len() as f64 / results.bars_per_year * 12.0).max(1.0);
            let trades_per_month = results.num_trades as f64 / num_months;
            let shortfall = (self.min_trades_per_month - trades_per_month).max(0.0);
            base - self.trade_penalty * shortfall
        } else {
            base
        }
    }
}

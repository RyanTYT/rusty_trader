//! Robustness evaluation — the "real edge" criterion.
//!
//! For each candidate param set, perturb each param by one step (time → next
//! natural scale; continuous → ±ε % of the range), run the neighborhood, +
//! score the stability. A real edge is a **plateau** (high mean, low
//! dispersion); an overfit edge is a **spike** (high at the candidate, low at
//! the neighbors → high dispersion → penalized by `RobustSharpe`).
//!
//! The neighborhood also doubles as the robustness FILTER: candidates with
//! dispersion above a threshold are likely overfit (rejected).

use std::collections::HashMap;

use trading_app::backtester::BacktestResults;

use crate::config::param_spec::{Distribution, ParamSpec};

/// The robustness score for one candidate.
#[derive(Debug, Clone)]
pub struct RobustScore {
    /// The candidate's own results.
    pub candidate: BacktestResults,
    /// The neighbors' results (the perturbations).
    pub neighborhood: Vec<BacktestResults>,
}

impl RobustScore {
    /// The neighborhood's results (excluding the candidate) — for the
    /// `Objective::score` call.
    pub fn neighborhood_results(&self) -> Vec<&BacktestResults> {
        self.neighborhood.iter().collect()
    }
}

/// Generates + evaluates the perturbation neighborhood for a candidate.
#[derive(Debug, Clone)]
pub struct RobustnessEvaluator {
    /// The perturbation step for continuous params (fraction of the range).
    /// Default 0.05 (±5% of the range).
    pub epsilon: f64,
}

impl Default for RobustnessEvaluator {
    fn default() -> Self {
        Self { epsilon: 0.05 }
    }
}

impl RobustnessEvaluator {
    /// Generate the neighborhood param sets for `candidate` — perturb each
    /// param by one step (continuous → ±ε % of the range; discrete → next/prev
    /// value). Returns the neighbor param sets (excluding the candidate itself).
    /// NB: does NOT re-resolve `build_upon` — the candidate's params are
    /// already final (resolved); the neighborhood perturbs each param in
    /// isolation (a sensitivity measure, not an invariant maintainer).
    pub fn neighborhood(
        &self,
        candidate: &HashMap<String, f64>,
        specs: &[ParamSpec],
    ) -> Vec<HashMap<String, f64>> {
        let mut neighbors = Vec::new();
        for spec in specs {
            let Some(&val) = candidate.get(&spec.name) else {
                continue;
            };
            let dist = spec.effective_distribution();
            match &dist {
                Distribution::Continuous { min, max, .. } => {
                    let span = max - min;
                    let delta = self.epsilon * span;
                    let lo = spec.value_type.cast((val - delta).max(*min));
                    let hi = spec.value_type.cast((val + delta).min(*max));
                    if lo != val {
                        neighbors.push(with(candidate, &spec.name, lo));
                    }
                    if hi != val {
                        neighbors.push(with(candidate, &spec.name, hi));
                    }
                }
                Distribution::Discrete(values) => {
                    // Perturb to the next + previous discrete value.
                    if let Some(idx) = values.iter().position(|v| (*v - val).abs() < 1e-9) {
                        if idx > 0 {
                            neighbors.push(with(candidate, &spec.name, values[idx - 1]));
                        }
                        if idx + 1 < values.len() {
                            neighbors.push(with(candidate, &spec.name, values[idx + 1]));
                        }
                    }
                }
            }
        }
        neighbors
    }
}

/// Clone `params` + override `name` → `val`.
fn with(params: &HashMap<String, f64>, name: &str, val: f64) -> HashMap<String, f64> {
    let mut p = params.clone();
    p.insert(name.to_string(), val);
    p
}

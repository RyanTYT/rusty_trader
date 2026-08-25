//! The optimizer — decides what param sets to evaluate next.
//!
//! Phase 1: [`GridOptimizer`] (exhaustive cross-product) + [`RandomOptimizer`]
//! (the baseline). Phase 2: [`TpeOptimizer`] (Tree-structured Parzen
//! Estimator — Bayesian, sample-efficient, handles mixed discrete/continuous).
//!
//! The `Optimizer` trait is **sequential** (`next_batch(history, budget)`) so
//! the TPE can adapt to prior results. The `GridOptimizer` ignores the
//! history (fixed grid); the `TpeOptimizer` fits densities from it.

use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use trading_app::backtester::BacktestResults;

use crate::config::param_spec::ParamSpec;

/// One evaluated param set: the params, the results, the robustness
/// neighborhood (if evaluated), + the objective score.
#[derive(Debug, Clone)]
pub struct EvalResult {
    pub params: HashMap<String, f64>,
    pub results: BacktestResults,
    /// The neighborhood's results (empty if robustness wasn't evaluated).
    pub neighborhood: Vec<BacktestResults>,
    /// The objective score (higher = better).
    pub score: f64,
}

/// The optimizer — generates the next batch of param sets to evaluate,
/// optionally adapting to prior results. Returns `None` when the search is
/// exhausted (grid done / budget spent).
pub trait Optimizer: Send + Sync {
    fn next_batch(
        &mut self,
        history: &[EvalResult],
        budget: usize,
    ) -> Option<Vec<HashMap<String, f64>>>;
}

/// Grid optimizer — the exhaustive cross-product. Time params use their
/// discrete "natural" scales; continuous params are discretized into
/// `n_steps_per_continuous` evenly-spaced values. Ignores the history (fixed
/// grid). Serves the grid in batches of `budget`.
pub struct GridOptimizer {
    grid: Vec<HashMap<String, f64>>,
    next: usize,
}

impl GridOptimizer {
    pub fn new(specs: &[ParamSpec], n_steps_per_continuous: usize) -> Self {
        let grid = generate_grid(specs, n_steps_per_continuous);
        tracing::info!("GridOptimizer: {} param sets", grid.len());
        Self { grid, next: 0 }
    }
}

impl Optimizer for GridOptimizer {
    fn next_batch(
        &mut self,
        _history: &[EvalResult],
        budget: usize,
    ) -> Option<Vec<HashMap<String, f64>>> {
        if self.next >= self.grid.len() {
            return None;
        }
        let end = (self.next + budget).min(self.grid.len());
        let batch = self.grid[self.next..end].to_vec();
        self.next = end;
        Some(batch)
    }
}

/// Random optimizer — samples `n_total` param sets uniformly at random. The
/// baseline: if `GridOptimizer`/`TpeOptimizer` doesn't beat `RandomOptimizer`
/// after a budget, the signal is weak.
pub struct RandomOptimizer {
    specs: Vec<ParamSpec>,
    n_total: usize,
    n_served: usize,
    rng: StdRng,
}

impl RandomOptimizer {
    pub fn new(specs: &[ParamSpec], n_total: usize, seed: u64) -> Self {
        tracing::info!("RandomOptimizer: {n_total} samples (seed {seed})");
        Self {
            specs: specs.to_vec(),
            n_total,
            n_served: 0,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Optimizer for RandomOptimizer {
    fn next_batch(
        &mut self,
        _history: &[EvalResult],
        budget: usize,
    ) -> Option<Vec<HashMap<String, f64>>> {
        if self.n_served >= self.n_total {
            return None;
        }
        let n = budget.min(self.n_total - self.n_served);
        let batch = (0..n)
            .map(|_| sample_params(&self.specs, &mut self.rng))
            .collect();
        self.n_served += n;
        Some(batch)
    }
}

/// Sample a random param set from the specs. Samples each param from its
/// effective distribution, then resolves `build_upon` (final = sampled +
/// build-upon's value) + casts (value_type) in spec order.
pub(crate) fn sample_params(specs: &[ParamSpec], rng: &mut StdRng) -> HashMap<String, f64> {
    let mut params = HashMap::new();
    for spec in specs {
        params.insert(spec.name.clone(), spec.sample_raw(rng));
    }
    ParamSpec::resolve_build_upon_and_cast(&mut params, specs);
    params
}

/// Generate the cross-product grid. For discrete: each value. For continuous:
/// `n_steps` evenly-spaced values in `[min, max]`. After the cross-product,
/// resolves `build_upon` + casts per combo (so the grid respects the
/// short/long-window build-upon + the int rounding).
fn generate_grid(specs: &[ParamSpec], n_steps: usize) -> Vec<HashMap<String, f64>> {
    let value_lists: Vec<Vec<f64>> = specs
        .iter()
        .map(|s| s.grid_values(n_steps))
        .collect();

    let mut grid: Vec<HashMap<String, f64>> = vec![HashMap::new()];
    for (spec, values) in specs.iter().zip(value_lists.iter()) {
        let mut next = Vec::with_capacity(grid.len() * values.len());
        for params in &grid {
            for &val in values {
                let mut p = params.clone();
                p.insert(spec.name.clone(), val);
                next.push(p);
            }
        }
        grid = next;
    }
    for combo in &mut grid {
        ParamSpec::resolve_build_upon_and_cast(combo, specs);
    }
    grid
}

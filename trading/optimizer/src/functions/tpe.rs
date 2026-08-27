//! TPE (Tree-structured Parzen Estimator) — the Bayesian optimizer.
//!
//! TPE fits two densities from the history:
//! - `l(x)` = the density of the **good** params (top γ quantile by score).
//! - `g(x)` = the density of the **bad** params (the rest).
//!
//! The acquisition function is `EI(x) ∝ l(x) / g(x)` — maximized where the
//! good density is high + the bad density is low. TPE samples candidates from
//! `l(x)`, evaluates `EI` on each, + picks the best.
//!
//! The "tree-structured" part: the densities are a **product of per-dimension
//! densities** (the dimensions are independent given the good/bad split). This
//! avoids the curse of dimensionality (no multivariate KDE).
//!
//! Per-dimension densities:
//! - **Continuous**: a Gaussian KDE (bandwidth = max(Silverman, a fraction of
//!   the range) — avoids collapse for tiny samples).
//! - **Discrete** (time scales): a smoothed histogram (a Dirichlet prior +
//!   Laplace smoothing to avoid zero probabilities).
//!
//! # Parallel batches
//! Each `next_batch` returns `budget` candidates — the top-`budget` by EI
//! among `n_candidates` sampled from `l(x)`. This is the simple parallel-TPE
//! approximation of qEI (joint EI of a batch). Good enough for a backtest
//! sweep (the candidates are run in parallel on the rayon pool).

use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::config::param_spec::{Distribution, ParamSpec};
use crate::functions::optimizer::{EvalResult, Optimizer};

/// The TPE optimizer.
pub struct TpeOptimizer {
    specs: Vec<ParamSpec>,
    n_total: usize,
    n_served: usize,
    /// The number of random warmup evaluations before TPE kicks in.
    n_warmup: usize,
    /// The quantile for the "good" split (e.g. 0.25 = top 25%).
    gamma: f64,
    /// The number of candidates sampled from `l(x)` per batch (the top-`budget`
    /// by EI are returned).
    n_candidates: usize,
    rng: StdRng,
}

impl TpeOptimizer {
    pub fn new(specs: &[ParamSpec], n_total: usize, seed: u64) -> Self {
        let n_warmup = (n_total / 10).max(5).min(30);
        Self {
            specs: specs.to_vec(),
            n_total,
            n_served: 0,
            n_warmup,
            gamma: 0.25,
            n_candidates: 64,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Optimizer for TpeOptimizer {
    fn next_batch(
        &mut self,
        history: &[EvalResult],
        budget: usize,
    ) -> Option<Vec<HashMap<String, f64>>> {
        if self.n_served >= self.n_total {
            return None;
        }
        let n = budget.min(self.n_total - self.n_served);
        let batch = if history.len() < self.n_warmup {
            // Warmup: random samples.
            (0..n)
                .map(|_| crate::functions::optimizer::sample_params(&self.specs, &mut self.rng))
                .collect::<Vec<_>>()
        } else {
            // TPE: sample n_candidates from l(x), pick the top-n by EI.
            let (good, bad) = split_good_bad(history, self.gamma);
            let candidates = (0..self.n_candidates)
                .map(|_| {
                    let params = sample_from_good(&self.specs, &good, &mut self.rng);
                    let ei = expected_improvement(&self.specs, &params, &good, &bad);
                    (params, ei)
                })
                .collect::<Vec<_>>();
            let mut sorted = candidates;
            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            sorted.into_iter().take(n).map(|(p, _)| p).collect()
        };
        self.n_served += n;
        Some(batch)
    }
}

/// Split the history into good (top γ quantile) + bad (the rest), by score.
fn split_good_bad(history: &[EvalResult], gamma: f64) -> (Vec<&EvalResult>, Vec<&EvalResult>) {
    let mut sorted: Vec<&EvalResult> = history.iter().collect();
    sorted.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let n_good = ((sorted.len() as f64) * gamma).ceil() as usize;
    let n_good = n_good.max(1);
    let (good, bad) = sorted.split_at(n_good);
    (good.to_vec(), bad.to_vec())
}

/// Sample a param set from the good density `l(x)` (the product of per-dim
/// good densities). For continuous: pick a random good point, add Gaussian
/// noise (the KDE bandwidth). For discrete: sample from the smoothed histogram.
fn sample_from_good(
    specs: &[ParamSpec],
    good: &[&EvalResult],
    rng: &mut StdRng,
) -> HashMap<String, f64> {
    let mut params = HashMap::new();
    for spec in specs {
        let dist = spec.effective_distribution();
        let (min, max) = dist.range();
        let values: Vec<f64> = good
            .iter()
            .filter_map(|e| e.params.get(&spec.name).copied())
            .collect();
        let val = if values.is_empty() {
            // Fallback: sample from the effective distribution.
            spec.sample_raw(rng)
        } else {
            match &dist {
                Distribution::Continuous { .. } => {
                    let idx = rng.gen_range(0..values.len());
                    let bw = kde_bandwidth(&values, (min, max));
                    let mut v = values[idx] + rng.random::<f64>() * bw;
                    v = v.max(min).min(max);
                    v
                }
                Distribution::Discrete(bins) => {
                    // Smoothed histogram: pick a good value (Laplace smoothing
                    // on the good frequencies).
                    let hist = histogram(bins, &values);
                    let total: f64 = hist.iter().sum::<f64>() + bins.len() as f64; // +1 per bin (Laplace)
                    let r = rng.random::<f64>() * total;
                    let mut acc = 0.0;
                    let mut chosen = bins[bins.len() - 1];
                    for (i, &count) in hist.iter().enumerate() {
                        acc += count + 1.0;
                        if r <= acc {
                            chosen = bins[i];
                            break;
                        }
                    }
                    chosen
                }
            }
        };
        params.insert(spec.name.clone(), val);
    }
    // Resolve build_upon (final = sampled + build-upon's value) + cast (value_type).
    ParamSpec::resolve_build_upon_and_cast(&mut params, specs);
    params
}

/// `EI(x) = ∏ (l_i(x_i) / g_i(x_i))`. Per dimension: the ratio of the good
/// density to the bad density at `x_i`. Returns the log-EI (more stable).
fn expected_improvement(
    specs: &[ParamSpec],
    params: &HashMap<String, f64>,
    good: &[&EvalResult],
    bad: &[&EvalResult],
) -> f64 {
    let mut log_ei = 0.0;
    for spec in specs {
        let Some(&x) = params.get(&spec.name) else {
            continue;
        };
        let dist = spec.effective_distribution();
        let good_vals: Vec<f64> = good
            .iter()
            .filter_map(|e| e.params.get(&spec.name).copied())
            .collect();
        let bad_vals: Vec<f64> = bad
            .iter()
            .filter_map(|e| e.params.get(&spec.name).copied())
            .collect();
        let (l, g) = match &dist {
            Distribution::Continuous { .. } => {
                let (min, max) = dist.range();
                let bw_g = kde_bandwidth(&good_vals, (min, max));
                let bw_b = kde_bandwidth(&bad_vals, (min, max));
                (
                    kde_density(&good_vals, x, bw_g),
                    kde_density(&bad_vals, x, bw_b),
                )
            }
            Distribution::Discrete(bins) => {
                let h_g = histogram(bins, &good_vals);
                let h_b = histogram(bins, &bad_vals);
                let lg = smoothed_prob(&h_g, bins.len());
                let gb = smoothed_prob(&h_b, bins.len());
                let idx = bins.iter().position(|v| (*v - x).abs() < 1e-9);
                (
                    idx.and_then(|i| lg[i]).unwrap_or(1e-9),
                    idx.and_then(|i| gb[i]).unwrap_or(1e-9),
                )
            }
        };
        if g > 0.0 {
            log_ei += (l / g).ln();
        }
    }
    log_ei
}

/// KDE bandwidth: max(Silverman's rule, a fraction of the range). Silverman
/// can collapse to 0 for tiny samples (n<2 or σ=0) — the range fraction is
/// the floor.
fn kde_bandwidth(values: &[f64], (min, max): (f64, f64)) -> f64 {
    let n = values.len() as f64;
    if n < 2.0 {
        return (max - min) * 0.2;
    }
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    let std = var.sqrt();
    // Silverman (1D): h = 1.06·σ·(4/(3n))^0.2. Floor at 5% of the range.
    let silverman = 1.06 * std * (4.0 / (3.0 * n)).powf(0.2);
    let floor = (max - min) * 0.05;
    silverman.max(floor)
}

/// Gaussian KDE density at `x`: `(1/n) Σ N((x - x_i) / h)` where N is the
/// standard normal PDF.
fn kde_density(values: &[f64], x: f64, h: f64) -> f64 {
    if h <= 0.0 || values.is_empty() {
        return 1e-9;
    }
    let n = values.len() as f64;
    let sum: f64 = values
        .iter()
        .map(|&x_i| {
            let z = (x - x_i) / h;
            normal_pdf(z) / h
        })
        .sum();
    (sum / n).max(1e-9)
}

/// The standard normal PDF: `(1/√(2π))·exp(-z²/2)`.
fn normal_pdf(z: f64) -> f64 {
    (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// The frequency counts of `data` over the `bins` values.
fn histogram(bins: &[f64], data: &[f64]) -> Vec<f64> {
    let mut counts = vec![0.0_f64; bins.len()];
    for &d in data {
        if let Some(i) = bins.iter().position(|b| (*b - d).abs() < 1e-9) {
            counts[i] += 1.0;
        }
    }
    counts
}

/// Smoothed probabilities (Laplace: +1 per bin, normalize).
fn smoothed_prob(counts: &[f64], n_bins: usize) -> Vec<Option<f64>> {
    let total: f64 = counts.iter().sum::<f64>() + n_bins as f64; // +1 per bin
    counts
        .iter()
        .map(|&c| {
            if total > 0.0 {
                Some((c + 1.0) / total)
            } else {
                None
            }
        })
        .collect()
}

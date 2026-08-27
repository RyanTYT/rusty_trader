//! The typed parameter search space.
//!
//! Each parameter has:
//! - a [`ValueType`] (I32, U32, F64, ...) — the type of the value (the sampler
//!   rounds to the nearest integer for int types).
//! - a [`Distribution`] — either a `Discrete` set of values, or a `Continuous`
//!   range `[min, max]` with one or more normal [`NormalMode`]s (a single mode
//!   = a plain normal; multiple modes = a multi-modal mixture). Sampling a
//!   continuous: pick a mode (weighted), sample from its normal, truncate to
//!   `[min, max]`.
//! - a [`SpecialParam`] — `None`, or a human-like lookback category
//!   (IntradayLookback/DailyLookback/WeeklyLookback) which **overrides** the
//!   `distribution` with the default human-like scales (seconds for now).
//! - an optional `build_upon` — the name of another param whose sampled value
//!   is **added** to this param's sampled value (for the short/long window
//!   case: `long_window = sampled_long + short_window`, so long is always
//!   > short).
//!
//! The "human params tend to win" belief is encoded as the default scales for
//! the [`SpecialParam`] categories (round, natural scales).

use rand::rngs::StdRng;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// The value type of a parameter. The sampler rounds to the nearest integer
/// for int types; floats are passed through; bool is thresholded at 0.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    I32,
    U32,
    F64,
    F32,
    Bool,
}

impl ValueType {
    /// Cast a sampled f64 to this type's representation (still f64, but
    /// rounded for int types / thresholded for bool).
    pub fn cast(self, v: f64) -> f64 {
        match self {
            ValueType::I32 | ValueType::U32 => v.round(),
            ValueType::F64 | ValueType::F32 => v,
            ValueType::Bool => {
                if v >= 0.5 {
                    1.0
                } else {
                    0.0
                }
            }
        }
    }
}

/// One mode of a normal distribution (a Gaussian). A single mode = a plain
/// normal; multiple modes = a multi-modal mixture. `weight` is the mixture
/// weight (default 1.0; the sampler normalizes across modes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalMode {
    pub mean: f64,
    pub std: f64,
    #[serde(default = "default_weight")]
    pub weight: f64,
}

fn default_weight() -> f64 {
    1.0
}

/// The distribution to sample a parameter from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Distribution {
    /// A discrete set of values — pick one (uniformly).
    Discrete(Vec<f64>),
    /// A continuous distribution over `[min, max]`. A mixture of one or more
    /// normal `modes` (a single mode = a plain normal; multiple = multi-modal).
    /// Sampling: pick a mode (weighted), sample from its normal, truncate to
    /// `[min, max]`. If `modes` is empty, falls back to uniform over `[min, max]`.
    Continuous {
        min: f64,
        max: f64,
        modes: Vec<NormalMode>,
    },
}

impl Distribution {
    /// The `(min, max)` range. For `Discrete`, the min/max of the values
    /// (or `(0.0, 0.0)` if empty).
    pub fn range(&self) -> (f64, f64) {
        match self {
            Distribution::Discrete(v) => {
                if v.is_empty() {
                    (0.0, 0.0)
                } else {
                    let min = v.iter().copied().fold(f64::INFINITY, f64::min);
                    let max = v.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                    (min, max)
                }
            }
            Distribution::Continuous { min, max, .. } => (*min, *max),
        }
    }

    /// Sample one RAW value from the distribution (no value-type cast — the
    /// caller casts via [`ValueType::cast`] if needed).
    pub fn sample(&self, rng: &mut StdRng) -> f64 {
        match self {
            Distribution::Discrete(v) => {
                if v.is_empty() {
                    0.0
                } else {
                    v[rng.gen_range(0..v.len())]
                }
            }
            Distribution::Continuous { min, max, modes } => {
                if modes.is_empty() {
                    return rng.gen_range(*min..*max);
                }
                let total_weight: f64 = modes.iter().map(|m| m.weight).sum();
                if total_weight <= 0.0 {
                    return rng.gen_range(*min..*max);
                }
                // Pick a mode (weighted).
                let mut r = rng.random::<f64>() * total_weight;
                let mut chosen = &modes[0];
                for m in modes {
                    r -= m.weight;
                    if r <= 0.0 {
                        chosen = m;
                        break;
                    }
                }
                // Sample from the chosen normal (Box-Muller), truncate to [min, max].
                let val = chosen.mean + chosen.std * sample_standard_normal(rng);
                val.max(*min).min(*max)
            }
        }
    }

    /// The grid values for the distribution. For `Discrete`: the values
    /// themselves. For `Continuous`: `n_steps` evenly-spaced values in
    /// `[min, max]` (inclusive of both endpoints; the modes don't affect the
    /// grid — the grid covers the range).
    pub fn grid_values(&self, n_steps: usize) -> Vec<f64> {
        match self {
            Distribution::Discrete(v) => v.clone(),
            Distribution::Continuous { min, max, .. } => {
                if n_steps <= 1 {
                    vec![(*min + *max) / 2.0]
                } else {
                    (0..n_steps)
                        .map(|i| min + (max - min) * i as f64 / (n_steps - 1) as f64)
                        .collect()
                }
            }
        }
    }

    /// Whether this is a discrete distribution (for the TPE / robustness to
    /// decide between KDE + smoothed-histogram).
    pub fn is_discrete(&self) -> bool {
        matches!(self, Distribution::Discrete(_))
    }
}

/// Sample a standard normal (Box-Muller transform).
fn sample_standard_normal(rng: &mut StdRng) -> f64 {
    let u1: f64 = rng.gen();
    let u2: f64 = rng.gen();
    let r = (-2.0 * u1.ln()).sqrt();
    let theta = 2.0 * std::f64::consts::PI * u2;
    r * theta.cos()
}

/// A "special" human-like lookback category. When set (not `None`), it
/// **overrides** the [`Distribution`] with the default human-like scales
/// (seconds for now — ease of compatibility with the existing time params).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SpecialParam {
    #[default]
    None,
    IntradayLookback,
    DailyLookback,
    WeeklyLookback,
}

impl SpecialParam {
    /// The default human-like scales (seconds) for this category. Returns
    /// `None` for `None`.
    pub fn default_scales(self) -> Option<&'static [f64]> {
        match self {
            SpecialParam::None => None,
            SpecialParam::IntradayLookback => Some(&[60.0, 300.0, 900.0, 1800.0, 3600.0]),
            SpecialParam::DailyLookback => Some(&[14400.0, 28800.0, 86400.0, 172800.0]),
            SpecialParam::WeeklyLookback => Some(&[604_800.0, 1_209_600.0, 2_592_000.0]),
        }
    }
}

/// The description of one tunable parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamSpec {
    pub name: String,
    pub value_type: ValueType,
    pub distribution: Distribution,
    #[serde(default)]
    pub special: SpecialParam,
    /// If `Some(other)`, the final value = (this param's sampled value) +
    /// (the `other` param's final value). Used for the short/long window case
    /// (long_window = sampled_long + short_window, so long > short). NB: the
    /// `other` param must be sampled/resolved BEFORE this one (order by
    /// listing the build-upon target earlier in the specs).
    #[serde(default)]
    pub build_upon: Option<String>,
}

impl ParamSpec {
    /// The effective distribution — `special` overrides `distribution` with
    /// the default human-like scales when set (not `None`).
    pub fn effective_distribution(&self) -> Distribution {
        if self.special != SpecialParam::None {
            if let Some(scales) = self.special.default_scales() {
                return Distribution::Discrete(scales.to_vec());
            }
        }
        self.distribution.clone()
    }

    /// Sample one RAW value from the effective distribution (no value-type cast).
    pub fn sample_raw(&self, rng: &mut StdRng) -> f64 {
        self.effective_distribution().sample(rng)
    }

    /// The grid values for the effective distribution.
    pub fn grid_values(&self, n_steps: usize) -> Vec<f64> {
        self.effective_distribution().grid_values(n_steps)
    }

    /// The `(min, max)` range of the effective distribution.
    pub fn range(&self) -> (f64, f64) {
        self.effective_distribution().range()
    }

    /// Whether the effective distribution is discrete.
    pub fn is_discrete(&self) -> bool {
        self.effective_distribution().is_discrete()
    }

    /// Resolve `build_upon` (final = sampled + build-upon's final) + cast
    /// (value_type) across a param set. Specs must be ordered so build-upon
    /// targets come before. Operates in place.
    pub fn resolve_build_upon_and_cast(
        params: &mut std::collections::HashMap<String, f64>,
        specs: &[ParamSpec],
    ) {
        for spec in specs {
            let mut val = *params.get(&spec.name).unwrap_or(&0.0);
            if let Some(other) = &spec.build_upon {
                if let Some(&other_val) = params.get(other) {
                    val += other_val;
                }
            }
            params.insert(spec.name.clone(), spec.value_type.cast(val));
        }
    }

    /// Convenience: a continuous param over `[min, max]` with a single normal
    /// mode at the midpoint (std = range/4 → ~95% of samples in [min, max]).
    pub fn continuous(name: impl Into<String>, min: f64, max: f64) -> Self {
        Self {
            name: name.into(),
            value_type: ValueType::F64,
            distribution: Distribution::Continuous {
                min,
                max,
                modes: vec![NormalMode {
                    mean: (min + max) / 2.0,
                    std: (max - min) / 4.0,
                    weight: 1.0,
                }],
            },
            special: SpecialParam::None,
            build_upon: None,
        }
    }

    /// Convenience: a discrete param with an explicit set of values.
    pub fn discrete(name: impl Into<String>, values: Vec<f64>) -> Self {
        Self {
            name: name.into(),
            value_type: ValueType::F64,
            distribution: Distribution::Discrete(values),
            special: SpecialParam::None,
            build_upon: None,
        }
    }

    /// Convenience: a special-param (uses the default human-like scales for
    /// the category). `value_type` is usually I32/U32 (bar/second counts).
    pub fn special(
        name: impl Into<String>,
        value_type: ValueType,
        special: SpecialParam,
    ) -> Self {
        Self {
            name: name.into(),
            value_type,
            distribution: Distribution::Discrete(
                special.default_scales().unwrap_or(&[]).to_vec(),
            ),
            special,
            build_upon: None,
        }
    }
}

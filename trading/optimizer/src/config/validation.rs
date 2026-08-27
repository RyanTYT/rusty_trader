//! Validation scheme — the in-sample/out-of-sample split (the overfitting
//! guardrail).
//!
//! Phase 1: [`Holdout`] (a single split). Phase 3: [`WalkForward`] (rolling
//! windows — the gold standard). The optimizer runs on each in-sample
//! window; the best params are validated on the immediately-following
//! out-of-sample window. The OOS windows tile the period contiguously → the
//! concatenated OOS equity curve is the "true" performance (the strategy's
//! actual performance if you'd re-optimized periodically + traded the OOS).

use chrono::Duration;
use trading_app::backtester::BacktestPeriod;

/// The validation scheme.
#[derive(Debug, Clone)]
pub enum ValidationScheme {
    /// No out-of-sample validation (optimize on the full period — risky, no
    /// overfitting guardrail). Use for quick exploration only.
    None,
    /// A single split: optimize on `in_sample`, validate the best params on
    /// `out_of_sample`.
    Holdout(Holdout),
    /// Rolling windows (the gold standard). For each window: optimize on the
    /// in-sample, validate on the immediately-following out-of-sample. The OOS
    /// windows tile the period contiguously.
    WalkForward(WalkForward),
}

/// A single in-sample/out-of-sample split.
#[derive(Debug, Clone)]
pub struct Holdout {
    pub in_sample: BacktestPeriod,
    pub out_sample: BacktestPeriod,
}

impl Holdout {
    /// Split a time range at `split` (exclusive end of in-sample, exclusive
    /// start of out-sample).
    pub fn split_at(
        start: chrono::DateTime<chrono::Utc>,
        split: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            in_sample: BacktestPeriod::TimeRange { start, end: split },
            out_sample: BacktestPeriod::TimeRange { start: split, end },
        }
    }
}

/// Rolling walk-forward windows. For each window: IS = `[cursor, cursor+is)`,
/// OS = `[cursor+is, cursor+is+os)`. The cursor advances by `out_sample`
/// each window → the OOS windows are contiguous (W2 OS starts where W1 OS
/// ends). The first IS period is the "training" period (no OOS covers it).
#[derive(Debug, Clone)]
pub struct WalkForward {
    pub in_sample: Duration,
    pub out_sample: Duration,
}

impl WalkForward {
    /// Generate the (in_sample, out_sample) windows for a full `TimeRange`
    /// period. The OOS windows tile `[start + in_sample, end]` contiguously.
    /// Returns an empty vec for non-`TimeRange` periods.
    pub fn windows(&self, full: &BacktestPeriod) -> Vec<(BacktestPeriod, BacktestPeriod)> {
        let (start, end) = match full {
            BacktestPeriod::TimeRange { start, end } => (*start, *end),
            _ => return Vec::new(),
        };
        let mut windows = Vec::new();
        let mut cursor = start;
        while cursor + self.in_sample + self.out_sample <= end {
            let is_end = cursor + self.in_sample;
            let os_end = is_end + self.out_sample;
            windows.push((
                BacktestPeriod::TimeRange {
                    start: cursor,
                    end: is_end,
                },
                BacktestPeriod::TimeRange {
                    start: is_end,
                    end: os_end,
                },
            ));
            cursor = cursor + self.out_sample; // step by out_sample → contiguous OOS
        }
        windows
    }
}

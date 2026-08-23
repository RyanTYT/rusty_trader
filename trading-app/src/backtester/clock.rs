//! Shared backtest clock — the "current bar time" the strategy reads and the
//! price oracle use *instead of* wall-clock `now()` / `Utc::now()`.
//!
//! In production, `now()` (DB clock) and `Utc::now()` (app clock) both mean
//! "real now", which coincides with the latest realtime bar. In a backtest
//! replaying a historical period, wall-clock is 2026 while the bar being
//! replayed is, e.g., 2024-06-15 — so any strategy read keyed on "today" would
//! be wrong. The replayer advances this clock to each bar's time before
//! invoking `on_bar_update`; the as-of query methods (Phase 1) and the price
//! oracle read it here.

use std::sync::Mutex;

use chrono::{DateTime, Utc};

#[derive(Debug, Default)]
pub struct BacktestClock {
    now: Mutex<Option<DateTime<Utc>>>,
}

impl BacktestClock {
    pub fn new() -> Self {
        Self {
            now: Mutex::new(None),
        }
    }

    /// Advance the clock to `t`. Called by the replayer before each tick.
    pub fn set(&self, t: DateTime<Utc>) {
        *self.now.lock().expect("BacktestClock poisoned") = Some(t);
    }

    /// The current backtest time, or wall-clock `Utc::now()` if never set
    /// (so a stray call before the first tick doesn't panic).
    pub fn now(&self) -> DateTime<Utc> {
        self.now
            .lock()
            .expect("BacktestClock poisoned")
            .unwrap_or_else(Utc::now)
    }
}

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
//!
//! Lock-free: the time is stored as an `AtomicI64` (timestamp nanos). The
//! backtester is single-threaded (the replayer's `spawn_blocking` thread is
//! the only writer/reader), but the clock is shared via `Arc` (replayer +
//! price supplier) + moved into `spawn_blocking`, so it must be `Send + Sync`
//! — `AtomicI64` satisfies that without a `Mutex`.

use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Utc};

#[derive(Debug, Default)]
pub struct BacktestClock {
    /// Timestamp nanos; `0` = not yet set (so `now()` falls back to wall-clock).
    now: AtomicI64,
}

impl BacktestClock {
    pub fn new() -> Self {
        Self {
            now: AtomicI64::new(0),
        }
    }

    /// Advance the clock to `t`. Called by the replayer before each tick.
    /// Lock-free atomic store.
    pub fn set(&self, t: DateTime<Utc>) {
        let nanos = t.timestamp_nanos_opt().unwrap_or(0);
        self.now.store(nanos, Ordering::Release);
    }

    /// The current backtest time, or wall-clock `Utc::now()` if never set
    /// (so a stray call before the first tick doesn't panic).
    /// Lock-free atomic load.
    pub fn now(&self) -> DateTime<Utc> {
        let nanos = self.now.load(Ordering::Acquire);
        if nanos == 0 {
            Utc::now()
        } else {
            DateTime::<Utc>::from_timestamp_nanos(nanos)
        }
    }
}

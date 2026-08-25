//! In-memory bar cache for the backtest. Stores the **full pre-populated bar
//! set** (not filtered by `bar_time` — strategies may hardcode lookbacks) +
//! tracks the **max end-time** (the latest bar fetched by `read_last_n`) so
//! the backtester can trim the replay stream to post-warm-up (no data leakage
//! / corruption).
//!
//! `read_last_n(N, bar_time)` reads the N bars before `bar_time` from the
//! cached bar set (in-memory slice, oldest-first, `min(N, available)`) +
//! updates `max_end_time` to the most-recent bar returned. The backtester
//! reads `max_end_time` after warm-up + discards the already-read bars from
//! the backtest stream.
//!
//! Shared across the sweep via a thread-local (set per-backtest in
//! `run_with_bars`). The bars are shared (the same set for every backtest);
//! the `max_end_time` tracker is per-backtest (each backtest's warm-up reads
//! its own lookback + tracks its own end bar).

use std::cell::RefCell;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::database::models_crud::historical_data::historical_data::{
    HistoricalDataFullKeys, HistoricalDataPrimaryKeysWoTime,
};

/// The bar cache + max-end-time tracker.
///
/// `bars` is the full pre-populated bar set (shared across the sweep via
/// `Arc` — cheap clone per backtest, no full-data duplication), sorted
/// oldest-first. `max_end_time` is the latest bar fetched by `read_last_n`
/// (per-backtest, via interior mutability) — the backtester reads it to trim
/// the replay stream to post-warm-up.
pub struct InMemoryHistoricalCache {
    bars: Arc<Vec<HistoricalDataFullKeys>>,
    max_end_time: RefCell<Option<DateTime<Utc>>>,
}

impl InMemoryHistoricalCache {
    /// Build from the full pre-populated bar set (shared via `Arc`). Each
    /// backtest clones the `Arc` (cheap) + gets a fresh `max_end_time`.
    pub fn new(bars: Arc<Vec<HistoricalDataFullKeys>>) -> Self {
        Self {
            bars,
            max_end_time: RefCell::new(None),
        }
    }

    /// Read the bars before `bar_time` matching `pk` (the lookback window).
    /// Returns `min(limit, available)` bars, oldest-first. Updates
    /// `max_end_time` to the most-recent bar returned (the end of the window).
    /// Returns `None` if no matching bars exist before `bar_time` (cache miss
    /// → the caller falls through to the DB).
    pub fn get_bars_before(
        &self,
        pk: &HistoricalDataPrimaryKeysWoTime,
        bar_time: DateTime<Utc>,
        limit: usize,
    ) -> Option<Vec<HistoricalDataFullKeys>> {
        // Filter by pk (asset type + stock/pe/currency) + time < bar_time.
        let before: Vec<&HistoricalDataFullKeys> = self
            .bars
            .iter()
            .filter(|b| bar_matches_pk(b, pk))
            .filter(|b| b.get_time() < bar_time)
            .collect();
        if before.is_empty() {
            return None;
        }
        // Take the last `limit` (most recent before bar_time), reverse to oldest-first.
        let take_n = limit.min(before.len());
        let mut window: Vec<HistoricalDataFullKeys> =
            before.iter().rev().take(take_n).map(|b| (*b).clone()).collect();
        window.reverse(); // oldest-first
        // Track the most-recent bar (the end of the window).
        if let Some(latest) = window.last() {
            let t = latest.get_time();
            let mut max_end = self.max_end_time.borrow_mut();
            match *max_end {
                Some(cur) if t > cur => *max_end = Some(t),
                None => *max_end = Some(t),
                _ => {}
            }
        }
        Some(window)
    }

    /// The latest bar fetched by `read_last_n` across all calls in this
    /// backtest. The backtester reads this after warm-up to trim the replay
    /// stream (discard bars <= this time).
    pub fn max_end_time(&self) -> Option<DateTime<Utc>> {
        *self.max_end_time.borrow()
    }
}

/// Whether a bar matches a pk (same asset type + stock/pe/currency).
fn bar_matches_pk(bar: &HistoricalDataFullKeys, pk: &HistoricalDataPrimaryKeysWoTime) -> bool {
    use crate::database::models_crud::historical_data::historical_data::HistoricalDataPrimaryKeysWoTime as Pk;
    match (bar, pk) {
        (HistoricalDataFullKeys::Stock(b), Pk::Stock(p)) => {
            b.stock == p.stock && b.primary_exchange == p.primary_exchange && b.currency == p.currency
        }
        (HistoricalDataFullKeys::DailyStock(b), Pk::DailyStock(p)) => {
            b.stock == p.stock && b.primary_exchange == p.primary_exchange && b.currency == p.currency
        }
        (HistoricalDataFullKeys::Options(b), Pk::Options(p)) => {
            b.stock == p.stock
                && b.primary_exchange == p.primary_exchange
                && b.currency == p.currency
                && b.expiry == p.expiry
                && b.strike == p.strike
                && b.multiplier == p.multiplier
                && b.option_type == p.option_type
        }
        (HistoricalDataFullKeys::Forex(b), Pk::Forex(p)) => b.pair == p.pair,
        _ => false,
    }
}

thread_local! {
    static HISTORICAL_CACHE: RefCell<Option<Arc<InMemoryHistoricalCache>>> =
        const { RefCell::new(None) };
}

/// Set the thread-local bar cache. Called by `InMemoryReplay::run_with_bars`
/// (per-backtest, on the rayon worker thread) so `read_last_n` can read the
/// cached bars + the backtester can read `max_end_time` after warm-up.
pub fn set(cache: Arc<InMemoryHistoricalCache>) {
    HISTORICAL_CACHE.with(|c| *c.borrow_mut() = Some(cache));
}

/// Get the thread-local bar cache, if set. Called by `read_last_n` (backtest)
/// + the backtester (to read `max_end_time`).
pub fn current() -> Option<Arc<InMemoryHistoricalCache>> {
    HISTORICAL_CACHE.with(|c| c.borrow().clone())
}

/// Clear the thread-local cache. Called by `run_with_bars` after the loop.
pub fn clear() {
    HISTORICAL_CACHE.with(|c| *c.borrow_mut() = None);
}

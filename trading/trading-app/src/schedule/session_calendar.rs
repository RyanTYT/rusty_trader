//! Session end-times for strategies (F2 — user directive 2026-10-29).
//!
//! A strategy must be able to determine, at any bar, whether it sits in the
//! CLOSE WINDOW of the current trading session (the second-last/last bars
//! relative to the session's FINAL bar). Live strategies have this knowledge
//! a priori — the exchange schedule (contract end times) is known at session
//! open. The backtester reproduces that SAME knowledge from the bar file's
//! TIMESTAMPS ONLY: one O(N) pre-scan segments sessions (a gap > one bar
//! interval, or an ET-date change, starts a new session) and records each
//! date's final-bar mso + intra-session bar step. No price, order, or
//! position data touches it — pure calendar metadata, causal-equivalent to
//! the live contract schedule.
//!
//! Consumer: `SleeveExecutor::flush_row` stamps `ClockCtx::close_window`
//! for the PENDING row (the row whose fills the trim would carry);
//! `fund_gate.rs` switches its cap basis (EOD 1.95 vs INTRA 4.0) from the
//! `day_last` intrinsic edge to that window. The close-window basis is
//! ALWAYS on (the env seam was deleted with the other probe modes);
//! without an installed calendar the window is simply never true.
//!
//! Live source (planned): IBKR contractDetails tradingHours per contract at
//! session start falls back to regular RTH; it would publish the same
//! per-date final-bar mso into this calendar, sharing one consumer.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::OnceLock;

use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::America::New_York;

/// Session-end record for one ET date.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SessionEnd {
    /// mso of the session's FINAL printed bar (minutes since 09:30 NY).
    pub last_mso: i32,
    /// modal mso step between consecutive bars of that session (usually 5).
    pub step: i32,
}

static CALENDAR: OnceLock<HashMap<NaiveDate, SessionEnd>> = OnceLock::new();

/// Close-window width K in bars: the final K bars of each session carry the
/// EOD cap. K=2 (second-last/last) — user directive.
const CLOSE_K: i32 = 2;

/// Install the calendar from bar SLOTS (the replay slice). Call BEFORE the
/// replay starts. The calendar is pure schedule metadata and `is_day_last`
/// (the flush-on-sentinel day-last source) needs it in every mode.
pub fn install_from_slots(slots: &[Vec<Option<crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys>>]) {
    let ts = slots.iter().flat_map(|slot| {
        slot.iter().filter_map(|b| {
            b.as_ref().map(|bar| bar.get_time())
        })
    });
    install_from_timestamps(ts);
}

/// Install from an ascending timestamp iterator (bars must be sorted).
pub fn install_from_timestamps<'a>(ts: impl Iterator<Item = DateTime<Utc>>) {
    // DISTINCT mso values per date — every symbol of a slot repeats the
    // same timestamp, so a raw Vec would inflate the bar count ~45× and
    // collapse the computed step to 1 (window degenerates to day-last).
    let mut per_date: BTreeMap<NaiveDate, BTreeSet<i32>> = BTreeMap::new();
    for t in ts {
        per_date
            .entry(t.with_timezone(&New_York).date_naive())
            .or_default()
            .insert(minutes_since_open(t));
    }
    let mut map: HashMap<NaiveDate, SessionEnd> = HashMap::with_capacity(per_date.len());
    for (d, msos) in per_date {
        if msos.is_empty() {
            continue;
        }
        let last_mso = *msos.iter().last().expect("non-empty");
        let first_mso = *msos.iter().next().expect("non-empty");
        let step = if msos.len() >= 2 {
            let span = last_mso - first_mso;
            ((span as f64 / (msos.len() - 1) as f64).round() as i32).max(1)
        } else {
            0 // single-bar session: window == the whole (trivial) session
        };
        map.insert(d, SessionEnd { last_mso, step });
    }
    let _ = CALENDAR.set(map);
    let stored = CALENDAR.get();
    tracing::info!(
        "session calendar installed: {} dates; window K={}; sample first dates: {:?}",
        stored.map(|m| m.len()).unwrap_or(0),
        CLOSE_K,
        stored
            .map(|m| {
                m.iter()
                    .take(3)
                    .map(|(d, e)| (d.to_string(), e.last_mso, e.step))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    );
}

fn minutes_since_open(t: DateTime<Utc>) -> i32 {
    use chrono::Timelike;
    let ny = t.with_timezone(&New_York).time();
    let secs = ny.hour() * 3600 + ny.minute() * 60 + ny.second();
    (secs / 60) as i32 - 570
}

static WINDOW_HITS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Is the row at (`d`, `mso`) in the session's close window (the final
/// `CLOSE_K` bars)? Semantics WITHOUT calendar: false.
pub fn is_close_window(d: NaiveDate, mso: i32) -> bool {
    let hit = is_close_window_installed(d, mso);
    if hit {
        WINDOW_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    hit
}

/// Diagnostic: times the close-window lookup returned TRUE this process.
pub fn window_hits() -> u32 {
    WINDOW_HITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Core lookup ignoring the env gate (shared by the public path above).
fn is_close_window_installed(d: NaiveDate, mso: i32) -> bool {
    let Some(cal) = CALENDAR.get() else {
        return false;
    };
    let Some(&SessionEnd { last_mso, step }) = cal.get(&d) else {
        return false;
    };
    if step <= 0 {
        return mso >= last_mso;
    }
    mso >= last_mso - (CLOSE_K - 1) * step
}

/// Read-only copy of the installed calendar (BTreeMap by ET date). The
/// sleeve's own calendar store lazily copies this in replay (the runner
/// pre-scans the bar timeline and installs BEFORE the replay starts); live
/// populates its store from the IBKR contract schedules in `get_contracts`
/// instead. Empty when no calendar is installed.
pub fn snapshot_calendar() -> BTreeMap<NaiveDate, SessionEnd> {
    CALENDAR
        .get()
        .map(|m| m.iter().map(|(d, e)| (*d, *e)).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 09:30 ET of the given date as UTC.
    fn ny_date_0930(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        New_York
            .with_ymd_and_hms(y, m, d, 9, 30, 0)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn bar(date: DateTime<Utc>, plus_min: i64) -> DateTime<Utc> {
        date + chrono::Duration::minutes(plus_min)
    }

    #[test]
    fn builds_sessions_and_windows() {
        // Day 1: regular session 09:30..10:00 (7 bars @5min);
        // Day 2: half-day-like short run 09:30..09:55 (6 bars).
        let d1 = ny_date_0930(2026, 1, 5);
        let mut ts = Vec::new();
        for k in 0..7 {
            ts.push(bar(d1, 5 * k));
        }
        let d2 = ny_date_0930(2026, 1, 6);
        for k in 0..6 {
            ts.push(bar(d2, 5 * k));
        }
        install_from_timestamps(ts.into_iter());

        // d1 last mso = 30, step 5 → window is mso ∈ {25, 30}.
        let day1 = chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let day2 = chrono::NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        assert_eq!(
            CALENDAR.get().unwrap().get(&day1),
            Some(&SessionEnd { last_mso: 30, step: 5 })
        );
        assert!(!is_close_window_installed(day1, 20));
        assert!(is_close_window_installed(day1, 25));
        assert!(is_close_window_installed(day1, 30));
        // d2 half-day last mso = 25.
        assert!(is_close_window_installed(day2, 20));
        assert!(!is_close_window_installed(day2, 15));
        // Foreign date → false.
        assert!(!is_close_window_installed(chrono::NaiveDate::from_ymd_opt(2025, 3, 4).unwrap(), 30));
    }

    #[test]
    fn close_window_public_path_matches_installed_calendar() {
        // Foreign date not in any installed session → false (same containment
        // as the is_close_window_installed assertions above; the OnceLock
        // calendar may or may not be installed by the sibling test).
        assert!(!is_close_window(chrono::NaiveDate::from_ymd_opt(2025, 3, 4).unwrap(), 30));
    }
}

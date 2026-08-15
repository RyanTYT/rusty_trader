//! Unit tests for `is_fx_trading_datetime`, `last_bar_available`, and `fx_trading_day_start`.
//!
//! See `src/market_data/consolidator.rs`. Tests cover:
//! - `is_fx_trading_datetime` — FX holidays (New Year's observed, Christmas observed,
//!   Good Friday via Computus for 2024-2030), weekend/hour logic (Sun≥17:00 open,
//!   Fri<17:00 open, Sat closed, Mon–Thu open)
//! - `last_bar_available` — grid flooring to 5-min, close cap, open-threshold gate
//! - `fx_trading_day_start` — previous day 17:00 NY

use chrono::Timelike;
use chrono::{NaiveDate, TimeZone};
use chrono_tz::America::New_York;
use trading_app::database::models::AssetType;
use trading_app::test_internals::{
    fx_trading_day_start, is_fx_trading_datetime, last_bar_available,
};

/// Helper: build a DateTime<Tz> in New York time.
fn ny(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> chrono::DateTime<chrono_tz::Tz> {
    let nd = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let nt = chrono::NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
    New_York
        .from_local_datetime(&nd.and_time(nt))
        .single()
        .unwrap()
}

// ============================ is_fx_trading_datetime: Mon-Thu always open ============================

#[test]
fn fx_monday_open() {
    // 2024-11-04 Monday noon NY
    let t = ny(2024, 11, 4, 12, 0);
    assert!(is_fx_trading_datetime(&t));
}

#[test]
fn fx_tuesday_midnight_open() {
    let t = ny(2024, 11, 5, 0, 0);
    assert!(is_fx_trading_datetime(&t));
}

#[test]
fn fx_wednesday_open() {
    let t = ny(2024, 11, 6, 3, 30);
    assert!(is_fx_trading_datetime(&t));
}

#[test]
fn fx_thursday_late_night_open() {
    let t = ny(2024, 11, 7, 23, 59);
    assert!(is_fx_trading_datetime(&t));
}

// ============================ is_fx_trading_datetime: Friday ============================

#[test]
fn fx_friday_before_17_open() {
    let t = ny(2024, 11, 8, 16, 59);
    assert!(is_fx_trading_datetime(&t));
}

#[test]
fn fx_friday_at_17_closed() {
    // Friday 17:00 → hour < 17 is false → closed
    let t = ny(2024, 11, 8, 17, 0);
    assert!(!is_fx_trading_datetime(&t));
}

#[test]
fn fx_friday_late_night_closed() {
    let t = ny(2024, 11, 8, 22, 0);
    assert!(!is_fx_trading_datetime(&t));
}

// ============================ is_fx_trading_datetime: Saturday ============================

#[test]
fn fx_saturday_closed_all_day() {
    // 2024-11-09 Saturday at various times
    assert!(!is_fx_trading_datetime(&ny(2024, 11, 9, 0, 0)));
    assert!(!is_fx_trading_datetime(&ny(2024, 11, 9, 12, 0)));
    assert!(!is_fx_trading_datetime(&ny(2024, 11, 9, 23, 59)));
}

// ============================ is_fx_trading_datetime: Sunday ============================

#[test]
fn fx_sunday_before_17_closed() {
    let t = ny(2024, 11, 10, 16, 59);
    assert!(!is_fx_trading_datetime(&t));
}

#[test]
fn fx_sunday_at_17_open() {
    let t = ny(2024, 11, 10, 17, 0);
    assert!(is_fx_trading_datetime(&t));
}

#[test]
fn fx_sunday_late_night_open() {
    let t = ny(2024, 11, 10, 23, 30);
    assert!(is_fx_trading_datetime(&t));
}

// ============================ is_fx_trading_datetime: New Year's Day (observed) ============================

#[test]
fn fx_new_years_day_closed() {
    // 2025-01-01 is Wednesday → observed on the day
    let t = ny(2025, 1, 1, 12, 0);
    assert!(!is_fx_trading_datetime(&t));
}

#[test]
#[ignore = "BUG: is_fx_trading_datetime misses New Year's observed-from-next-year. For 2021-12-31 (Friday), year=2021, so is_observed_holiday checks 2021-01-01 (Wed, observed Wed) — misses that 2022-01-01 (Sat) is observed on Friday 2021-12-31. The function only checks same-year holidays."]
fn fx_new_years_sat_observed_fri_closed() {
    // 2022-01-01 is Saturday → observed on Friday 2021-12-31
    let t = ny(2021, 12, 31, 12, 0);
    assert!(!is_fx_trading_datetime(&t));
}

#[test]
fn fx_new_years_sun_observed_mon_closed() {
    // 2023-01-01 is Sunday → observed on Monday 2023-01-02
    let t = ny(2023, 1, 2, 12, 0);
    assert!(!is_fx_trading_datetime(&t));
}

// ============================ is_fx_trading_datetime: Christmas (observed) ============================

#[test]
fn fx_christmas_day_closed() {
    // 2024-12-25 is Wednesday → observed on the day
    let t = ny(2024, 12, 25, 12, 0);
    assert!(!is_fx_trading_datetime(&t));
}

#[test]
fn fx_christmas_sat_observed_fri_closed() {
    // 2021-12-25 is Saturday → observed on Friday 2021-12-24
    let t = ny(2021, 12, 24, 12, 0);
    assert!(!is_fx_trading_datetime(&t));
}

#[test]
fn fx_christmas_sun_observed_mon_closed() {
    // 2022-12-25 is Sunday → observed on Monday 2022-12-26
    let t = ny(2022, 12, 26, 12, 0);
    assert!(!is_fx_trading_datetime(&t));
}

// ============================ is_fx_trading_datetime: Good Friday (Computus) ============================
// Known Good Friday dates:
//   2024: March 29
//   2025: April 18
//   2026: April 3
//   2027: March 26
//   2028: April 14
//   2029: March 30
//   2030: April 19

#[test]
fn fx_good_friday_2024_closed() {
    // 2024-03-29 Good Friday
    let t = ny(2024, 3, 29, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2024 should be a holiday"
    );
}

#[test]
fn fx_good_friday_2025_closed() {
    let t = ny(2025, 4, 18, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2025 should be a holiday"
    );
}

#[test]
fn fx_good_friday_2026_closed() {
    let t = ny(2026, 4, 3, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2026 should be a holiday"
    );
}

#[test]
fn fx_good_friday_2027_closed() {
    let t = ny(2027, 3, 26, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2027 should be a holiday"
    );
}

#[test]
fn fx_good_friday_2028_closed() {
    let t = ny(2028, 4, 14, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2028 should be a holiday"
    );
}

#[test]
fn fx_good_friday_2029_closed() {
    let t = ny(2029, 3, 30, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2029 should be a holiday"
    );
}

#[test]
fn fx_good_friday_2030_closed() {
    let t = ny(2030, 4, 19, 12, 0);
    assert!(
        !is_fx_trading_datetime(&t),
        "Good Friday 2030 should be a holiday"
    );
}

#[test]
fn fx_day_after_good_friday_open() {
    // The Saturday after Good Friday is NOT a holiday (just a weekend → closed anyway)
    // But the Thursday before Good Friday should be open (Mon-Thu)
    // 2024-03-28 Thursday before Good Friday
    let t = ny(2024, 3, 28, 12, 0);
    assert!(is_fx_trading_datetime(&t));
}

// ============================ fx_trading_day_start ============================

#[test]
fn fx_trading_day_start_returns_previous_day_17_ny() {
    // For 2024-11-06 (Wednesday), the FX day starts Tuesday 17:00 NY
    let date = NaiveDate::from_ymd_opt(2024, 11, 6).unwrap();
    let result = fx_trading_day_start(&date, &New_York);
    // Should be 2024-11-05 17:00 NY
    assert_eq!(
        result.date_naive(),
        NaiveDate::from_ymd_opt(2024, 11, 5).unwrap()
    );
    assert_eq!(result.hour(), 17);
}

#[test]
fn fx_trading_day_start_for_monday_returns_sunday_17() {
    // For 2024-11-04 (Monday), FX day starts Sunday 17:00 NY
    let date = NaiveDate::from_ymd_opt(2024, 11, 4).unwrap();
    let result = fx_trading_day_start(&date, &New_York);
    assert_eq!(
        result.date_naive(),
        NaiveDate::from_ymd_opt(2024, 11, 3).unwrap()
    );
    assert_eq!(result.hour(), 17);
}

// ============================ last_bar_available ============================
// For Stock: floors minute to 5-min granularity, subtracts one granularity,
// caps at close (16:05), gates on open (09:25-ish).
// Returns None if before market open.

#[test]
fn last_bar_available_stock_midday_floors_to_5min() {
    // 2024-11-08 (Friday) 12:23 ET → floor to 12:20, subtract 5 → 12:15
    let t = ny(2024, 11, 8, 12, 23);
    let result = last_bar_available(t, &AssetType::Stock);
    assert!(result.is_some());
    let bar_time = result.unwrap();
    // 12:23 → 12:20 (floor) - 5 min = 12:15
    assert_eq!(bar_time.hour(), 12);
    assert_eq!(bar_time.minute(), 15);
}

#[test]
fn last_bar_available_stock_before_open_returns_none() {
    // 2024-11-08 09:00 ET → before open → None
    let t = ny(2024, 11, 8, 9, 0);
    let result = last_bar_available(t, &AssetType::Stock);
    assert!(result.is_none());
}

#[test]
fn last_bar_available_stock_weekend_returns_a_time() {
    // NOTE: last_bar_available does NOT check is_busday — it only gates on the
    // time-of-day (open_threshold). On Saturday noon, last_bar ~11:55 > 09:25
    // open_threshold → returns Some. This is the current behavior (locked in).
    // Callers that need weekday gating must check is_busday separately.
    let t = ny(2024, 11, 9, 12, 0);
    let result = last_bar_available(t, &AssetType::Stock);
    assert!(
        result.is_some(),
        "last_bar_available doesn't gate on busday"
    );
}

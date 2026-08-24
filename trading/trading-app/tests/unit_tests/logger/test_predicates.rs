//! Unit tests for the pure datetime predicates in `logger.rs`.
//!
//! See `src/logger.rs`. Tests cover:
//! - `is_autorestart` — UTC 23:55–00:05 window
//! - `is_stock_open_hard` — NYSE 09:25–16:05 ET on busdays
//! - `is_apac_reset_now` — HKT 04:40–06:10 & 20:10–21:20 windows (±5min lax), weekdays,
//!   plus ET weekend window Fri 23:00–Sat 03:00 (±5min lax)
//! - `is_any_open` — composition of fx + stock

use chrono::{TimeZone, Utc};
use chrono_tz::America::New_York;
use trading_app::test_internals::{is_any_open, is_apac_reset_now, is_autorestart, is_stock_open_hard};

// ============================ is_autorestart ============================

#[test]
fn is_autorestart_22_54_false() {
    let t = Utc.with_ymd_and_hms(2024, 11, 8, 22, 54, 0).unwrap();
    assert!(!is_autorestart(t));
}

#[test]
fn is_autorestart_23_55_true() {
    let t = Utc.with_ymd_and_hms(2024, 11, 8, 23, 55, 0).unwrap();
    assert!(is_autorestart(t));
}

#[test]
fn is_autorestart_23_59_true() {
    let t = Utc.with_ymd_and_hms(2024, 11, 8, 23, 59, 0).unwrap();
    assert!(is_autorestart(t));
}

#[test]
fn is_autorestart_00_00_true() {
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 0, 0, 0).unwrap();
    assert!(is_autorestart(t));
}

#[test]
fn is_autorestart_00_05_true() {
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 0, 5, 0).unwrap();
    assert!(is_autorestart(t));
}

#[test]
fn is_autorestart_00_06_false() {
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 0, 6, 0).unwrap();
    assert!(!is_autorestart(t));
}

#[test]
fn is_autorestart_noon_false() {
    let t = Utc.with_ymd_and_hms(2024, 11, 8, 12, 0, 0).unwrap();
    assert!(!is_autorestart(t));
}

// ============================ is_stock_open_hard ============================
// busday && time > 09:25 && time < 16:05 (in the passed Tz)

#[test]
fn is_stock_open_hard_before_0925_false() {
    // 2024-11-08 is a Friday (busday). 09:00 ET → before 09:25 → false
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 9, 0, 0).unwrap();
    assert!(!is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_at_0925_false() {
    // 09:25 is NOT > 09:25 → false (strict inequality)
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 9, 25, 0).unwrap();
    assert!(!is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_at_0926_true() {
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 9, 26, 0).unwrap();
    assert!(is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_noon_true() {
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 12, 0, 0).unwrap();
    assert!(is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_at_1604_true() {
    // 16:04 < 16:05 → true
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 16, 4, 0).unwrap();
    assert!(is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_at_1605_false() {
    // 16:05 is NOT < 16:05 → false (strict inequality)
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 16, 5, 0).unwrap();
    assert!(!is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_weekend_false() {
    // 2024-11-09 is Saturday → not a busday → false regardless of time
    let t = New_York.with_ymd_and_hms(2024, 11, 9, 12, 0, 0).unwrap();
    assert!(!is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_sunday_false() {
    // 2024-11-10 is Sunday
    let t = New_York.with_ymd_and_hms(2024, 11, 10, 12, 0, 0).unwrap();
    assert!(!is_stock_open_hard(&t));
}

#[test]
fn is_stock_open_hard_nyse_holiday_false() {
    // 2024-11-28 is Thanksgiving (NYSE holiday) → not a busday → false
    let t = New_York.with_ymd_and_hms(2024, 11, 28, 12, 0, 0).unwrap();
    assert!(!is_stock_open_hard(&t));
}

// ============================ is_any_open (composition) ============================

#[test]
fn is_any_open_during_stock_hours_true() {
    let t = New_York.with_ymd_and_hms(2024, 11, 8, 12, 0, 0).unwrap();
    assert!(is_any_open(&t));
}

#[test]
fn is_any_open_weekend_night_false() {
    // Saturday 22:00 ET → stock closed, fx closed (Sat 22:00 ET > 17:00 Friday open cutoff)
    // Actually FX is open Sunday 17:00+ → Saturday is closed.
    let t = New_York.with_ymd_and_hms(2024, 11, 9, 22, 0, 0).unwrap();
    assert!(!is_any_open(&t));
}

// ============================ is_apac_reset_now ============================
// HKT 04:40–06:10 & 20:10–21:20 (±5min lax), Sun–Fri; ET weekend Fri 23:00–Sat 03:00 (±5min)

#[test]
fn is_apac_reset_now_outside_windows_false() {
    // Wednesday 12:00 UTC = Wednesday 20:00 HKT → not in either window → false
    let t = Utc.with_ymd_and_hms(2024, 11, 6, 12, 0, 0).unwrap();
    assert!(!is_apac_reset_now(&t));
}

#[test]
fn is_apac_reset_now_first_window_hkt_0500_true() {
    // Wednesday 05:00 HKT = Tuesday 21:00 UTC (HKT is UTC+8)
    // 21:00 UTC Tuesday = 05:00 HKT Wednesday → in first window (04:40-06:10)
    let t = Utc.with_ymd_and_hms(2024, 11, 5, 21, 0, 0).unwrap();
    assert!(is_apac_reset_now(&t));
}

#[test]
fn is_apac_reset_now_second_window_hkt_2045_true() {
    // Wednesday 20:45 HKT = Wednesday 12:45 UTC → in second window (20:10-21:20)
    let t = Utc.with_ymd_and_hms(2024, 11, 6, 12, 45, 0).unwrap();
    assert!(is_apac_reset_now(&t));
}

#[test]
fn is_apac_reset_now_saturday_no_daily_window_false() {
    // Saturday 05:00 HKT = Friday 21:00 UTC. is_daily_day excludes Saturday.
    // ET: Friday 21:00 UTC = Friday 16:00 ET (not in Fri 23:00 window). → false
    let t = Utc.with_ymd_and_hms(2024, 11, 8, 21, 0, 0).unwrap();
    assert!(!is_apac_reset_now(&t));
}

#[test]
fn is_apac_reset_now_friday_et_weekend_window_true() {
    // Friday 23:00 ET = Saturday 04:00 UTC (in November, ET is UTC-5)
    // 04:00 UTC Saturday = 23:00 ET Friday → in weekend window (Fri 23:00 - 5min = 22:55)
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 4, 0, 0).unwrap();
    assert!(is_apac_reset_now(&t));
}

#[test]
fn is_apac_reset_now_saturday_et_weekend_window_true() {
    // Saturday 02:00 ET = Saturday 07:00 UTC → in weekend window (Sat 03:00 + 5min = 03:05)
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 7, 0, 0).unwrap();
    assert!(is_apac_reset_now(&t));
}

#[test]
fn is_apac_reset_now_saturday_et_after_window_false() {
    // Saturday 04:00 ET = Saturday 09:00 UTC → after 03:05 ET → false
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 9, 0, 0).unwrap();
    assert!(!is_apac_reset_now(&t));
}

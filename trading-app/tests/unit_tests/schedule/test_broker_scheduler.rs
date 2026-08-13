//! Unit tests for `IbkrRegion::schedule` and `IbkrRegion::is_in_maintenance`.
//!
//! See `src/schedule/broker_scheduler.rs`. The maintenance window is
//! Friday 23:00 → Saturday 03:00 Eastern Time (all 3 regions share this schedule).
//! Tests cover the boundary conditions and all weekdays.

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use trading_app::schedule::broker_scheduler::IbkrRegion;

/// Helper: build a UTC DateTime from a naive date + (hour, minute) in Eastern time.
fn et_to_utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
    let naive_date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let naive_time = chrono::NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
    let naive_dt = naive_date.and_time(naive_time);
    New_York
        .from_local_datetime(&naive_dt)
        .single()
        .unwrap()
        .with_timezone(&Utc)
}

const ALL_REGIONS: [IbkrRegion; 3] = [
    IbkrRegion::NorthAmerica,
    IbkrRegion::Europe,
    IbkrRegion::Apac,
];

// ============================ schedule ============================

#[test]
fn schedule_north_america() {
    let w = IbkrRegion::NorthAmerica.schedule();
    assert_eq!(w.start_day, chrono::Weekday::Fri);
    assert_eq!(w.start_time, chrono::NaiveTime::from_hms_opt(23, 0, 0).unwrap());
    assert_eq!(w.end_day, chrono::Weekday::Sat);
    assert_eq!(w.end_time, chrono::NaiveTime::from_hms_opt(3, 0, 0).unwrap());
}

#[test]
fn schedule_europe() {
    // All regions currently share the same schedule
    let w = IbkrRegion::Europe.schedule();
    assert_eq!(w.start_day, chrono::Weekday::Fri);
    assert_eq!(w.end_day, chrono::Weekday::Sat);
}

#[test]
fn schedule_apac() {
    let w = IbkrRegion::Apac.schedule();
    assert_eq!(w.start_day, chrono::Weekday::Fri);
    assert_eq!(w.end_day, chrono::Weekday::Sat);
}

// ============================ is_in_maintenance: Friday ============================

#[test]
fn friday_before_23_et_not_in_maintenance() {
    // Friday 22:59 ET → before 23:00 → not in maintenance
    let t = et_to_utc(2024, 11, 8, 22, 59); // 2024-11-08 is a Friday
    for region in ALL_REGIONS {
        assert!(!region.is_in_maintenance(t), "Friday 22:59 should be false");
    }
}

#[test]
fn friday_at_23_et_in_maintenance() {
    // Friday 23:00 ET → in maintenance
    let t = et_to_utc(2024, 11, 8, 23, 0);
    for region in ALL_REGIONS {
        assert!(region.is_in_maintenance(t), "Friday 23:00 should be true");
    }
}

#[test]
fn friday_late_night_in_maintenance() {
    // Friday 23:59 ET → in maintenance
    let t = et_to_utc(2024, 11, 8, 23, 59);
    assert!(IbkrRegion::Apac.is_in_maintenance(t));
}

// ============================ is_in_maintenance: Saturday ============================

#[test]
fn saturday_early_morning_in_maintenance() {
    // Saturday 02:59 ET → in maintenance
    let t = et_to_utc(2024, 11, 9, 2, 59); // 2024-11-09 is a Saturday
    assert!(IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn saturday_at_03_et_not_in_maintenance() {
    // Saturday 03:00 ET → maintenance ends
    let t = et_to_utc(2024, 11, 9, 3, 0);
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn saturday_noon_not_in_maintenance() {
    let t = et_to_utc(2024, 11, 9, 12, 0);
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

// ============================ is_in_maintenance: other weekdays ============================

#[test]
fn monday_never_in_maintenance() {
    let t = et_to_utc(2024, 11, 4, 12, 0); // 2024-11-04 is a Monday
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn tuesday_never_in_maintenance() {
    let t = et_to_utc(2024, 11, 5, 23, 30);
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn wednesday_never_in_maintenance() {
    let t = et_to_utc(2024, 11, 6, 0, 0);
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn thursday_never_in_maintenance() {
    let t = et_to_utc(2024, 11, 7, 23, 59);
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn sunday_never_in_maintenance() {
    let t = et_to_utc(2024, 11, 10, 1, 0);
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

// ============================ UTC boundary cases ============================

#[test]
fn utc_time_that_maps_to_friday_et() {
    // 2024-11-08 23:00 UTC = 2024-11-08 18:00 ET (Friday) → before 23:00 ET → false
    let t = Utc.with_ymd_and_hms(2024, 11, 8, 23, 0, 0).unwrap();
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn utc_time_that_maps_to_saturday_et() {
    // 2024-11-09 08:00 UTC = 2024-11-09 03:00 EST (Saturday) → maintenance just ended → false
    // (DST: in November, ET is UTC-5)
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 8, 0, 0).unwrap();
    assert!(!IbkrRegion::Apac.is_in_maintenance(t));
}

#[test]
fn utc_time_during_friday_maintenance() {
    // 2024-11-09 04:00 UTC = 2024-11-08 23:00 ET (Friday) → in maintenance
    let t = Utc.with_ymd_and_hms(2024, 11, 9, 4, 0, 0).unwrap();
    assert!(IbkrRegion::Apac.is_in_maintenance(t));
}

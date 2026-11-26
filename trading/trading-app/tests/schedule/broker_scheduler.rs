//! Tests for `src/schedule/broker_scheduler.rs`.
//!
//! Covered (pure, deterministic):
//!   - `IbkrRegion::schedule` (src/schedule/broker_scheduler.rs:37)
//!   - `IbkrRegion::is_in_maintenance` (:47) — exact start/end boundaries,
//!     ET-vs-UTC weekday, EDT/EST offsets, sub-second edges.
//!   - `IbkrStateService` + `BrokerStateChecker` (:72)
//!   - `BrokerScheduler` impls (:83 for `IbkrRegion`, :117 for `IbkrStateService`)
//!
//! NOTE: `IbkrRegion` has no `FromStr`/`Display`/alias parsing in the source
//! (grep for `IbkrRegion` outside this module returns nothing), so there is
//! nothing to test for "parsing/aliases". `MaintenanceWindow` has no `Default`
//! impl; `schedule()` is the only constructor.
//!
//! The two `get_next_*` methods read `Utc::now()` (:85, :97), so they can only
//! be checked through invariants that hold for every `now`; the exact
//! date arithmetic is asserted by the `is_in_maintenance` tables below.

use chrono::{DateTime, Datelike, NaiveTime, TimeDelta, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::{Tz, US::Eastern};

use trading_app::schedule::broker_scheduler::{
    BrokerScheduler, BrokerState, BrokerStateChecker, IbkrRegion, IbkrStateService,
    MaintenanceWindow,
};

// ──────────────────────────── helpers ────────────────────────────

/// Build an exact UTC instant (never derived from the wall clock).
fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid UTC instant")
}

/// Build a UTC instant from an Eastern wall-clock time. Panics on DST gaps /
/// overlaps so a bad fixture cannot silently shift by an hour.
fn et_utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Eastern
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("unambiguous Eastern wall-clock time")
        .to_utc()
}

fn hms(hour: u32, minute: u32, second: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, minute, second).expect("valid naive time")
}

fn regions() -> [IbkrRegion; 3] {
    [
        IbkrRegion::NorthAmerica,
        IbkrRegion::Europe,
        IbkrRegion::Apac,
    ]
}

// ──────────── IbkrRegion::schedule (:37) / MaintenanceWindow ────────────

#[test]
fn schedule_is_fri_2300_to_sat_0300_et_for_every_region() {
    let expected = MaintenanceWindow {
        start_day: Weekday::Fri,
        start_time: hms(23, 0, 0),
        end_day: Weekday::Sat,
        end_time: hms(3, 0, 0),
    };

    for region in regions() {
        let schedule = region.schedule();
        assert_eq!(schedule.start_day, expected.start_day, "{region:?}");
        assert_eq!(schedule.start_time, expected.start_time, "{region:?}");
        assert_eq!(schedule.end_day, expected.end_day, "{region:?}");
        assert_eq!(schedule.end_time, expected.end_time, "{region:?}");
    }
}

/// `MaintenanceWindow`'s fields are public, but nothing consumes an arbitrary
/// window: `is_in_maintenance` re-derives its own via `schedule()` (:52) and
/// hard-codes `Weekday::Fri`/`Weekday::Sat` (:56-57), ignoring `start_day` and
/// `end_day` entirely. This test only pins the public shape of the type.
#[test]
fn maintenance_window_is_a_plain_public_record() {
    let custom = MaintenanceWindow {
        start_day: Weekday::Mon,
        start_time: hms(1, 2, 3),
        end_day: Weekday::Tue,
        end_time: hms(4, 5, 6),
    };
    assert_eq!(custom.start_day, Weekday::Mon);
    assert_eq!(custom.start_time, hms(1, 2, 3));
    assert_eq!(custom.end_day, Weekday::Tue);
    assert_eq!(custom.end_time, hms(4, 5, 6));
}

// ─────────── IbkrRegion::is_in_maintenance (:47) ───────────

/// Window is Fri 23:00 ET (inclusive) → Sat 03:00 ET (exclusive).
/// Exact UTC instants: EST is UTC-5, EDT is UTC-4.
#[test]
fn is_in_maintenance_exact_utc_boundaries() {
    // (label, instant, expected)
    let cases: &[(&str, DateTime<Utc>, bool)] = &[
        // ── EST (UTC-5), week of 2025-01-06 ──
        (
            "Thu 22:59:59 ET — day before, not yet",
            utc(2025, 1, 10, 3, 59, 59),
            false,
        ),
        (
            "Thu 23:00:00 ET — wrong weekday",
            utc(2025, 1, 10, 4, 0, 0),
            false,
        ),
        (
            "Fri 00:00:00 ET — Friday daytime",
            utc(2025, 1, 10, 5, 0, 0),
            false,
        ),
        (
            "Fri 22:59:59 ET — 1s before start",
            utc(2025, 1, 11, 3, 59, 59),
            false,
        ),
        (
            "Fri 23:00:00 ET — start, inclusive",
            utc(2025, 1, 11, 4, 0, 0),
            true,
        ),
        (
            "Fri 23:59:59 ET — last second of Friday",
            utc(2025, 1, 11, 4, 59, 59),
            true,
        ),
        (
            "Sat 00:00:00 ET — crosses midnight",
            utc(2025, 1, 11, 5, 0, 0),
            true,
        ),
        (
            "Sat 02:59:59 ET — 1s before end",
            utc(2025, 1, 11, 7, 59, 59),
            true,
        ),
        (
            "Sat 03:00:00 ET — end, exclusive",
            utc(2025, 1, 11, 8, 0, 0),
            false,
        ),
        (
            "Sat 12:00:00 ET — Saturday daytime",
            utc(2025, 1, 11, 17, 0, 0),
            false,
        ),
        (
            "Sun 02:00:00 ET — Sunday",
            utc(2025, 1, 12, 7, 0, 0),
            false,
        ),
        (
            "Mon 02:00:00 ET — Monday",
            utc(2025, 1, 13, 7, 0, 0),
            false,
        ),
        // ── EDT (UTC-4), week of 2025-07-07 ──
        (
            "Fri 22:59:59 EDT — 1s before start",
            utc(2025, 7, 12, 2, 59, 59),
            false,
        ),
        (
            "Fri 23:00:00 EDT — start, inclusive",
            utc(2025, 7, 12, 3, 0, 0),
            true,
        ),
        (
            "Sat 02:00:00 EDT — mid-window",
            utc(2025, 7, 12, 6, 0, 0),
            true,
        ),
        (
            "Sat 02:59:59 EDT — 1s before end",
            utc(2025, 7, 12, 6, 59, 59),
            true,
        ),
        (
            "Sat 03:00:00 EDT — end, exclusive",
            utc(2025, 7, 12, 7, 0, 0),
            false,
        ),
    ];

    for region in regions() {
        for (label, instant, expected) in cases {
            assert_eq!(
                region.is_in_maintenance(*instant),
                *expected,
                "[{region:?}] {label} ({instant})"
            );
        }
    }
}

/// The window is anchored to the *Eastern* calendar day, so a UTC timestamp
/// whose UTC weekday differs from its ET weekday must follow ET.
#[test]
fn is_in_maintenance_uses_eastern_weekday_not_utc_weekday() {
    // (label, UTC instant, ET weekday it maps to, expected)
    let cases: &[(&str, DateTime<Utc>, Weekday, bool)] = &[
        (
            "Sun 04:30Z = Sat 23:30 ET — after Sat 03:00 end",
            utc(2025, 1, 12, 4, 30, 0),
            Weekday::Sat,
            false,
        ),
        (
            "Mon 04:30Z = Sun 23:30 ET",
            utc(2025, 1, 13, 4, 30, 0),
            Weekday::Sun,
            false,
        ),
        (
            "Fri 04:30Z = Thu 23:30 ET",
            utc(2025, 1, 10, 4, 30, 0),
            Weekday::Thu,
            false,
        ),
        (
            "Sat 04:30Z = Fri 23:30 ET — inside window",
            utc(2025, 1, 11, 4, 30, 0),
            Weekday::Fri,
            true,
        ),
    ];

    for region in regions() {
        for (label, instant, et_weekday, expected) in cases {
            let mapped = instant.with_timezone(&Eastern);
            // Fixture sanity: the UTC and ET weekdays genuinely differ.
            assert_ne!(mapped.weekday(), instant.weekday(), "fixture for {label}");
            assert_eq!(mapped.weekday(), *et_weekday, "fixture for {label}");
            assert_eq!(mapped.time(), hms(23, 30, 0), "fixture for {label}");
            assert_eq!(
                region.is_in_maintenance(*instant),
                *expected,
                "[{region:?}] {label} ({mapped})"
            );
        }
    }
}

/// DST transition weeks (2025: starts Mar 9, ends Nov 2). The window itself
/// never straddles the 02:00 transition, but its UTC offset changes from one
/// week to the next; pin both sides.
#[test]
fn is_in_maintenance_around_dst_transitions() {
    let cases: &[(&str, DateTime<Utc>, bool)] = &[
        // Spring forward on Sun 2025-03-09; the preceding Fri/Sat are still EST.
        (
            "Fri 2025-03-07 23:00 EST",
            utc(2025, 3, 8, 4, 0, 0),
            true,
        ),
        ("Sat 2025-03-08 03:00 EST", utc(2025, 3, 8, 8, 0, 0), false),
        // Fall back on Sun 2025-11-02; the preceding Fri/Sat are still EDT.
        (
            "Fri 2025-10-31 23:00 EDT",
            utc(2025, 11, 1, 3, 0, 0),
            true,
        ),
        ("Sat 2025-11-01 03:00 EDT", utc(2025, 11, 1, 7, 0, 0), false),
        // The following weekend is EST again: same wall clock, +1h UTC.
        (
            "Fri 2025-11-07 23:00 EST",
            utc(2025, 11, 8, 4, 0, 0),
            true,
        ),
        ("Sat 2025-11-08 03:00 EST", utc(2025, 11, 8, 8, 0, 0), false),
    ];

    for region in regions() {
        for (label, instant, expected) in cases {
            assert_eq!(
                region.is_in_maintenance(*instant),
                *expected,
                "[{region:?}] {label} ({instant})"
            );
        }
    }
}

/// Sub-second granularity on both edges.
#[test]
fn is_in_maintenance_subsecond_edges() {
    let start = et_utc(2025, 1, 10, 23, 0, 0);
    let end = et_utc(2025, 1, 11, 3, 0, 0);

    let cases: &[(&str, DateTime<Utc>, bool)] = &[
        (
            "start - 1ns",
            start - TimeDelta::nanoseconds(1),
            false,
        ),
        ("start exactly", start, true),
        ("start + 1ns", start + TimeDelta::nanoseconds(1), true),
        ("end - 1ns", end - TimeDelta::nanoseconds(1), true),
        ("end exactly", end, false),
        ("end + 1ns", end + TimeDelta::nanoseconds(1), false),
    ];

    for region in regions() {
        for (label, instant, expected) in cases {
            assert_eq!(
                region.is_in_maintenance(*instant),
                *expected,
                "[{region:?}] {label} ({instant})"
            );
        }
    }
}

// ─────────── BrokerScheduler for IbkrRegion (:83) ───────────

/// `get_next_broker_unavailable` must always land on a Friday 23:00 ET.
/// It reads `Utc::now()` (:85), so only the invariant is asserted here.
#[test]
fn next_broker_unavailable_is_always_friday_2300_et() {
    for region in regions() {
        let before = Utc::now();
        let next = region
            .get_next_broker_unavailable()
            .expect("Friday 23:00 ET is always representable");
        let after = Utc::now();

        let local = next.with_timezone(&Eastern);
        assert_eq!(local.weekday(), Weekday::Fri, "[{region:?}] {next}");
        assert_eq!(local.time(), hms(23, 0, 0), "[{region:?}] {next}");
        assert_eq!(local.second(), 0, "[{region:?}] {next}");
        assert_eq!(local.nanosecond(), 0, "[{region:?}] {next}");

        // At most one week out. On a Friday at/after 23:00 ET the same-day
        // Friday is returned, which is already in the past — that is only
        // reachable while the broker is unavailable (documented contract:
        // "should only be called when broker is currently unavailable").
        let ahead = next.signed_duration_since(after);
        assert!(
            ahead <= TimeDelta::days(7),
            "[{region:?}] next {next} is more than a week after {after}"
        );
        if next < before {
            assert!(
                region.is_in_maintenance(before),
                "[{region:?}] past result {next} returned at {before} outside maintenance"
            );
        }
    }
}

/// `get_next_broker_available` always resolves to 03:00 ET; the calendar day
/// is "today" unless today is Friday (then Saturday). `Utc::now()` (:97) makes
/// the day part time-dependent, so guard against a midnight crossing by only
/// asserting the day when the wall clock did not roll over during the call.
#[test]
fn next_broker_available_is_always_0300_et() {
    for region in regions() {
        let before = Utc::now();
        let next = region
            .get_next_broker_available()
            .expect("03:00 ET is unambiguous in the US");
        let after = Utc::now();

        let local = next.with_timezone(&Eastern);
        assert_eq!(local.time(), hms(3, 0, 0), "[{region:?}] {next}");
        assert_eq!(local.nanosecond(), 0, "[{region:?}] {next}");

        let weekday_before = before.with_timezone(&Eastern).weekday();
        let weekday_after = after.with_timezone(&Eastern).weekday();
        if weekday_before == weekday_after {
            let expected = if weekday_before == Weekday::Fri {
                Weekday::Sat
            } else {
                weekday_before
            };
            assert_eq!(local.weekday(), expected, "[{region:?}] {next} (now {after})");
        }

        // Same-day 03:00 can be up to ~21h behind; Friday's start can be up to
        // 27h ahead (Fri 00:00 → Sat 03:00).
        let delta = next.signed_duration_since(after);
        assert!(
            delta <= TimeDelta::hours(27) && delta >= TimeDelta::hours(-24),
            "[{region:?}] next {next} is implausibly far from {after} ({delta})"
        );
    }
}

// ─────────── IbkrStateService / BrokerStateChecker (:72) ───────────

/// `get_current_state` is a one-line delegation to
/// `ibkr_region.is_in_maintenance(Utc::now())` (:74-76). Pin that consistency
/// across the (rare) case where the call straddles a window edge.
#[test]
fn state_service_agrees_with_region_for_the_same_instant() {
    for region in regions() {
        let service = IbkrStateService {
            ibkr_region: region.clone(),
        };

        let before = Utc::now();
        let state = service.get_current_state();
        let after = Utc::now();

        let at_before = region.is_in_maintenance(before);
        if at_before == region.is_in_maintenance(after) {
            assert_eq!(
                matches!(state, BrokerState::Unavailable),
                at_before,
                "[{region:?}] at {before}"
            );
        }
    }
}

/// Both `BrokerScheduler` impls delegate to the region, and the trait objects
/// stay object-safe for callers that only hold `&dyn ...`.
#[test]
fn scheduler_and_checker_trait_objects_match_the_region() {
    let region = IbkrRegion::Europe;

    let region_scheduler: &dyn BrokerScheduler = &region;
    assert_eq!(
        region_scheduler
            .get_next_broker_unavailable()
            .expect("Friday 23:00 ET")
            .with_timezone(&Eastern)
            .time(),
        hms(23, 0, 0)
    );
    assert_eq!(
        region_scheduler
            .get_next_broker_available()
            .expect("03:00 ET")
            .with_timezone(&Eastern)
            .time(),
        hms(3, 0, 0)
    );

    let service = IbkrStateService {
        ibkr_region: IbkrRegion::Apac,
    };
    let service_scheduler: &dyn BrokerScheduler = &service;
    assert_eq!(
        service_scheduler
            .get_next_broker_unavailable()
            .expect("Friday 23:00 ET")
            .with_timezone(&Eastern)
            .weekday(),
        Weekday::Fri
    );

    let checker: &dyn BrokerStateChecker = &service;
    let _ = checker.get_current_state();
}

#[test]
fn region_and_service_derives_are_preserved() {
    assert_eq!(format!("{:?}", IbkrRegion::NorthAmerica), "NorthAmerica");
    assert_eq!(format!("{:?}", IbkrRegion::Europe), "Europe");
    assert_eq!(format!("{:?}", IbkrRegion::Apac), "Apac");

    // `BrokerState` deliberately has no derives (src/schedule/broker_scheduler.rs:12);
    // it is only usable through `matches!`.
    let state = BrokerState::Available;
    assert!(matches!(state, BrokerState::Available));
    assert!(!matches!(state, BrokerState::Unavailable));

    let cloned = IbkrRegion::Europe.clone();
    assert_eq!(cloned.schedule().start_time, hms(23, 0, 0));

    let service = IbkrStateService {
        ibkr_region: IbkrRegion::NorthAmerica,
    };
    let service_clone = service.clone();
    assert!(format!("{service_clone:?}").contains("NorthAmerica"));
}

/// Compile-time guard: `Utc` is what `is_in_maintenance` accepts, `Tz` is what
/// the scheduler returns. Keeps the two conversions exercised explicitly.
#[test]
fn scheduler_return_type_is_tz_aware() {
    let next: DateTime<Tz> = IbkrRegion::Apac
        .get_next_broker_unavailable()
        .expect("Friday 23:00 ET");
    let as_utc: DateTime<Utc> = next.to_utc();
    assert_eq!(as_utc.timestamp(), next.timestamp());
}

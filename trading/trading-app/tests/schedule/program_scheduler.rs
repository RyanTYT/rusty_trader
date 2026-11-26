//! Tests for `src/schedule/program_scheduler.rs`.
//!
//! Covered:
//!   - `AppReturnState` (:45) — every variant is constructible and its payload
//!     round-trips through a match. The enum has no derives (no
//!     `Debug`/`Clone`/`PartialEq`) and no `Default` impl.
//!
//! NOT covered — blockers:
//!   - `AppReturnState::log_state` (:58) and `is_terminal_state` (:95) are
//!     private. `is_terminal_state` is the only thing that decides whether
//!     `run_program` breaks its `'outer` loop (:237-239), so its two-variant
//!     truth table cannot be asserted from an integration test.
//!   - `sleep_until` (:27) is private and reads `Utc::now()`.
//!   - `run_program` (:103) cannot be driven offline:
//!       * installs real SIGTERM/SIGINT handlers (:116-117) and only exits on
//!         a signal or a broker-state transition;
//!       * calls `IbkrStateService::get_current_state` (:120) → `Utc::now()`;
//!       * in the `Available` arm calls `with_gateway_retry("/tmp/ibc.log", 3, ..)`
//!         (:140), which spawns/awaits an IB Gateway via `IBGateway::start`
//!         (src/ibc.rs:398) and writes a log file;
//!       * the closure calls `init_application()` which builds
//!         `ApplicationState` (src/init_app.rs:94) — DB + IB + strategy wiring.
//!     The init closure is only invoked *inside* the gateway callback
//!     (src/ibc.rs:414), so no fake future can be substituted before IBC
//!     is started.

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

use trading_app::schedule::program_scheduler::AppReturnState;

fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .expect("valid UTC instant")
}

/// All nine variants (:46-54), including payload preservation.
#[test]
fn app_return_state_variants_are_constructible_and_distinct() {
    let cases: Vec<(&str, AppReturnState)> = vec![
        ("BrokerDown", AppReturnState::BrokerDown),
        (
            "InitAppErr",
            AppReturnState::InitAppErr("db connect refused".to_string()),
        ),
        (
            "NoBrokerSchedule",
            AppReturnState::NoBrokerSchedule("Date overflow".to_string()),
        ),
        (
            "UnstableConnMktHours",
            AppReturnState::UnstableConnMktHours,
        ),
        (
            "UnstableConnOutsideHours",
            AppReturnState::UnstableConnOutsideHours(
                utc(2025, 1, 11, 3, 0).with_timezone(&Tz::US__Eastern),
            ),
        ),
        (
            "UnstableConnBrokenPipe",
            AppReturnState::UnstableConnBrokenPipe,
        ),
        ("UnstableConnAPAC", AppReturnState::UnstableConnAPAC),
        (
            "SigintTerminalSignal",
            AppReturnState::SigintTerminalSignal,
        ),
        (
            "SigtermTerminalSignal",
            AppReturnState::SigtermTerminalSignal,
        ),
    ];

    assert_eq!(cases.len(), 9, "AppReturnState has nine variants");
    for (label, state) in cases {
        // Every arm below must be reachable: panic with the label if not.
        let kind = match state {
            AppReturnState::BrokerDown => "BrokerDown",
            AppReturnState::InitAppErr(_) => "InitAppErr",
            AppReturnState::NoBrokerSchedule(_) => "NoBrokerSchedule",
            AppReturnState::UnstableConnMktHours => "UnstableConnMktHours",
            AppReturnState::UnstableConnOutsideHours(_) => "UnstableConnOutsideHours",
            AppReturnState::UnstableConnBrokenPipe => "UnstableConnBrokenPipe",
            AppReturnState::UnstableConnAPAC => "UnstableConnAPAC",
            AppReturnState::SigintTerminalSignal => "SigintTerminalSignal",
            AppReturnState::SigtermTerminalSignal => "SigtermTerminalSignal",
        };
        assert_eq!(kind, label);
    }
}

/// String payloads are preserved verbatim (including the empty string).
#[test]
fn error_payloads_round_trip_verbatim() {
    for payload in ["", "boom", "Date overflow", "couldn't convert time_zone_id: X"] {
        match AppReturnState::InitAppErr(payload.to_string()) {
            AppReturnState::InitAppErr(got) => assert_eq!(got, payload),
            _ => panic!("wrong variant for InitAppErr"),
        }
        match AppReturnState::NoBrokerSchedule(payload.to_string()) {
            AppReturnState::NoBrokerSchedule(got) => assert_eq!(got, payload),
            _ => panic!("wrong variant for NoBrokerSchedule"),
        }
    }
}

/// `UnstableConnOutsideHours(DateTime<Tz>)` carries the *Tz-aware* instant
/// produced by `ConnectionAlert` (see program_scheduler.rs:186-189). The UTC
/// value must not be normalized away.
#[test]
fn unstable_conn_outside_hours_keeps_its_tz_datetime() {
    // 2025-01-11T04:00:00Z is Fri 2025-01-10 23:00 EST.
    let et: DateTime<Tz> = utc(2025, 1, 11, 4, 0).with_timezone(&Tz::US__Eastern);
    assert_eq!(et.to_utc(), utc(2025, 1, 11, 4, 0));
    assert_eq!(et.hour(), 23);
    assert_eq!(et.day(), 10);

    match AppReturnState::UnstableConnOutsideHours(et) {
        AppReturnState::UnstableConnOutsideHours(got) => {
            assert_eq!(got, et);
            assert_eq!(got.timezone(), et.timezone());
            assert_eq!(got.to_utc(), et.to_utc());
            assert_eq!(got.timestamp(), et.timestamp());
        }
        _ => panic!("wrong variant for UnstableConnOutsideHours"),
    }
}

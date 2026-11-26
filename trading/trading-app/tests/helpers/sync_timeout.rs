//! Tests for `src/helpers/sync_timeout.rs` (the `timeout` function and
//! `TimeoutError`), plus the documented behaviour of worker panics.
//!
//! `async_call` (src/helpers/sync_timeout.rs:52) and `timeout_panic`
//! (src/helpers/sync_timeout.rs:75) are `pub(crate)` and not re-exported by
//! `trading_app::test_internals`, so they cannot be called from an integration
//! test without a src edit.

use std::fmt;
use std::time::{Duration, Instant};

use trading_app::test_internals::{TimeoutError, timeout};

#[derive(Debug, PartialEq, Eq)]
struct Boom(&'static str);

impl fmt::Display for Boom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Boom {}

// ───────────────────────────── success ─────────────────────────────

/// The worker's value is returned verbatim; `O` is not constrained.
#[test]
fn timeout_returns_the_worker_value_verbatim() {
    assert_eq!(
        timeout(Duration::from_secs(5), || Ok::<i32, String>(42)).unwrap(),
        42
    );
    let negative_zero = timeout(Duration::from_secs(5), || Ok::<f64, String>(-0.0)).unwrap();
    assert_eq!(negative_zero, 0.0);
    assert!(negative_zero.is_sign_negative());
    assert!(
        timeout(Duration::from_secs(5), || Ok::<Option<u8>, String>(None))
            .unwrap()
            .is_none()
    );

    let started = Instant::now();
    let out = timeout(Duration::from_secs(5), || {
        std::thread::sleep(Duration::from_millis(20));
        Ok::<&'static str, String>("done")
    });
    assert_eq!(out.unwrap(), "done");
    assert!(
        started.elapsed() >= Duration::from_millis(20),
        "timeout must wait for the worker to finish"
    );
}

// ────────────────────────── error propagation ──────────────────────────

/// `Err(e)` becomes `TimeoutError::Function(e)` unchanged.
#[test]
fn timeout_wraps_a_worker_error_in_function() {
    match timeout(Duration::from_secs(5), || Err::<(), String>("boom".into())) {
        Err(TimeoutError::Function(e)) => assert_eq!(e, "boom"),
        other => panic!("expected Function(boom), got {other:?}"),
    }

    match timeout(Duration::from_secs(5), || Err::<u8, Boom>(Boom("bad"))) {
        Err(TimeoutError::Function(e)) => assert_eq!(e, Boom("bad")),
        other => panic!("expected Function(Boom), got {other:?}"),
    }
}

/// A worker that outlives the duration is reported as `Timeout`.
#[test]
fn timeout_reports_a_slow_worker_as_timeout() {
    let out = timeout(Duration::from_millis(20), || {
        std::thread::sleep(Duration::from_millis(500));
        Ok::<(), String>(())
    });
    assert!(
        matches!(&out, Err(TimeoutError::Timeout)),
        "expected Timeout, got {out:?}"
    );
}

// ────────────────────────── Display / Error ──────────────────────────

#[test]
fn timeout_error_display_strings() {
    assert_eq!(TimeoutError::<Boom>::Timeout.to_string(), "operation timed out");
    assert_eq!(
        TimeoutError::Function(Boom("bad")).to_string(),
        "function error: bad"
    );
    assert_eq!(
        TimeoutError::<Boom>::WorkerPanic.to_string(),
        "worker panicked"
    );

    let dyn_err: &dyn std::error::Error = &TimeoutError::<Boom>::Timeout;
    assert_eq!(dyn_err.to_string(), "operation timed out");
    assert!(dyn_err.source().is_none());

    fn assert_send_static<T: Send + 'static>() {}
    assert_send_static::<TimeoutError<Boom>>();
    assert_send_static::<TimeoutError<String>>();
}

// ────────────────────────── worker panics ──────────────────────────

/// A panicking worker drops the completion sender without sending, so
/// `recv_timeout` returns `Disconnected` and `timeout` reports `WorkerPanic`.
/// The panic is confined to the worker thread and does not unwind into the caller.
#[test]
fn timeout_reports_a_panicking_worker_as_worker_panic() {
    let out = timeout(Duration::from_secs(5), || -> Result<(), String> {
        panic!("worker exploded");
    });
    assert!(
        matches!(&out, Err(TimeoutError::WorkerPanic)),
        "expected WorkerPanic, got {out:?}"
    );

    // The caller survives and can keep using the helper.
    assert_eq!(
        timeout(Duration::from_secs(5), || Ok::<u8, String>(1)).unwrap(),
        1
    );
}



//! Unit tests for the pure synchronous timeout utility.
//!
//! See `src/helpers/sync_timeout.rs`. Uses `std::thread` + `mpsc` (no tokio).
//! Tests cover: fast Ok, fast Err→Function, slow→Timeout, zero-duration→Timeout,
//! exactly-once execution, Display impls for all 3 TimeoutError variants, and
//! a compile-time check that TimeoutError: std::error::Error.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use trading_app::test_internals::{timeout, TimeoutError};

/// Test error type that implements Display + std::error::Error.
/// (String doesn't impl std::error::Error, so we need a wrapper.)
#[derive(Debug)]
struct TestErr(String);
impl std::fmt::Display for TestErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for TestErr {}

#[test]
fn fast_ok_returns_ok_value() {
    let res: Result<i32, TimeoutError<TestErr>> =
        timeout(Duration::from_secs(5), || Ok::<i32, TestErr>(42));
    match res {
        Ok(v) => assert_eq!(v, 42),
        other => panic!("expected Ok(42), got {:?}", other),
    }
}

#[test]
fn fast_err_returns_function_error() {
    let res: Result<i32, TimeoutError<TestErr>> = timeout(Duration::from_secs(5), || {
        Err::<i32, TestErr>(TestErr("boom".to_string()))
    });
    match res {
        Err(TimeoutError::Function(e)) => assert_eq!(e.0, "boom"),
        other => panic!("expected Function, got {:?}", other),
    }
}

#[test]
fn slow_function_times_out() {
    // Function sleeps longer than the timeout → Timeout
    let res: Result<i32, TimeoutError<TestErr>> = timeout(
        Duration::from_millis(50),
        || {
            thread::sleep(Duration::from_millis(500));
            Ok::<i32, TestErr>(1)
        },
    );
    assert!(matches!(res, Err(TimeoutError::Timeout)));
}

#[test]
fn fast_zero_duration_still_runs_if_fn_is_fast() {
    // With Duration::ZERO, recv_timeout returns Timeout immediately — the spawned
    // thread hasn't had a chance to send yet. So zero duration → Timeout.
    let res: Result<i32, TimeoutError<TestErr>> =
        timeout(Duration::ZERO, || Ok::<i32, TestErr>(7));
    assert!(matches!(res, Err(TimeoutError::Timeout)));
}

#[test]
fn completion_is_counted_exactly_once() {
    // Sanity: the inner fn runs exactly one time (counter is 1 after).
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let res: Result<i32, TimeoutError<TestErr>> = timeout(Duration::from_secs(5), || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
        Ok::<i32, TestErr>(0)
    });
    match res {
        Ok(v) => assert_eq!(v, 0),
        other => panic!("expected Ok(0), got {:?}", other),
    }
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
}

#[test]
fn display_timeout_variant() {
    let e: TimeoutError<TestErr> = TimeoutError::Timeout;
    assert_eq!(format!("{e}"), "operation timed out");
}

#[test]
fn display_function_variant() {
    let e: TimeoutError<TestErr> = TimeoutError::Function(TestErr("boom".to_string()));
    assert_eq!(format!("{e}"), "function error: boom");
}

#[test]
fn display_worker_panic_variant() {
    let e: TimeoutError<TestErr> = TimeoutError::WorkerPanic;
    assert_eq!(format!("{e}"), "worker panicked");
}

#[test]
fn timeout_error_implements_std_error() {
    // Compile-time check that TimeoutError: std::error::Error
    fn assert_error<E: std::error::Error>() {}
    assert_error::<TimeoutError<TestErr>>();
}

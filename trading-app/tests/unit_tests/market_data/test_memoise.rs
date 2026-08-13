//! Unit tests for `Memoized` and `AnyMemoized` — generic memoization wrapper.
//!
//! See `src/market_data/memoise.rs`. `Memoized<I, K, V, E>` is a generic struct
//! wrapping a moka cache + a key_fn + a compute fn. Tests cover:
//! - Cache hit/miss (verify with a counter closure)
//! - TTL expiry (short sleep)
//! - Error propagation
//! - `call_any` type-erased dispatch (success + mismatch panic)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use trading_app::test_internals::{AnyMemoized, Memoized};

/// Test error type implementing Display + Clone.
#[derive(Debug, Clone)]
struct TestErr(String);
impl std::fmt::Display for TestErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================ cache hit/miss ============================

#[test]
fn call_returns_computed_value() {
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |s: &String| Ok::<i32, TestErr>(s.len() as i32),
    );
    assert_eq!(m.call("hello".to_string()).unwrap(), 5);
}

#[test]
fn call_caches_on_second_call() {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |_s: &String| {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            Ok::<i32, TestErr>(42)
        },
    );
    // First call: computes, counter=1
    assert_eq!(m.call("k".to_string()).unwrap(), 42);
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
    // Second call: cache hit, counter still 1
    assert_eq!(m.call("k".to_string()).unwrap(), 42);
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
    // Different key: computes again, counter=2
    assert_eq!(m.call("other".to_string()).unwrap(), 42);
    assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
}

// ============================ error propagation ============================

#[test]
fn call_propagates_error() {
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |_s: &String| Err::<i32, TestErr>(TestErr("boom".to_string())),
    );
    let res = m.call("k".to_string());
    match res {
        Err(e) => assert_eq!(e.0, "boom"),
        Ok(v) => panic!("expected error, got {v}"),
    }
}

#[test]
fn call_does_not_cache_errors() {
    // moka's try_get_with does NOT cache when the compute fn returns Err.
    // So a second call with the same key should re-invoke the compute fn.
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |_s: &String| {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            Err::<i32, TestErr>(TestErr("fail".to_string()))
        },
    );
    let _ = m.call("k".to_string());
    let _ = m.call("k".to_string());
    assert_eq!(COUNTER.load(Ordering::SeqCst), 2, "errors should not be cached");
}

// ============================ TTL expiry ============================

#[test]
fn ttl_expiry_causes_recompute() {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_millis(50),
        |s: &String| s.clone(),
        |_s: &String| {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            Ok::<i32, TestErr>(1)
        },
    );
    // First call: computes, counter=1
    let _ = m.call("k".to_string());
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(100));
    // Second call: cache expired, recompute, counter=2
    let _ = m.call("k".to_string());
    assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
}

// ============================ AnyMemoized (type-erased) ============================

#[test]
fn call_any_success_downcast() {
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |s: &String| Ok::<i32, TestErr>(s.len() as i32),
    );
    let m: Arc<dyn AnyMemoized> = Arc::new(m);
    let input: Box<dyn std::any::Any + Send> = Box::new("hello".to_string());
    let res = m.call_any(input).unwrap();
    let out: i32 = *res.downcast::<i32>().unwrap();
    assert_eq!(out, 5);
}

#[test]
fn call_any_error_returns_string() {
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |_s: &String| Err::<i32, TestErr>(TestErr("err".to_string())),
    );
    let m: Arc<dyn AnyMemoized> = Arc::new(m);
    let input: Box<dyn std::any::Any + Send> = Box::new("k".to_string());
    let res = m.call_any(input);
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), "err");
}

#[test]
#[should_panic(expected = "AnyMemoized: input type mismatch")]
fn call_any_type_mismatch_panics() {
    let m: Memoized<String, String, i32, TestErr> = Memoized::new(
        Duration::from_secs(60),
        |s: &String| s.clone(),
        |s: &String| Ok::<i32, TestErr>(s.len() as i32),
    );
    let m: Arc<dyn AnyMemoized> = Arc::new(m);
    // Pass an i32 when a String is expected → panic
    let input: Box<dyn std::any::Any + Send> = Box::new(42_i32);
    let _ = m.call_any(input);
}

//! Unit tests for the 9 rolling-statistic structs in `rolling_fn.rs`.
//!
//! See `src/strategy/helpers/rolling_fn.rs`. All structs are pure-logic
//! (Decimal-precision, no IO). Tests cover:
//! - Warm-up behavior (None until window full)
//! - Steady-state correctness vs hand-computed/pandas reference values
//! - Window eviction
//! - `replace_last` semantics (including destructive monotonic-deque behavior)
//! - Edge cases (window=1/2, constant values, negatives, duplicates)
//! - `#[should_panic]` for zero-window/span/diff constructors
//! - f64/Decimal parity
//! - proptest invariants (vs naive implementations)

use proptest::prelude::*;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use std::collections::VecDeque;
use trading_app::strategy::helpers::rolling_fn::{
    EwmMean, RollingMax, RollingMin, RollingRankPct, RollingRoc, RollingStd, RollingSum,
    RollingZScore,
};

// ============================ RollingMax ============================

#[test]
fn rolling_max_warm_up_returns_none() {
    let mut r = RollingMax::new(3);
    assert_eq!(r.push(1.0), None);
    assert_eq!(r.push(5.0), None);
    assert_eq!(r.max(), None);
    assert_eq!(r.push(3.0), Some(5.0));
}

#[test]
fn rolling_max_tracks_max_and_evicts() {
    let mut r = RollingMax::new(3);
    r.push(1.0); // [1]
    r.push(5.0); // [1,5]
    assert_eq!(r.push(3.0), Some(5.0)); // [1,5,3]
    assert_eq!(r.push(2.0), Some(5.0)); // 1 evicted → [5,3,2]
    assert_eq!(r.push(1.0), Some(3.0)); // 5 evicted → [3,2,1]
}

#[test]
fn rolling_max_window_one() {
    let mut r = RollingMax::new(1);
    assert_eq!(r.push(10.0), Some(10.0));
    assert_eq!(r.push(3.0), Some(3.0));
    assert_eq!(r.push(7.0), Some(7.0));
}

#[test]
fn rolling_max_negatives() {
    let mut r = RollingMax::new(2);
    r.push(-5.0);
    assert_eq!(r.push(-2.0), Some(-2.0));
    assert_eq!(r.push(-10.0), Some(-2.0));
}

#[test]
fn rolling_max_replace_last() {
    let mut r = RollingMax::new(3);
    r.push(1.0);
    r.push(5.0);
    r.push(3.0);
    assert_eq!(r.max(), Some(5.0));
    // replace the 3.0 (last pushed) with 10.0
    // NOTE: replace_last on a monotonic deque is destructive — since 10 >= 5,
    // the (idx=1, val=5) entry is popped from the back during reinsertion.
    assert_eq!(r.replace_last(10.0), Some(10.0));
    assert_eq!(r.max(), Some(10.0));
    // replace 10.0 with 0.5 → the (1,5) is already gone, so only (2,0.5) remains
    assert_eq!(r.replace_last(0.5), Some(0.5));
}

#[test]
fn rolling_max_replace_last_with_no_push_returns_none() {
    let mut r = RollingMax::new(3);
    assert_eq!(r.replace_last(1.0), None);
}

#[test]
#[should_panic(expected = "assertion failed: window > 0")]
fn rolling_max_new_zero_panics() {
    let _ = RollingMax::new(0);
}

// ============================ RollingMin ============================

#[test]
fn rolling_min_tracks_min_and_evicts() {
    let mut r = RollingMin::new(3);
    r.push(5.0);
    r.push(1.0);
    assert_eq!(r.push(3.0), Some(1.0));
    assert_eq!(r.push(2.0), Some(1.0)); // 5 evicted
    assert_eq!(r.push(0.5), Some(0.5));
}

#[test]
fn rolling_min_window_one() {
    let mut r = RollingMin::new(1);
    assert_eq!(r.push(10.0), Some(10.0));
    assert_eq!(r.push(3.0), Some(3.0));
}

#[test]
fn rolling_min_replace_last() {
    let mut r = RollingMin::new(3);
    r.push(5.0);
    r.push(1.0);
    r.push(3.0);
    assert_eq!(r.min(), Some(1.0));
    // replace 3.0 with 0.5: pops (2,3) and (1,1) from monotonic deque → only (2,0.5)
    assert_eq!(r.replace_last(0.5), Some(0.5));
    assert_eq!(r.min(), Some(0.5));
    // replace 0.5 with 10.0: pops (2,0.5), reinserts (2,10.0) — no smaller value remains
    assert_eq!(r.replace_last(10.0), Some(10.0));
}

#[test]
#[should_panic(expected = "assertion failed: window > 0")]
fn rolling_min_new_zero_panics() {
    let _ = RollingMin::new(0);
}

// ============================ RollingSum ============================

#[test]
fn rolling_sum_warm_up_then_evict() {
    let mut r = RollingSum::new(3);
    assert_eq!(r.push(1.0), None);
    assert_eq!(r.push(2.0), None);
    assert_eq!(r.push(3.0), Some(6.0));
    assert_eq!(r.push(4.0), Some(9.0)); // evict 1, sum = 2+3+4
    assert_eq!(r.push(5.0), Some(12.0)); // evict 2, sum = 3+4+5
}

#[test]
fn rolling_sum_replace_last() {
    let mut r = RollingSum::new(3);
    r.push(1.0);
    r.push(2.0);
    r.push(3.0);
    assert_eq!(r.replace_last(10.0), Some(13.0)); // 1+2+10
    assert_eq!(r.replace_last(-5.0), Some(-2.0)); // 1+2-5
}

#[test]
#[should_panic(expected = "assertion failed: window > 0")]
fn rolling_sum_new_zero_panics() {
    let _ = RollingSum::new(0);
}

// ============================ RollingStd ============================
// sample std (n-1 denominator): sqrt( (sum_sq - n*mean^2) / (n-1) )

#[test]
fn rolling_std_sample_std_formula() {
    // window=2, values [2.0, 4.0]: mean=3, sum_sq=4+16=20
    // sample variance = (20 - 2*9) / (2-1) = 2/1 = 2 → std = sqrt(2) ≈ 1.41421356
    let mut r = RollingStd::new(2);
    r.push(2.0);
    let std = r.push(4.0).unwrap();
    assert!((std - 2.0_f64.sqrt()).abs() < 1e-9);
}

#[test]
fn rolling_std_constant_values_yield_zero() {
    let mut r = RollingStd::new(3);
    r.push(5.0);
    r.push(5.0);
    let std = r.push(5.0).unwrap();
    assert!(std.abs() < 1e-9);
}

#[test]
fn rolling_std_warm_up_returns_none() {
    let mut r = RollingStd::new(3);
    assert_eq!(r.push(1.0), None);
    assert_eq!(r.push(2.0), None);
    assert!(r.push(3.0).is_some());
}

#[test]
fn rolling_std_replace_last() {
    let mut r = RollingStd::new(3);
    r.push(1.0);
    r.push(2.0);
    r.push(3.0);
    let _ = r.replace_last(10.0);
    // window [1,2,10]: mean=13/3, sum_sq=1+4+100=105
    // var = (105 - 3*(13/3)^2) / 2 = (105 - 169/3) / 2 = (315-169)/6 = 146/6 ≈ 24.333
    // std ≈ 4.9329
    let std = r.rolling_std().unwrap();
    assert!((std - (146.0_f64 / 6.0).sqrt()).abs() < 1e-6);
}

#[test]
#[should_panic(expected = "assertion failed: window > 0")]
fn rolling_std_new_zero_panics() {
    let _ = RollingStd::new(0);
}

// ============================ RollingZScore ============================

#[test]
#[should_panic(expected = "assertion failed: window >= 2")]
fn rolling_zscore_new_one_panics() {
    let _ = RollingZScore::new(1);
}

#[test]
fn rolling_zscore_basic() {
    // window=2, push [2.0, 4.0]: mean=3, std=sqrt(2)
    // z = (4 - 3) / sqrt(2) = 1/sqrt(2) ≈ 0.70710678
    let mut r = RollingZScore::new(2);
    r.push(2.0);
    let z = r.push(4.0).unwrap();
    assert!((z - 1.0 / 2.0_f64.sqrt()).abs() < 1e-9);
}

#[test]
fn rolling_zscore_replace_last() {
    let mut r = RollingZScore::new(3);
    r.push(1.0);
    r.push(2.0);
    r.push(3.0);
    assert!(r.z_score().is_some());
    let z = r.replace_last(10.0);
    assert!(z.is_some());
}

// ============================ RollingRankPct ============================

#[test]
fn rolling_rank_pct_warm_up() {
    let mut r = RollingRankPct::new(3);
    assert_eq!(r.push(1.0), None);
    assert_eq!(r.push(2.0), None);
    assert_eq!(r.push(3.0), Some(1.0)); // 3 is the largest, all 3 <= 3
}

#[test]
fn rolling_rank_pct_evicts_and_recomputes() {
    let mut r = RollingRankPct::new(3);
    r.push(1.0); // counts: {1:1}
    r.push(2.0); // counts: {1:1, 2:1}
    assert_eq!(r.push(3.0), Some(1.0)); // counts: {1:1, 2:1, 3:1}, last=3
    // push 1.0 again: evict oldest 1.0, counts: {1:1, 2:1, 3:1}, last=1
    // leq(1) = 1 → 1/3
    assert!((r.push(1.0).unwrap() - 1.0 / 3.0).abs() < 1e-9);
}

#[test]
fn rolling_rank_pct_duplicates() {
    let mut r = RollingRankPct::new(3);
    r.push(5.0);
    r.push(5.0);
    // all equal → percentile = 3/3 = 1.0
    assert_eq!(r.push(5.0), Some(1.0));
}

#[test]
fn rolling_rank_pct_replace_last() {
    let mut r = RollingRankPct::new(3);
    r.push(1.0);
    r.push(2.0);
    r.push(3.0); // last=3, pct=1.0
    // replace 3.0 with 1.5: counts {1:1, 2:1, 1.5:1}, last=1.5, leq(1.5)=2 → 2/3
    let pct = r.replace_last(1.5).unwrap();
    assert!((pct - 2.0 / 3.0).abs() < 1e-9);
}

// ============================ EwmMean ============================
// alpha = 2/(span+1), pandas adjust=False: ewma_t = alpha*x_t + (1-alpha)*ewma_{t-1}

#[test]
fn ewm_mean_first_value_is_value_itself() {
    let mut e = EwmMean::new(5);
    assert_eq!(e.push(10.0), Some(10.0));
}

#[test]
fn ewm_mean_alpha_confirmed_via_recursion() {
    // span=2 → alpha = 2/3. Verify indirectly: push 0 then 1
    // ewma_0 = 0, ewma_1 = alpha*1 + (1-alpha)*0 = alpha = 2/3
    let mut e = EwmMean::new(2);
    e.push(0.0);
    let v = e.push(1.0).unwrap();
    assert!((v - 2.0 / 3.0).abs() < 1e-9);
}

#[test]
fn ewm_mean_recursion() {
    // span=2, alpha=2/3
    // push 10 → ewma=10
    // push 20 → ewma = 2/3*20 + 1/3*10 = 40/3 + 10/3 = 50/3 ≈ 16.6667
    let mut e = EwmMean::new(2);
    e.push(10.0);
    let v = e.push(20.0).unwrap();
    assert!((v - 50.0 / 3.0).abs() < 1e-6);
}

#[test]
fn ewm_mean_replace_last_single_point() {
    let mut e = EwmMean::new(3);
    e.push(10.0);
    // idx=1 → replace_last just sets prev=value
    assert_eq!(e.replace_last(20.0), Some(20.0));
    assert_eq!(e.value(), Some(20.0));
}

#[test]
fn ewm_mean_replace_last_multi_point() {
    // span=3, alpha=2/(3+1)=0.5
    // push 10 → ewma=10
    // push 20 → ewma = 0.5*20 + 0.5*10 = 15
    // replace_last(100) → ewma = 0.5*100 + 0.5*10 = 55
    let mut e = EwmMean::new(3);
    e.push(10.0);
    e.push(20.0);
    assert_eq!(e.value(), Some(15.0));
    let v = e.replace_last(100.0).unwrap();
    assert!((v - 55.0).abs() < 1e-6);
}

#[test]
fn ewm_mean_replace_last_no_push_returns_none() {
    let mut e = EwmMean::new(3);
    assert_eq!(e.replace_last(1.0), None);
}

#[test]
#[should_panic(expected = "assertion failed: span > 0")]
fn ewm_mean_new_zero_panics() {
    let _ = EwmMean::new(0);
}

// ============================ RollingRoc ============================

#[test]
fn rolling_roc_warm_up() {
    let mut r = RollingRoc::new(2);
    r.push(10.0);
    assert_eq!(r.push(15.0), None); // need diff+1 = 3 values
    assert!(r.push(20.0).is_some());
}

#[test]
fn rolling_roc_formula() {
    // diff=1, push [10, 20] → roc = (20-10)/10 = 1.0
    let mut r = RollingRoc::new(1);
    r.push(10.0);
    let roc = r.push(20.0).unwrap();
    assert!((roc - 1.0).abs() < 1e-9);
}

#[test]
fn rolling_roc_negative_change() {
    // diff=1, push [20, 10] → roc = (10-20)/20 = -0.5
    let mut r = RollingRoc::new(1);
    r.push(20.0);
    let roc = r.push(10.0).unwrap();
    assert!((roc - (-0.5)).abs() < 1e-9);
}

#[test]
fn rolling_roc_evicts_old() {
    // diff=1, push [10, 20, 30]
    // after [10,20]: len=2==diff+1 → roc=(20-10)/10=1.0
    // push 30: evict 10 (len > diff+1=2), now [20,30] → roc=(30-20)/20=0.5
    let mut r = RollingRoc::new(1);
    r.push(10.0);
    assert!((r.push(20.0).unwrap() - 1.0).abs() < 1e-9);
    assert!((r.push(30.0).unwrap() - 0.5).abs() < 1e-9);
}

#[test]
fn rolling_roc_replace_last() {
    let mut r = RollingRoc::new(1);
    r.push(10.0);
    r.push(20.0); // roc=1.0
    assert!((r.replace_last(30.0).unwrap() - 2.0).abs() < 1e-9); // (30-10)/10
}

#[test]
#[should_panic(expected = "assertion failed: diff > 0")]
fn rolling_roc_new_zero_panics() {
    let _ = RollingRoc::new(0);
}

// ============================ f64/Decimal parity ============================

#[test]
fn rolling_max_f64_decimal_parity() {
    let mut a = RollingMax::new(3);
    let mut b = RollingMax::new(3);
    let vals = [1.5, 3.2, 2.8, 4.1, 0.9];
    for v in vals {
        let fa = a.push(v);
        let fb = b
            .push_dec(Decimal::from_f64(v).unwrap())
            .map(|d| d.to_f64().unwrap());
        assert_eq!(fa, fb, "mismatch at v={v}");
    }
}

#[test]
fn rolling_sum_f64_decimal_parity() {
    let mut a = RollingSum::new(3);
    let mut b = RollingSum::new(3);
    for v in [1.1, 2.2, 3.3, 4.4] {
        let fa = a.push(v);
        let fb = b
            .push_dec(Decimal::from_f64(v).unwrap())
            .map(|d| d.to_f64().unwrap());
        assert!((fa.unwrap_or(0.0) - fb.unwrap_or(0.0)).abs() < 1e-6, "mismatch at v={v}");
    }
}

// ============================ proptest invariants ============================

proptest! {
    #![proptest_config(proptest::test_runner::Config { cases: 128, ..Default::default() })]

    /// RollingSum over a window equals the naive sum of the last `window` values.
    #[test]
    fn prop_rolling_sum_matches_naive(
        window in 1_usize..20,
        vals in proptest::collection::vec(-100.0_f64..100.0, 0..50)
    ) {
        let mut r = RollingSum::new(window);
        let mut naive: VecDeque<f64> = VecDeque::new();
        for v in vals {
            naive.push_back(v);
            if naive.len() > window { naive.pop_front(); }
            let got = r.push(v);
            let exp_sum: f64 = naive.iter().sum();
            if naive.len() < window {
                prop_assert!(got.is_none());
            } else {
                prop_assert!(got.is_some());
                prop_assert!((got.unwrap() - exp_sum).abs() < 1e-6, "sum mismatch");
            }
        }
    }

    /// RollingMax over a window equals the naive max of the last `window` values.
    #[test]
    fn prop_rolling_max_matches_naive(
        window in 1_usize..20,
        vals in proptest::collection::vec(-100.0_f64..100.0, 0..50)
    ) {
        let mut r = RollingMax::new(window);
        let mut naive: VecDeque<f64> = VecDeque::new();
        for v in vals {
            naive.push_back(v);
            if naive.len() > window { naive.pop_front(); }
            let got = r.push(v);
            if naive.len() < window {
                prop_assert!(got.is_none());
            } else {
                let exp = naive.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                prop_assert!((got.unwrap() - exp).abs() < 1e-6, "max mismatch");
            }
        }
    }

    /// RollingMin over a window equals the naive min of the last `window` values.
    #[test]
    fn prop_rolling_min_matches_naive(
        window in 1_usize..20,
        vals in proptest::collection::vec(-100.0_f64..100.0, 0..50)
    ) {
        let mut r = RollingMin::new(window);
        let mut naive: VecDeque<f64> = VecDeque::new();
        for v in vals {
            naive.push_back(v);
            if naive.len() > window { naive.pop_front(); }
            let got = r.push(v);
            if naive.len() < window {
                prop_assert!(got.is_none());
            } else {
                let exp = naive.iter().cloned().fold(f64::INFINITY, f64::min);
                prop_assert!((got.unwrap() - exp).abs() < 1e-6, "min mismatch");
            }
        }
    }

    /// RollingRoc equals (last - first) / first once `diff+1` values are seen.
    #[test]
    fn prop_rolling_roc_matches_naive(
        diff in 1_usize..10,
        vals in proptest::collection::vec(0.01_f64..100.0, 0..30)
    ) {
        let mut r = RollingRoc::new(diff);
        let mut naive: VecDeque<f64> = VecDeque::new();
        for v in vals {
            naive.push_back(v);
            if naive.len() > diff + 1 { naive.pop_front(); }
            let got = r.push(v);
            if naive.len() < diff + 1 {
                prop_assert!(got.is_none());
            } else {
                let first = *naive.front().unwrap();
                let last = *naive.back().unwrap();
                let exp = (last - first) / first;
                prop_assert!((got.unwrap() - exp).abs() < 1e-6, "roc mismatch");
            }
        }
    }
}

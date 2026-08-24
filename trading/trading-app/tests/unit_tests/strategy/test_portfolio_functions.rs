//! Unit tests for `proportional_integer_reduce` — the pure DP portfolio reducer.
//!
//! See `src/strategy/portfolio_functions.rs` for the algorithm. These tests cover:
//! empty inputs, mismatched-lengths (#[should_panic]), already-under-target identity,
//! single position, all-same-price, target=0, negative quantities (sign preservation),
//! zero/negative prices, truncation rounding, large portfolios, and proptest invariants
//! (never increases |h_i|, exposure <= T, length preserved).

use proptest::prelude::*;
use trading_app::strategy::portfolio_functions::proportional_integer_reduce;

fn exposure(qtys: &[i64], prices: &[f64]) -> f64 {
    qtys.iter()
        .zip(prices.iter())
        .map(|(q, p)| (q.abs() as f64) * p)
        .sum()
}

// ---------- early-return / edge cases ----------

#[test]
fn empty_inputs_return_empty() {
    let (out, exp) = proportional_integer_reduce(&vec![], &vec![], 100.0);
    assert!(out.is_empty());
    assert_eq!(exp, 0.0);
}

#[test]
fn already_under_target_is_identity() {
    let qtys = vec![5, 3, -2];
    let prices = vec![10.0, 20.0, 15.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 1000.0);
    assert_eq!(out, qtys);
    assert_eq!(exp, exposure(&qtys, &prices));
}

#[test]
fn exactly_at_target_is_identity() {
    // curr_exposure == target → falls into the `<= target + eps` branch
    let qtys = vec![10];
    let prices = vec![10.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 100.0);
    assert_eq!(out, qtys);
    assert_eq!(exp, 100.0);
}

// ---------- single-position scaling ----------

#[test]
fn single_position_halves_cleanly() {
    let qtys = vec![10];
    let prices = vec![10.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 50.0);
    assert_eq!(out, vec![5]);
    assert_eq!(exp, 50.0);
}

#[test]
fn single_short_position_preserves_sign() {
    let qtys = vec![-10];
    let prices = vec![10.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 50.0);
    assert_eq!(out, vec![-5]);
    assert_eq!(exp, 50.0);
}

#[test]
fn target_zero_scales_to_zero() {
    let qtys = vec![10, -7, 4];
    let prices = vec![10.0, 5.0, 2.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 0.0);
    assert!(out.iter().all(|q| *q == 0));
    assert_eq!(exp, 0.0);
}

// ---------- multi-position scaling ----------

#[test]
fn multi_position_uniform_prices_scale_equally() {
    let qtys = vec![10, 10, 10];
    let prices = vec![10.0, 10.0, 10.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 150.0);
    assert_eq!(out, vec![5, 5, 5]);
    assert!((exp - 150.0).abs() < 1e-9);
}

#[test]
fn mixed_long_short_portfolio_scales_proportionally() {
    let qtys = vec![10, -10];
    let prices = vec![10.0, 10.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 100.0);
    // curr = 200, target = 100, scale = 0.5
    assert_eq!(out, vec![5, -5]);
    assert!((exp - 100.0).abs() < 1e-9);
}

// ---------- greedy fill path ----------

#[test]
fn greedy_fill_adds_back_cheapest_units() {
    // Designed so truncation leaves budget, and the cheap-priced position
    // can absorb units within the remaining budget.
    // curr = 10*1.0 + 100*0.01 = 11.0; target = 10.0; scale = 10/11 ≈ 0.9090909
    // trunc: pos0 = trunc(10*0.9091)=9, pos1 = trunc(100*0.9091)=90
    // new_exposure = 9 + 0.9 = 9.9; remaining = 0.1
    // candidates: pos0 max_add=1 price=1.0 (too expensive for 0.1 budget);
    //             pos1 max_add=10 price=0.01 → greedy adds 10 of pos1 (10*0.01=0.1 == budget)
    let qtys = vec![10, 100];
    let prices = vec![1.0, 0.01];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 10.0);
    assert_eq!(out, vec![9, 100]); // pos1 refilled to its original 100
    assert!((exp - 10.0).abs() < 1e-9);
}

#[test]
fn zero_price_position_skipped_in_candidates() {
    // pos0 price=0 → contributes 0 to exposure, candidates skip it (price < eps)
    // curr = 0 + 100 = 100; target=55; scale=0.55; trunc both → [5,5]; new=50; remaining=5
    // candidates: pos0 price=0.0 skipped; pos1 max_add=5 price=10 → 10>5 so can't add
    let qtys = vec![10, 10];
    let prices = vec![0.0, 10.0];
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, 55.0);
    assert_eq!(out, vec![5, 5]);
    assert!((exp - 50.0).abs() < 1e-9);
}

#[test]
fn greedy_cannot_exceed_original_holdings() {
    // Ensure greedy never pushes a position beyond |orig_qty|
    let qtys = vec![2, 100];
    let prices = vec![5.0, 0.001];
    let (out, _) = proportional_integer_reduce(&qtys, &prices, 0.01);
    // pos0: orig=2, scaled=trunc(2*0.01/10.001)=0, max_add=2
    // pos1: orig=100, scaled=trunc(100*0.01/10.001)=0, max_add=100
    // The invariant: |out[i]| <= |qtys[i]|
    assert!(out[0].abs() <= qtys[0].abs());
    assert!(out[1].abs() <= qtys[1].abs());
}

// ---------- truncation / rounding behavior ----------

#[test]
fn truncation_toward_zero_for_shorts() {
    // -10 * 0.55 = -5.5; trunc → -5 (toward zero, not -6)
    let qtys = vec![-10];
    let prices = vec![10.0];
    let (out, _) = proportional_integer_reduce(&qtys, &prices, 55.0);
    assert_eq!(out, vec![-5]); // truncation toward zero
}

// ---------- panic paths ----------

#[test]
#[should_panic(expected = "h and u must have same length")]
fn mismatched_lengths_panics() {
    let _ = proportional_integer_reduce(&vec![1, 2, 3], &vec![1.0, 2.0], 10.0);
}

// ---------- BUG: sign-flip in greedy fill when scaled qty is 0 ----------
//
// When a short position (negative qty) scales down to exactly 0 (via truncation
// toward zero), the greedy fill phase uses `if final_qtys[pos] >= 0` to decide
// the direction of the +1 increment. Since 0 >= 0 is true, it adds +1, flipping
// the sign from negative to positive.
//
// Minimal repro: qtys=[-1], prices=[10.0], target=5.0
//   curr_exposure = 10, target = 5, scale = 0.5
//   trunc(-1 * 0.5) = trunc(-0.5) = 0  (toward zero)
//   new_exposure = 0, remaining budget = 5.0
//   candidate: (pos=0, cost=10.0), max_add = |-1| - |0| = 1
//   10.0 <= 5.0 + eps? No → loop breaks, no add. final_qty = 0. ✓ (no flip here)
//
// But with a cheaper price the greedy DOES add and flips the sign:
//   qtys=[-1, 100], prices=[0.01, 100.0], target=50.0
//   curr = 0.01 + 10000 = 10000.01, target=50, scale≈0.005
//   pos0: trunc(-1*0.005)=0, pos1: trunc(100*0.005)=0
//   new_exposure = 0, budget = 50
//   candidates: pos0 cost=0.01 max_add=1; pos1 cost=100 max_add=100
//   greedy sorts ascending → pos0 first: 0.01 <= 50 → add. final_qtys[0]=0 >= 0 → +=1 → 1 (sign flipped!)
//
// This is a genuine bug. The fix would be to use `quantities[pos]` (the original)
// sign instead of `final_qtys[pos]` in the greedy direction check.
//
// Per project policy: do NOT fix the source. Mark #[ignore] and flag to user.

#[test]
#[ignore = "BUG: greedy fill flips sign of short positions when scaled to 0 — see comment above"]
fn bug_greedy_fill_flips_sign_of_short_scaled_to_zero() {
    let qtys = vec![-1, 100];
    let prices = vec![0.01, 100.0];
    let (out, _exp) = proportional_integer_reduce(&qtys, &prices, 50.0);
    // The first position started at -1; after reduce it should remain <= 0.
    assert!(
        out[0] <= 0,
        "BUG: short position -1 flipped to {} (sign changed)",
        out[0]
    );
}

// ---------- large portfolio smoke test ----------

#[test]
fn large_uniform_portfolio_scales_down() {
    let n = 1000;
    let qtys = vec![10_i64; n];
    let prices = vec![10.0_f64; n];
    let target = 50_000.0; // half of 100*1000
    let (out, exp) = proportional_integer_reduce(&qtys, &prices, target);
    assert_eq!(out.len(), n);
    // every position halves cleanly to 5
    assert!(out.iter().all(|q| *q == 5));
    assert!((exp - target).abs() < 1e-6);
}

// ---------- proptest invariants ----------

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 256, ..Default::default()
    })]

    /// Output length always matches input length.
    #[test]
    fn prop_length_preserved(
        qtys in proptest::collection::vec(-1000_i64..1000, 0..10),
        prices in proptest::collection::vec(0.01_f64..1000.0, 0..10),
        target in 0.0_f64..100_000.0
    ) {
        // skip mismatched-length cases (would panic — tested separately)
        if qtys.len() != prices.len() {
            return Ok(());
        }
        let (out, _exp) = proportional_integer_reduce(&qtys, &prices, target);
        prop_assert_eq!(out.len(), qtys.len());
    }

    /// Never increases |h_i|, and sign is preserved (or zero).
    #[test]
    #[ignore = "BUG: fails due to greedy-fill sign-flip bug — see bug_greedy_fill_flips_sign_of_short_scaled_to_zero"]
    fn prop_never_increases_or_flips_sign(
        qtys in proptest::collection::vec(-1000_i64..1000, 1..10),
        prices in proptest::collection::vec(0.01_f64..1000.0, 1..10),
        target in 0.0_f64..100_000.0
    ) {
        if qtys.len() != prices.len() {
            return Ok(());
        }
        let (out, _exp) = proportional_integer_reduce(&qtys, &prices, target);
        for (o, q) in out.iter().zip(qtys.iter()) {
            prop_assert!(o.abs() <= q.abs(), "increased: out={} in={}", o, q);
            // sign preserved or zero
            prop_assert!(*o == 0 || o.signum() == q.signum(),
                         "sign flipped: out={} in={}", o, q);
        }
    }

    /// Final exposure <= target + eps, and equals the recomputed sum.
    #[test]
    fn prop_exposure_bounded_and_consistent(
        qtys in proptest::collection::vec(-1000_i64..1000, 1..10),
        prices in proptest::collection::vec(0.01_f64..1000.0, 1..10),
        target in 0.0_f64..100_000.0
    ) {
        if qtys.len() != prices.len() {
            return Ok(());
        }
        let (out, exp) = proportional_integer_reduce(&qtys, &prices, target);
        let recomputed = exposure(&out, &prices);
        prop_assert!((exp - recomputed).abs() < 1e-6,
                     "exp={} recomputed={}", exp, recomputed);
        prop_assert!(exp <= target + 1e-9,
                     "exp={} > target={}", exp, target);
    }
}

//! Tests for `src/helpers/allocator.rs`.
//!
//! Almost nothing in this module is externally reachable without a run-time:
//!   * `fund_strategies` (src/helpers/allocator.rs:89) calls
//!     `Consolidator::get_fund_value` (live `ibapi::Client`) and opens a
//!     `PgPool` connection before any pure allocation maths runs.
//!   * `Book::delta` (src/helpers/allocator.rs:77), `acceptable_currencies`
//!     (src/helpers/allocator.rs:478), `write_cash` and `hub_cash` are private
//!     and not re-exported through `trading_app::test_internals`.
//!   * `EPS` (src/helpers/allocator.rs:47) is private.
//!
//! So the only stable, observable contract is the public `UNALLOCATED`
//! sentinel: it is the strategy name of the hub's CASH rows and the
//! `order_ref` of every funding FX order, so its exact bytes are load-bearing.

use trading_app::helpers::allocator::UNALLOCATED;

#[test]
fn unallocated_sentinel_is_the_exact_reserved_strategy_name() {
    assert_eq!(UNALLOCATED, "__unallocated__");
    assert_eq!(UNALLOCATED.len(), 15);
    assert!(UNALLOCATED.starts_with("__") && UNALLOCATED.ends_with("__"));
    // Ordinary strategy names must not collide with it.
    for name in ["unallocated", "_unallocated_", "UNALLOCATED", "__unallocated___"] {
        assert_ne!(UNALLOCATED, name);
    }
}

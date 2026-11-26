//! Tests for `src/schedule/contract_scheduler.rs`.
//!
//! Almost nothing here is testable without a live IB Gateway, so this module
//! covers only the public `Interval` value type and documents the blockers.
//!
//! NOT covered — hard blockers:
//!   - `Interval` fields `open`/`close` (:44-45) are private, and the type has
//!     no constructor, accessor, or `PartialEq`/`Ord` impl, so its arithmetic,
//!     equality and ordering cannot be exercised from an integration test.
//!     The only assertions possible are layout/derive ones (below).
//!   - `IbkrContractScheduler::new` (:60) takes `Arc<ibapi::Client>`.
//!     `ibapi::Client::connect` (rust-ibapi-fork `client/sync.rs`) opens a TCP
//!     session to TWS/IB Gateway; there is no offline constructor and the
//!     struct's fields are private, so the scheduler cannot be built at all.
//!   - `IbkrContractScheduler::contains_contract` (:68) is public and pure
//!     (`self.schedules.contains_key`), but needs an instance, so it is
//!     client-bound in practice.
//!   - `fetch_schedule` (:72) calls `client.contract_details` inside a 10s
//!     timeout — network round trip.
//!   - `update_schedule` (:165) and `get_interval` (:232) are private and both
//!     call `fetch_schedule`; `get_interval` also reads `Utc::now()` (:253) and
//!     spins on an `AtomicBool`.
//!   - `ContractScheduler` impl (:300): `add_schedule` (:301),
//!     `add_all_schedules` (:320), `is_trading` (:364),
//!     `get_next_latest_unavailable_data` (:370) and
//!     `get_next_earliest_available_data` (:388) all route through
//!     `get_interval`; the `get_next_*` pair additionally read `Utc::now()`
//!     (:374, :397).
//!   - The `ContractScheduler` trait itself (:26) is not object-safe: it has
//!     generic methods (:28, :32, :36), so no `&dyn ContractScheduler` test
//!     double is possible either.

use std::mem::{align_of, size_of};

use trading_app::schedule::contract_scheduler::Interval;

fn assert_copy_clone_debug<T: Copy + Clone + std::fmt::Debug>() {}

/// `Interval` is a plain `{ i64, i64 }` pair with `Debug/Clone/Copy` derives
/// (:42-46). Pins the layout that the private-field arithmetic depends on.
#[test]
fn interval_is_copy_clone_debug_pair_of_i64() {
    assert_copy_clone_debug::<Interval>();
    assert_eq!(size_of::<Interval>(), 2 * size_of::<i64>());
    assert_eq!(align_of::<Interval>(), align_of::<i64>());
}

// `Interval` deliberately does not implement `PartialEq`/`Ord` (:42), and it
// exposes no getters, so callers outside the crate can neither build nor
// inspect one. That is a compile-time fact, not something a runtime test can
// assert; the blocker is recorded in the module doc above. If a public
// constructor/accessor is ever added, add real arithmetic/boundary coverage
// here — the layout test above is not a substitute.

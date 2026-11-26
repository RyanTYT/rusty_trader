//! Tests for `src/helpers/` (contract, sentinel, sync_timeout, allocator).
//!
//! Pure, deterministic logic only — no IB client, no DB, no network.
//!
//! `src/helpers/contract.rs` is already covered indirectly by
//! `tests/market_data/consolidator.rs:944–1240` (all `build_contract_from_stock`
//! prefixes, `get_local_symbol` formats, `HashContract` identity and the
//! `CurrentPosFk` arm of `get_contract_from`). This tree adds only what that
//! coverage does not: the remaining `get_contract_from` enum arms, additional
//! `get_local_symbol` edge cases, and the sentinel/timeout/allocator helpers.
//!
//! NOT covered — blocked by private visibility or live services:
//!   * `fund_strategies` (src/helpers/allocator.rs:89) — needs a live `ibapi::Client`
//!     (`Consolidator::get_fund_value`) and a Postgres pool.
//!   * `Book::delta` (src/helpers/allocator.rs:77), `acceptable_currencies`
//!     (src/helpers/allocator.rs:478), `write_cash` and `hub_cash` — private; not
//!     re-exported by `trading_app::test_internals`.
//!   * `async_call` (src/helpers/sync_timeout.rs:52) and `timeout_panic`
//!     (src/helpers/sync_timeout.rs:75) — `pub(crate)` and not exposed through
//!     `test_internals` (only `timeout` is, src/lib.rs:47). Covering them would
//!     need a src-level re-export; no src edits were made.

mod allocator;
mod contract;
mod sentinel;
mod sync_timeout;

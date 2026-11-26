//! Tests for `src/schedule` (broker maintenance windows, contract schedules,
//! the program supervisor loop).
//!
//! Scope: pure, deterministic logic only — no IB client, no DB, no network,
//! no wall-clock-dependent assertions.
//!
//! Coverage map:
//!   - `broker_scheduler.rs` — `IbkrRegion::schedule` / `is_in_maintenance`
//!     (exact UTC boundaries, ET weekday, DST offsets), `IbkrStateService`,
//!     trait-object dispatch, `get_next_*` invariants.
//!   - `contract_scheduler.rs` — only the public `Interval` type layout/derives;
//!     everything else is behind a live `Arc<ibapi::Client>` (see module doc).
//!   - `program_scheduler.rs` — `AppReturnState` construction/payloads;
//!     `run_program` needs signal handlers + an IB Gateway (see module doc).

mod broker_scheduler;
mod contract_scheduler;
mod program_scheduler;

//! Small cross-cutting utilities shared by the rest of the crate.
//!
//! - [`contract`](crate::helpers::contract) — the local contract vocabulary
//!   (`LocalContractTypes`) and conversions between DB rows, strategy diffs
//!   and IBKR contracts;
//! - [`sync_timeout`](crate::helpers::sync_timeout) — run blocking calls on a
//!   worker thread, with optional deadline / panic-on-timeout variants;
//! - [`sentinel`](crate::helpers::sentinel) — placeholder values for fixed-size
//!   buffers;
//! - [`allocator`](crate::helpers::allocator) — bounded mpsc-based queue
//!   helpers for strategies.

/// Bounded queue helpers for strategies.
pub mod allocator;
/// Contract vocabulary and row ⇄ IBKR contract conversions.
pub mod contract;
/// Placeholder values for fixed-size buffers and streams.
pub mod sentinel;
/// Worker-thread execution with optional timeouts.
pub mod sync_timeout;

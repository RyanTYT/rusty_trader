//! The live order-event pipeline.
//!
//! [`controller`](crate::execution::order_update_stream::controller) owns the single IBKR order subscription and forwards events;
//! [`event_handlers`](crate::execution::order_update_stream::event_handlers) turns each event into database writes. The split exists so
//! the subscription lifecycle (one OS thread, one Tokio task, one process-wide
//! latch) stays independent of the event semantics.
//!
//! ```text
//! IBKR order stream
//!   └─ controller: OS thread, 5 s poll, resubscribe on error
//!        └─ mpsc (1024) ─ controller: async dispatch task
//!             ├─ order_status::{submitted, cancelled}
//!             ├─ open_order::submitted
//!             ├─ execution::on_execution_update
//!             └─ commission_report::on_commission_update
//! ```
//!
//! Live only: the module is compiled under `#[cfg(not(feature = "backtest"))]`
//! in [`crate::execution`]. Replays drive the handlers directly instead.

pub mod controller;
pub mod event_handlers;

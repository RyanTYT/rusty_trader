//! Order execution: from strategy intent to broker orders and back.
//!
//! This layer sits between the strategies (which decide *what* should be held)
//! and IBKR (which reports what actually happened). It turns a strategy's
//! [`BarUpdateOutcome`](crate::strategy::strategy::BarUpdateOutcome) into
//! placements/cancellations, applies broker events to the database, and
//! reconciles local state after a restart or reconnect.
//!
//! # What lives here
//!
//! - [`order_engine`](crate::execution::order_engine) — placement and cancellation, bar-outcome handling, the
//!   reconciliation gate, and [`OrderIBKR`](crate::execution::order_engine::OrderIBKR), the
//!   contract + order pair that flows through the layer.
//! - [`fx_organiser`](crate::execution::fx_organiser) — plans the FX legs and parked batches needed to fund a
//!   target-position change.
//! - [`fx_backed_up_order`](crate::execution::fx_backed_up_order) — the durable (redb) store for orders that cannot
//!   be sent yet because their currency cash is not available.
//! - [`order_update_stream`](crate::execution::order_update_stream) — the live IBKR subscription and the dispatch of
//!   order status, open-order, execution and commission events.
//! - [`syncer`](crate::execution::syncer) — startup/reconnect reconciliation of executions, open orders
//!   and positions against the broker.
//! - [`traits`](crate::execution::traits) — the seams ([`OrderRouter`](crate::execution::traits::order_router::OrderRouter),
//!   [`PositionsTracker`](crate::execution::traits::positions_tracker::PositionsTracker)) that let
//!   the engine and syncer be tested without IB Gateway.
//!
//! # Lifecycle
//!
//! 1. A strategy emits an intent; [`order_engine::OrderEngine::handle_bar_update_outcome`](crate::execution::order_engine::OrderEngine::handle_bar_update_outcome)
//!    either places/cancels directly or reads target/current positions and
//!    builds a funding plan.
//! 2. Orders are submitted and an optimistic row is written to the open-order
//!    table. Orders awaiting FX cash are parked in the redb store instead.
//! 3. IBKR sends an execution or order status; the controller routes it to the
//!    matching handler under [`event_handlers`](crate::execution::order_update_stream::event_handlers).
//! 4. The handler applies the change in **one** `DbTxn`: open order,
//!    transaction row, current position and currency position commit together.
//!    Post-commit work (strategy hooks, placing backed-up orders) happens only
//!    after that commit.
//!
//! # Invariants
//!
//! - Never hold a `DbTxn` across an IBKR call, a sleep, or a spawned task.
//! - An execution is idempotent by `execution_id`; duplicates are ignored.
//! - An open order is identified by `(perm_id, order_id)`; the optimistic row
//!   uses `perm_id = -1` until the broker acknowledges it.
//! - A strategy's order reference is stored verbatim, so both `strategy` and
//!   `strategy:price` keys appear in the open-order table.
//!
//! # Live vs backtest
//!
//! [`order_update_stream`](crate::execution::order_update_stream) and [`syncer`](crate::execution::syncer) are compiled only for live builds.
//! Backtests drive the same handlers through [`traits`](crate::execution::traits) fakes and the
//! `OrderSubmitter`/`PriceSupplier` seams, without IB Gateway.
//!
//! [`BarUpdateOutcome`]: crate::strategy::strategy::BarUpdateOutcome

/// Durable store for orders parked while their FX cash is unavailable.
pub mod fx_backed_up_order;
/// FX funding planner: closing sales, root FX orders and parked batches.
pub mod fx_organiser;
/// Order placement, cancellation, bar outcomes and the reconciliation gate.
pub mod order_engine;
/// Live IBKR order-update subscription and event dispatch.
#[cfg(not(feature = "backtest"))]
pub mod order_update_stream;
/// Startup/reconnect reconciliation against IBKR.
#[cfg(not(feature = "backtest"))]
pub mod syncer;
/// Executor-facing ports used by the engine, syncer and tests.
pub mod traits;

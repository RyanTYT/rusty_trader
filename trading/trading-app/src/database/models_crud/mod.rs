//! Table-specific CRUD handles.
//!
//! Each submodule binds one table (or one interface over several tables) to a
//! [`CRUD`](crate::database::crud::CRUD) and exposes only the operations that
//! are valid for that table. Full-CRUD tables use
//! [`implement_all_crud_methods!`](crate::implement_all_crud_methods);
//! append-only identity tables (`logs`, `notifications`) expose only
//! `create`/`read_all`, so misuse is a compile error.
//!
//! Interface enums dispatch by
//! [`AssetType`](crate::database::models::AssetType) and return
//! [`DbError::KeyVariantMismatch`](crate::database::db::DbError::KeyVariantMismatch)
//! when the key variant and the chosen table disagree.

/// Cancelled-order audit rows (`logs.cancelled_orders`).
pub mod cancelled_orders;
/// Current positions, stock and option interfaces (`trading.current_*`).
pub mod current_positions;
/// Historical bars: stock, option, forex, and batch ingest
/// (`market_data.historical_*`).
pub mod historical_data;
/// Append-only application logs (`logs.logs`).
pub mod logs;
/// Append-only user notifications (`trading.notifications`).
pub mod notification;
/// Open orders, stock and option interfaces (`trading.open_*`).
pub mod open_orders;
/// Commissions waiting for their execution (`trading.staged_commissions`).
pub mod staged_commissions;
/// The strategy registry (`trading.strategy`).
pub mod strategy;
/// Target positions, stock and option interfaces (`trading.target_*`).
pub mod target_positions;
/// Executions, stock and option interfaces (`trading.*_transactions`).
pub mod transactions;

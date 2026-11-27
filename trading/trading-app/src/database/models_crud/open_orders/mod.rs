//! Resting (working) orders.
//!
//! [`open_orders`] dispatches by [`AssetType`](crate::database::models::AssetType);
//! the concrete modules own the stock and option tables. Rows are keyed by
//! `(order_perm_id, order_id)` and carry the strategy verbatim.
pub mod open_option_orders;
pub mod open_orders;
pub mod open_stock_orders;

//! Desired holdings and the target-vs-current diff.
//!
//! [`target_positions`] is the interface; the diff queries FULL OUTER JOIN the
//! matching current table and return `qty_diff = target - current` (positive =
//! buy, negative = sell). The concrete modules hold the stock and option tables.
pub mod target_option_positions;
pub mod target_positions;
pub mod target_stock_positions;

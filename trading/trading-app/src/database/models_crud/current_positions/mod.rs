//! Current holdings.
//!
//! [`current_positions`] is the asset-dispatching interface handle; the
//! `current_*_positions` modules hold the concrete stock and option tables and
//! their row/key types. Positions are signed (negative = short), and the
//! `CASH:<currency>` pseudo-symbols in the stock table carry the account's cash.
pub mod current_option_positions;
pub mod current_positions;
pub mod current_stock_positions;

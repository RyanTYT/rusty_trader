//! Executed fills.
//!
//! [`transactions`] is the interface; each row is one execution, keyed by
//! `execution_id`, with signed quantity and fees applied by the commission
//! triggers. The concrete modules hold the stock and option tables.
pub mod option_transactions;
pub mod stock_transactions;
pub mod transactions;

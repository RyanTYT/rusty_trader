//! Historical market data.
//!
//! [`historical_data`] is the asset-dispatching interface (stock, option and
//! forex tables); the concrete modules hold the per-table rows and CRUD
//! handles. [`batch_operations`](crate::database::models_crud::historical_data::batch_operations) provides the COPY-based bulk writer used by the
//! market-data consumers.
pub mod batch_operations;
pub mod historical_data;
pub mod historical_forex_data;
pub mod historical_options_data;
pub mod historical_stock_data;

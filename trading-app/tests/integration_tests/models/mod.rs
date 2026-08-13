// Per-table CRUD roundtrip integration tests.
//
// Each test file exercises the full CRUDTrait lifecycle (create → read → update →
// delete) for one database table, using the shared infrastructure in `init.rs`.
//
// NOTE: These tests require a live Postgres + DATABASE_URL env var.

pub mod init;
mod test_strategy;
mod test_notification;
mod test_staged_commissions;
mod test_logs;
mod test_cancelled_orders;
mod test_current_stock_positions;
mod test_current_option_positions;
mod test_target_stock_positions;
mod test_target_option_positions;
mod test_open_stock_orders;
mod test_open_option_orders;
mod test_stock_transactions;
mod test_option_transactions;
mod test_historical_data;
mod test_historical_options_data;
mod test_historical_forex_data;
mod test_daily_historical_data;

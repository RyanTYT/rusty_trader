// Per-table CRUD roundtrip + advanced ops integration tests.
//
// This module mirrors `src/database/models_crud/` — each subdirectory tests the
// advanced `*Ops` trait methods for one interface CRUD enum.
//
// NOTE: These tests require a live Postgres + DATABASE_URL env var.

pub mod init;

// Per-table CRUD roundtrips (existing)
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

// Advanced ops — organized by interface enum
mod current_positions;
mod open_orders;
mod target_positions;
mod transactions;
mod historical_data;

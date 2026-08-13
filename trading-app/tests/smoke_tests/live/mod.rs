// Live IBKR smoke tests.
//
// These tests boot a persistent IB Gateway via IBC and exercise the full
// IBKR API surface. They are ALL marked #[ignore] and only run with:
//
//     DATABASE_URL=... cargo test --test smoke_tests -- --ignored
//
// Prerequisites:
// - IBC installed at `/IBCLinux-3.21.2/scripts/ibcstart.sh`
// - IB Gateway credentials in env vars
// - Postgres + DATABASE_URL

mod init;
mod test_validate_contract;
mod test_get_current_price;
mod test_populate_historical_data;
mod test_get_strategy_sgd_value;
mod test_add_schedule;
mod test_subscribe_to_data;
mod test_hook_strategy;
mod test_place_order;
mod test_syncer;
mod test_order_update_stream;
mod test_init_app_smoke;
mod test_full_flow;

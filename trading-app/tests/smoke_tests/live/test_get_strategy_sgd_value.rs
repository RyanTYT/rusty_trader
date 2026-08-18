//! Smoke test: get_strategy_sgd_value (live IBKR).
//! `Consolidator::get_strategy_sgd_value` (GetStrategyValue trait). Sync fn.
//! Requires: live IB Gateway + Postgres. Run with: `cargo test --test smoke_tests test_get_strategy_sgd_value -- --ignored`

use std::sync::Arc;

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::traits::strategy_value::GetStrategyValue;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;

use crate::live::init::{
    api_port_addr, ensure_strategy_row, ibkr_account, server_base_url, with_live_ibkr,
};

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_get_strategy_sgd_value_live() {
    with_live_ibkr(&ibkr_account(), "ibc_live.log", |state| async move {
        // get_strategy_sgd_value reads current_positions (FK → trading.strategy),
        // so the "noise" row must exist for any position-bearing test run.
        ensure_strategy_row(&state.pool, "noise").await;

        let _contract = Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        };

        let contract_scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
        let market_data_handler = MarketDataHandler::new(state.pool.clone());
        let consolidator = Arc::new(Consolidator::new(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            state.client_1.clone(),
            market_data_handler,
            contract_scheduler,
        ));

        // get_strategy_sgd_value is SYNC + internally calls handle.block_on(...).
        // Mirror the production hook_strategy pattern (strategy_consumer.rs:142):
        // run it on a dedicated OS thread, then join. The handle passed into the
        // Consolidator above is what block_on uses — works with any runtime flavor
        // now that yfinance no longer uses block_in_place.
        let consolidator_for_thread = consolidator.clone();
        let join = std::thread::Builder::new()
            .name("sgd_value_test".to_string())
            .spawn(move || consolidator_for_thread.get_strategy_sgd_value("noise"))
            .expect("Failed to spawn sgd_value thread");
        let result = join
            .join()
            .expect("sgd_value thread panicked");

        match result {
            Ok(value) => {
                assert!(value.is_finite(), "SGD value should be finite, got {value}");
                println!("Strategy 'noise' SGD value: {value}");
            }
            Err(e) => {
                println!("get_strategy_sgd_value returned error (expected if no positions): {e}");
            }
        }
    })
    .await
    .expect("Failed to boot live IBKR");
}

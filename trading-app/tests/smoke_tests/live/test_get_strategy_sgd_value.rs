//! Smoke test: get_strategy_sgd_value (live IBKR).
//! `Consolidator::get_strategy_sgd_value` (GetStrategyValue trait). Sync fn.
//! Requires: live IB Gateway + Postgres. Run with: `cargo test --test smoke_tests test_get_strategy_sgd_value -- --ignored`

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::traits::strategy_value::GetStrategyValue;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;

use crate::live::init::{api_port_addr, ibkr_account, server_base_url, with_live_ibkr};

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_get_strategy_sgd_value_live() {
    with_live_ibkr(&ibkr_account(), "ibc_live.log", |state| async move {
        let _contract = Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        };

        let contract_scheduler =
            std::sync::Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
        let market_data_handler = MarketDataHandler::new(state.pool.clone());
        let consolidator = Consolidator::new(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            state.client_1.clone(),
            market_data_handler,
            contract_scheduler,
        );

        tokio::task::block_in_place(|| {
            let result = consolidator.get_strategy_sgd_value("noise");

            match result {
                Ok(value) => {
                    assert!(value.is_finite(), "SGD value should be finite, got {value}");
                    println!("Strategy 'noise' SGD value: {value}");
                }
                Err(e) => {
                    println!(
                        "get_strategy_sgd_value returned error (expected if no positions): {e}"
                    );
                }
            }
        });
    })
    .await
    .expect("Failed to boot live IBKR");
}

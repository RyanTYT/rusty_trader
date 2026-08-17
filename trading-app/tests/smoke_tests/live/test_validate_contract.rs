//! Smoke test: validate_contract (live IBKR).
//! `Consolidator::validate_contract` calls `client.contract_details()`.
//! Requires: live IB Gateway. Run with: `cargo test --test smoke_tests test_validate_contract -- --ignored`

use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;

use crate::live::init::{api_port_addr, ibkr_account, server_base_url, with_live_ibkr};

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_validate_contract_live() {
    with_live_ibkr(&ibkr_account(), "ibc_live.log", |state| async move {
        let contract = Contract {
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

        let result = consolidator.validate_contract(contract.clone(), Duration::from_secs(30));
        assert!(
            result.is_some(),
            "validate_contract should return Some for AAPL"
        );

        let validated = result.unwrap();
        assert!(validated.symbol.to_string() == "AAPL");
        assert!(validated.contract_id > 0, "contract_id should be populated");

        let bad_contract = Contract {
            symbol: "NONEXISTENT123XYZ".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            ..Default::default()
        };
        let bad_result = consolidator.validate_contract(bad_contract, Duration::from_secs(30));
        assert!(
            bad_result.is_none(),
            "expected None for non-existent contract"
        );
    })
    .await
    .expect("Failed to boot live IBKR");
}

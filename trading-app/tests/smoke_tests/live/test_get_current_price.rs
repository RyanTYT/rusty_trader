//! Smoke test: get_current_price (live IBKR).
//! `Consolidator::get_current_price` (PriceSupplier trait).
//! Requires: live IB Gateway + market open. Run with: `cargo test --test smoke_tests test_get_current_price -- --ignored`

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::traits::current_price::PriceSupplier;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;

use crate::live::init::{with_live_ibkr, ibkr_account, api_port_addr, server_base_url};

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_get_current_price_live() {
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

        let price = consolidator
            .get_current_price(contract, false, &[])
            .expect("get_current_price failed");

        assert!(price > 0.0, "expected positive price, got {price}");
        assert!(
            (50.0..=500.0).contains(&price),
            "AAPL price {price} out of expected range"
        );
    })
    .await
    .expect("Failed to boot live IBKR");
}

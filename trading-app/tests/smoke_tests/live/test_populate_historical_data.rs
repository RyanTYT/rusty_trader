//! Smoke test: populate_historical_data (live IBKR).
//! `Consolidator::populate_historical_data` (PriceSupplier trait).
//! Requires: live IB Gateway + Postgres. Run with: `cargo test --test smoke_tests test_populate_historical_data -- --ignored`

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;

use crate::live::init::{with_live_ibkr, ibkr_account, api_port_addr, server_base_url};

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_populate_historical_data_live() {
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

        let config = HistoricalDataConfig::new(
            ibapi::market_data::historical::Duration::DAY,
            ibapi::market_data::historical::BarSize::Min5,
            ibapi::market_data::historical::WhatToShow::Trades,
            false,
        );

        consolidator
            .populate_historical_data(&contract, &config)
            .await
            .expect("populate_historical_data failed");

        println!("populate_historical_data succeeded — smoke test passed");
    })
    .await
    .expect("Failed to boot live IBKR");
}

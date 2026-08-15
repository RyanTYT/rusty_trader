//! Smoke test: add_schedule (live IBKR).
//! `IbkrContractScheduler::add_schedule` (ContractScheduler trait). Sync, takes &mut self.
//! Requires: live IB Gateway + Postgres. Run with: `cargo test --test smoke_tests test_add_schedule -- --ignored`

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};

use crate::live::init::with_live_ibkr;

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_add_schedule_live() {
    with_live_ibkr("DU111111", "ibc_live.log", |state| async move {
        let mut scheduler = IbkrContractScheduler::new(state.client_1.clone());

        let contract = Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        };

        // add_schedule is sync (not async) — remove .await
        scheduler
            .add_schedule(&contract)
            .expect("add_schedule failed");

        assert!(
            scheduler.contains_contract(&contract),
            "scheduler should contain AAPL after add_schedule"
        );

        // is_trading is sync too
        let dt = chrono::Utc::now();
        let is_trading = scheduler
            .is_trading(&contract, &dt)
            .expect("is_trading failed");
        println!("AAPL is_trading at {dt}: {is_trading}");
    })
    .await;
}

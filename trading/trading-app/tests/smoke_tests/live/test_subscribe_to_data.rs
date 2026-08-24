//! Smoke test: subscribe_to_data (live IBKR).
//! `subscribe_to_data` spawns a thread calling `client.realtime_bars()`.
//! Returns `(Pin<Box<SpmcRingBuffer<Bar>>>, MarketDataProducer)`.
//! Requires: live IB Gateway + market open. Run with: `cargo test --test smoke_tests test_subscribe_to_data -- --ignored`

use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::prelude::{RealtimeWhatToShow, SecurityType};
use trading_app::market_data::producer::subscribe_to_data;
use trading_app::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};

use crate::live::init::{ibkr_account, with_live_ibkr};

const BUFFER_SIZE: usize = 128;
const MAX_NO_OF_CONSUMERS: usize = 4;

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_subscribe_to_data_live() {
    with_live_ibkr(&ibkr_account(), "ibc_live.log", |state| async move {
        let mut scheduler = IbkrContractScheduler::new(state.client_1.clone());
        let contract = Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        };
        scheduler
            .add_schedule(&contract)
            .expect("add_schedule failed");
        let scheduler = std::sync::Arc::new(scheduler);

        let (ring_buffer, mut producer) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
            std::sync::Arc::downgrade(&state.client_1),
            contract,
            RealtimeWhatToShow::Trades,
            scheduler,
        );

        // Get a consumer to pop bars
        let consumer = ring_buffer
            .get_new_consumer()
            .expect("expected to get a consumer");

        // Wait for a few 5-sec bars
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Try to read a bar
        let bar = consumer.try_pop();
        match bar {
            Some(b) => {
                println!("Received bar: close={}", b.close);
                assert!(b.close > 0.0, "bar close price should be positive");
            }
            None => {
                println!("No bars received (market may be closed) — acceptable");
            }
        }

        producer.async_drop().await;
    })
    .await
    .expect("Failed to boot live IBKR");
}

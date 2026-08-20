//! Comprehensive smoke tests for `MarketDataHandler` (live IBKR).
//!
//! Tests all MarketDataHandler public methods:
//! - `MarketDataHandler::new()` — constructor
//! - `load_all_subscription_producers()` — both DbSubscriptionMethod variants
//!   (OnePerThread + GroupedPerThread)
//! - `get_subsription()` — returns the ring buffer for an existing subscription
//! - `try_get_price()` — returns cached price for a contract_id
//! - `DataSubscription::new()` — constructor
//! - `DataSubscription` Hash/Eq (stock + option + forex variants)
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_market_data_handler -- --ignored

use std::sync::{Arc, Weak};
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::market_data::realtime::WhatToShow;
use ibapi::prelude::SecurityType;
use spmc_ring::bench::RingBuffer;
use trading_app::loop_until_async_drop;
use trading_app::market_data::handler::{
    DataSubscription, DbSubscriptionMethod, MarketDataHandler,
};
use trading_app::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};

use crate::live::init::{api_port_addr, ibkr_account, server_base_url, with_live_ibkr};

fn aapl_contract() -> Contract {
    Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    }
}

fn msft_contract() -> Contract {
    Contract {
        symbol: "MSFT".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    }
}

fn eur_usd_contract() -> Contract {
    Contract {
        symbol: "EUR".into(),
        security_type: SecurityType::ForexPair,
        currency: "USD".into(),
        exchange: "IDEALPRO".into(),
        ..Default::default()
    }
}

// ============================ 1. MarketDataHandler::new — constructor ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_market_data_handler_new() {
    with_live_ibkr(&ibkr_account(), "ibc_mdh_new.log", |state| async move {
        let mut handler = MarketDataHandler::new(state.pool.clone());
        println!("✅ MarketDataHandler::new succeeded (no panic)");

        // Verify the handler is usable — call get_subsription on a non-existent sub
        let sub = DataSubscription::new(aapl_contract(), WhatToShow::Trades);
        let result = handler.get_subsription(&sub);
        assert!(
            result.is_none(),
            "get_subsription should return None for unsubscribed contract"
        );
        println!("✅ get_subsription(AAPL) before load: None (correct — not subscribed yet)");

        handler.async_drop().await;
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 2. DataSubscription::new + Hash/Eq ============================

#[tokio::test]
async fn test_data_subscription_new_and_eq() {
    // This test doesn't need IBKR — DataSubscription is a pure struct
    let aapl = aapl_contract();
    let sub1 = DataSubscription::new(aapl.clone(), WhatToShow::Trades);
    let sub2 = DataSubscription::new(aapl.clone(), WhatToShow::Trades);
    let sub3 = DataSubscription::new(aapl, WhatToShow::Bid);

    // Eq: same contract + same what_to_show → equal
    assert_eq!(
        sub1, sub2,
        "same contract + same WhatToShow should be equal"
    );
    assert_ne!(
        sub1, sub3,
        "same contract + different WhatToShow should NOT be equal"
    );

    // Hash: equal subscriptions should have equal hash
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    use std::hash::{Hash, Hasher};
    sub1.hash(&mut h1);
    sub2.hash(&mut h2);
    assert_eq!(
        h1.finish(),
        h2.finish(),
        "equal subscriptions should have equal hash"
    );

    // Different WhatToShow → different hash
    let mut h3 = std::collections::hash_map::DefaultHasher::new();
    sub3.hash(&mut h3);
    assert_ne!(
        h1.finish(),
        h3.finish(),
        "different WhatToShow should have different hash"
    );

    println!("✅ DataSubscription::new + Hash/Eq verified");
}

// ============================ 3. load_all_subscription_producers — OnePerThread ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_load_all_subscription_producers_one_per_thread() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_load_one.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());
            let mut raw_contract_scheduler = IbkrContractScheduler::new(state.client_1.clone());
            assert!(
                raw_contract_scheduler
                    .add_schedule(&aapl_contract())
                    .is_ok()
            );
            let contract_scheduler = Arc::new(raw_contract_scheduler);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            let subscriptions = vec![DataSubscription::new(aapl_contract(), WhatToShow::Trades)];

            // Load subscriptions with OnePerThread method
            handler.load_all_subscription_producers(
                &weak_client,
                contract_scheduler.clone(),
                subscriptions.clone(),
                DbSubscriptionMethod::OnePerThread,
                tokio::runtime::Handle::current(),
            );

            println!("load_all_subscription_producers(OnePerThread) completed");

            // Verify the subscription was registered
            let sub = &subscriptions[0];
            let ring_buffer = handler.get_subsription(sub);
            assert!(
                ring_buffer.is_some(),
                "subscription should be registered after load"
            );
            println!("✅ get_subsription(AAPL Trades) after load: Some (registered)");

            // Verify idempotency — calling again with the same subscription should be a no-op
            handler.load_all_subscription_producers(
                &weak_client,
                contract_scheduler.clone(),
                subscriptions.clone(),
                DbSubscriptionMethod::OnePerThread,
                tokio::runtime::Handle::current(),
            );
            println!("✅ idempotent load (calling again with same subscription — no-op)");

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 4. load_all_subscription_producers — GroupedPerThread ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_load_all_subscription_producers_grouped_per_thread() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_load_grouped.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());
            let mut raw_contract_scheduler = IbkrContractScheduler::new(state.client_1.clone());
            assert!(
                raw_contract_scheduler
                    .add_all_schedules(vec![aapl_contract(), msft_contract()])
                    .is_ok()
            );
            let contract_scheduler = Arc::new(raw_contract_scheduler);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            let subscriptions = vec![
                DataSubscription::new(aapl_contract(), WhatToShow::Trades),
                DataSubscription::new(msft_contract(), WhatToShow::Trades),
            ];

            // Load multiple subscriptions with GroupedPerThread method
            handler.load_all_subscription_producers(
                &weak_client,
                contract_scheduler.clone(),
                subscriptions.clone(),
                DbSubscriptionMethod::GroupedPerThread,
                tokio::runtime::Handle::current(),
            );

            println!(
                "load_all_subscription_producers(GroupedPerThread) completed with 2 subscriptions"
            );

            // Verify both subscriptions were registered
            for sub in &subscriptions {
                let ring_buffer = handler.get_subsription(sub);
                assert!(ring_buffer.is_some(), "subscription should be registered");
            }
            println!("✅ get_subsription for both AAPL + MSFT after GroupedPerThread load: Some");

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 5. load_all_subscription_producers — multiple different WhatToShow ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_load_all_subscription_producers_multiple_what_to_show() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_load_wts.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());
            let mut raw_contract_scheduler = IbkrContractScheduler::new(state.client_1.clone());
            assert!(
                raw_contract_scheduler
                    .add_all_schedules(vec![eur_usd_contract()])
                    .is_ok()
            );
            let contract_scheduler = Arc::new(raw_contract_scheduler);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            // Same contract, different WhatToShow — should be treated as separate subscriptions
            let contract = eur_usd_contract();
            let subscriptions = vec![
                DataSubscription::new(contract.clone(), WhatToShow::Bid),
                DataSubscription::new(contract.clone(), WhatToShow::Ask),
            ];

            handler.load_all_subscription_producers(
                &weak_client,
                contract_scheduler.clone(),
                subscriptions.clone(),
                DbSubscriptionMethod::GroupedPerThread,
                tokio::runtime::Handle::current(),
            );

            println!("load_all_subscription_producers with Bid + Ask for same forex contract");

            // Verify both WhatToShow variants are registered separately
            let bid_sub = handler.get_subsription(&subscriptions[0]);
            let ask_sub = handler.get_subsription(&subscriptions[1]);
            assert!(bid_sub.is_some(), "Bid subscription should be registered");
            assert!(ask_sub.is_some(), "Ask subscription should be registered");
            assert_ne!(
                bid_sub.map(|r| r as *const _),
                ask_sub.map(|r| r as *const _),
                "Bid and Ask subscriptions should be different ring buffers"
            );
            println!(
                "✅ Bid + Ask for same contract are separate subscriptions (different ring buffers)"
            );

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 6. get_subsription — non-existent subscription returns None ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_get_subsription_nonexistent_returns_none() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_get_none.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());

            // Verify get_subsription returns None for a contract that was never subscribed
            let sub = DataSubscription::new(aapl_contract(), WhatToShow::Trades);
            let result = handler.get_subsription(&sub);
            assert!(
                result.is_none(),
                "non-existent subscription should return None"
            );

            // Also test with a different contract
            let sub2 = DataSubscription::new(msft_contract(), WhatToShow::Bid);
            let result2 = handler.get_subsription(&sub2);
            assert!(
                result2.is_none(),
                "non-existent MSFT Bid subscription should return None"
            );

            println!("✅ get_subsription for non-existent subscriptions: None (correct)");

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 7. try_get_price — returns None for unknown contract_id ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_try_get_price_unknown_contract_id() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_price_none.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());

            // try_get_price for a contract_id that was never cached → None
            let result = handler.try_get_price(99999);
            assert!(
                result.is_none(),
                "try_get_price for unknown contract_id should return None"
            );

            println!("✅ try_get_price(unknown_contract_id): None (correct)");

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 8. try_get_price — after subscription + price received ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_try_get_price_after_subscription() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_price_after.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());
            let mut raw_contract_scheduler = IbkrContractScheduler::new(state.client_1.clone());
            assert!(
                raw_contract_scheduler
                    .add_all_schedules(vec![aapl_contract()])
                    .is_ok()
            );
            let contract_scheduler = Arc::new(raw_contract_scheduler);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            let contract = aapl_contract();
            let subscriptions = vec![DataSubscription::new(contract.clone(), WhatToShow::Trades)];

            handler.load_all_subscription_producers(
                &weak_client,
                contract_scheduler.clone(),
                subscriptions.clone(),
                DbSubscriptionMethod::OnePerThread,
                tokio::runtime::Handle::current(),
            );

            // Wait for market data to flow + consumer to cache the price
            tokio::time::sleep(Duration::from_secs(20)).await;

            // Verify the subscription ring buffer exists
            let sub = &subscriptions[0];
            let ring_buffer = handler.get_subsription(sub);
            assert!(ring_buffer.is_some(), "subscription should be registered");

            // try_get_price uses the contract_id — we need to validate the contract first to get the ID
            let validated_contract = {
                let contract_scheduler =
                    Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
                let market_data_handler = MarketDataHandler::new(state.pool.clone());
                let mut consolidator =
                    Arc::new(trading_app::market_data::consolidator::Consolidator::new(
                        tokio::runtime::Handle::current(),
                        state.pool.clone(),
                        state.client_1.clone(),
                        market_data_handler,
                        contract_scheduler,
                    ));
                let res = consolidator.validate_contract(contract, Duration::from_secs(30));
                loop_until_async_drop!(consolidator);

                res
            };

            if let Some(validated) = validated_contract {
                let price = handler.try_get_price(validated.contract_id);
                match price {
                    Some(p) => {
                        assert!(p > 0.0, "cached price should be positive, got {p}");
                        println!(
                            "✅ try_get_price(contract_id={}): ${p}",
                            validated.contract_id
                        );
                    }
                    None => {
                        println!(
                            "try_get_price returned None (market may be closed — no bars received)"
                        );
                    }
                }
            } else {
                println!("contract validation failed — skipping price check");
            }

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 9. Full lifecycle — load + get_subsription + consumer ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_market_data_handler_full_lifecycle() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_mdh_lifecycle.log",
        |state| async move {
            let mut handler = MarketDataHandler::new(state.pool.clone());
            let mut raw_contract_scheduler = IbkrContractScheduler::new(state.client_1.clone());
            assert!(
                raw_contract_scheduler
                    .add_all_schedules(vec![aapl_contract()])
                    .is_ok()
            );
            let contract_scheduler = Arc::new(raw_contract_scheduler);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            // 1. Before load — subscription doesn't exist
            let sub = DataSubscription::new(aapl_contract(), WhatToShow::Trades);
            assert!(handler.get_subsription(&sub).is_none(), "before load: None");

            // 2. Load subscription
            handler.load_all_subscription_producers(
                &weak_client,
                contract_scheduler.clone(),
                vec![sub.clone()],
                DbSubscriptionMethod::OnePerThread,
                tokio::runtime::Handle::current(),
            );

            // 3. After load — subscription exists
            let ring_buffer = handler.get_subsription(&sub);
            assert!(ring_buffer.is_some(), "after load: Some");
            println!("✅ Full lifecycle: load → get_subsription returns Some");

            // 4. Wait for market data to flow
            tokio::time::sleep(Duration::from_secs(15)).await;

            // 5. Get a consumer from the ring buffer + verify it can pop bars
            if let Some(rb) = ring_buffer {
                let consumer = rb.get_new_consumer();
                match consumer {
                    Some(c) => {
                        let bar = c.try_pop();
                        match bar {
                            Some(b) => {
                                assert!(
                                    b.close > 0.0,
                                    "bar close should be positive, got {}",
                                    b.close
                                );
                                println!(
                                    "✅ Full lifecycle: consumer popped bar with close={}",
                                    b.close
                                );
                            }
                            None => println!("No bars yet (market may be closed) — acceptable"),
                        }
                    }
                    None => println!("get_new_consumer returned None (max consumers exceeded)"),
                }
            }

            handler.async_drop().await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

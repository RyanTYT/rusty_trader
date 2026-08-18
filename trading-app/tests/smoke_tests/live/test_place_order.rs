//! Smoke test: place_order (live IBKR).
//! `OrderEngine::place_order` calls `client.next_order_id()` + `client.submit_order()`.
//! Returns `i32` (order_perm_id), not Result.
//! **WARNING: PLACES A REAL ORDER ON THE PAPER TRADING ACCOUNT.**
//! Requires: live IB Gateway + paper trading account. Run with: `cargo test --test smoke_tests test_place_order -- --ignored`

use ibapi::contracts::Contract;
use ibapi::orders::Action;
use ibapi::orders::order_builder::market_order;
use ibapi::prelude::SecurityType;
use std::sync::{Arc, Weak};
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};

use crate::live::init::{
    api_port_addr, ensure_strategy_row, ibkr_account, server_base_url, with_live_ibkr,
};

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading account — PLACES A REAL ORDER"]
async fn test_place_order_live() {
    with_live_ibkr(&ibkr_account(), "ibc_live.log", |state| async move {
        // place_order writes the optimistic open_order row (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let contract = Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        };

        let order = market_order(Action::Buy, 1.0);
        let order_ibkr = OrderIBKR::new(contract, order, -1);
        let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

        // place_order is sync, returns i32 (not Result)
        let order_perm_id = OrderEngine::place_order(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            &weak_client,
            order_ibkr,
        );

        // order_perm_id == 0 means client was dead; > 0 means order placed
        if order_perm_id > 0 {
            println!("Order placed successfully, perm_id: {order_perm_id}");
        } else {
            println!("place_order returned {order_perm_id} (client may be dead)");
        }
    })
    .await
    .expect("Failed to boot live IBKR");
}

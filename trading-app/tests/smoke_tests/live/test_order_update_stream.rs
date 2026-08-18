//! Smoke test: order_update_stream (live IBKR).
//! `OrderUpdateStreamController::new` spawns a thread calling `client.order_update_stream()`.
//! Requires: live IB Gateway + Postgres. Run with: `cargo test --test smoke_tests test_order_update_stream -- --ignored`

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_update_stream::controller::OrderUpdateStreamController;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::live::init::{
    api_port_addr, ensure_strategy_row, ibkr_account, server_base_url, with_live_ibkr,
};

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_order_update_stream_live() {
    with_live_ibkr(&ibkr_account(), "ibc_live.log", |state| async move {
        // The controller processes order events that write open_orders/
        // transactions/positions (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let noise = StrategyEnum::Noise(Noise::new(
            state.pool.clone(),
            tokio::runtime::Handle::current(),
        ));
        let mut strategy_map = HashMap::new();
        strategy_map.insert(noise.get_name(), noise);
        let strategy_map = Arc::new(strategy_map);

        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
        let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.master_client);

        let controller = OrderUpdateStreamController::new(
            state.pool.clone(),
            weak_client,
            strategy_map,
            Some("noise".to_string()),
            tokio::runtime::Handle::current(),
            order_store,
        );

        assert!(
            controller.is_some(),
            "OrderUpdateStreamController should start"
        );
        let controller = controller.unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("OrderUpdateStreamController ran for 5 seconds without panic");

        drop(controller);
    })
    .await
    .expect("Failed to boot live IBKR");
}

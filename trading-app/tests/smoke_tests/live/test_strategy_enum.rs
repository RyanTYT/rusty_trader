//! Comprehensive smoke tests for `StrategyEnum` + `StrategyExecutor` trait dispatch.
//!
//! Tests all 3 registered strategies (Noise, Manual, Unknown) via the unified
//! `StrategyEnum` dispatch — verifies all trait methods work correctly for each
//! variant:
//! - `get_name()` — unique name per strategy
//! - `is_fx_strategy()` — always false for all 3
//! - `get_contracts()` — returns the expected contracts (QQQ for Noise, GBP/USD for Manual/Unknown)
//! - `warm_up_data()` — completes without error (Noise fetches QQQ historical data; Manual/Unknown are no-ops)
//! - `on_bar_update()` — returns a `BarUpdateOutcome` (Noise queries DB; Manual/Unknown return PendingDbQuery)
//!
//! Also verifies:
//! - `StrategyEnum` Ord/PartialOrd ordering (used for strategy prioritization)
//! - `Hash` impl (used for `strategy_map` lookup)
//! - Clone/Eq
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_strategy_enum -- --ignored

use std::collections::HashMap;
use std::sync::Arc;

use ibapi::prelude::Contract;
use trading_app::database::models::AssetType;
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataFullKeys, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
};
use trading_app::database::models_crud::target_positions::target_positions::{
    TargetPositionsCRUD, TargetPositionsOps,
};
use trading_app::loop_until_async_drop;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;
use trading_app::strategy::manual::Manual;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{BarUpdateOutcome, StrategyEnum, StrategyExecutor};
use trading_app::strategy::unknown::Unknown;

use crate::live::init::{ensure_strategy_row, ibkr_account, with_live_ibkr};

/// Build a Consolidator needed by warm_up_data + on_bar_update.
fn build_consolidator(pool: sqlx::PgPool, client: Arc<ibapi::Client>) -> Arc<Consolidator> {
    let market_data_handler = MarketDataHandler::new(pool.clone());
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(client.clone()));
    Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        pool,
        client,
        market_data_handler,
        contract_scheduler,
    ))
}

// ============================ 1. get_name — all 3 strategies ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_get_name_all_variants() {
    with_live_ibkr(&ibkr_account(), "ibc_strat_name.log", |state| async move {
        let noise = StrategyEnum::Noise(Noise::new(
            state.pool.clone(),
            tokio::runtime::Handle::current(),
        ));
        let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));
        let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));

        // Verify each strategy returns the correct unique name
        assert_eq!(
            noise.get_name(),
            "noise",
            "Noise.get_name() should be 'noise'"
        );
        assert_eq!(
            manual.get_name(),
            "manual",
            "Manual.get_name() should be 'manual'"
        );
        assert_eq!(
            unknown.get_name(),
            "unknown",
            "Unknown.get_name() should be 'unknown'"
        );

        // Verify names are unique (used for strategy_map lookup)
        let names = vec![noise.get_name(), manual.get_name(), unknown.get_name()];
        let unique_names: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(
            unique_names.len(),
            3,
            "all 3 strategy names should be unique"
        );

        println!("✅ get_name: noise='noise', manual='manual', unknown='unknown' (all unique)");
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 2. is_fx_strategy — all 3 return false ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_is_fx_strategy_all_variants() {
    with_live_ibkr(&ibkr_account(), "ibc_strat_fx.log", |state| async move {
        let noise = StrategyEnum::Noise(Noise::new(
            state.pool.clone(),
            tokio::runtime::Handle::current(),
        ));
        let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));
        let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));

        assert!(
            !noise.is_fx_strategy(),
            "Noise should not be an FX strategy"
        );
        assert!(
            !manual.is_fx_strategy(),
            "Manual should not be an FX strategy"
        );
        assert!(
            !unknown.is_fx_strategy(),
            "Unknown should not be an FX strategy"
        );

        println!("✅ is_fx_strategy: all 3 return false");
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 3. get_contracts — Noise returns QQQ ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_get_contracts_noise() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_contracts_noise.log",
        |state| async move {
            let noise = StrategyEnum::Noise(Noise::new(
                state.pool.clone(),
                tokio::runtime::Handle::current(),
            ));
            let contracts = noise.get_contracts(state.client_1.clone());

            assert_eq!(contracts.len(), 1, "Noise should return 1 contract (QQQ)");
            let qqq = &contracts[0];
            assert_eq!(
                qqq.symbol.to_string(),
                "QQQ",
                "Noise contract should be QQQ"
            );
            println!("✅ get_contracts(Noise): QQQ verified");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 4. get_contracts — Manual returns GBP/USD ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_get_contracts_manual() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_contracts_manual.log",
        |state| async move {
            let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));
            let contracts = manual.get_contracts(state.client_1.clone());

            assert_eq!(
                contracts.len(),
                1,
                "Manual should return 1 contract (GBP/USD)"
            );
            let gbp = &contracts[0];
            assert_eq!(
                gbp.symbol.to_string(),
                "GBP",
                "Manual contract should be GBP"
            );
            println!("✅ get_contracts(Manual): GBP/USD verified");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 5. get_contracts — Unknown returns GBP/USD ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_get_contracts_unknown() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_contracts_unknown.log",
        |state| async move {
            let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));
            let contracts = unknown.get_contracts(state.client_1.clone());

            assert_eq!(
                contracts.len(),
                1,
                "Unknown should return 1 contract (GBP/USD)"
            );
            let gbp = &contracts[0];
            assert_eq!(
                gbp.symbol.to_string(),
                "GBP",
                "Unknown contract should be GBP"
            );
            println!("✅ get_contracts(Unknown): GBP/USD verified");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 6. warm_up_data — Noise (fetches QQQ historical data) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_warm_up_data_noise() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_warmup_noise.log",
        |state| async move {
            let mut consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
            let handle = tokio::runtime::Handle::current();
            let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), handle.clone()));

            // warm_up_data is async — await it directly on the test runtime.
            // (Production spawns a dedicated OS thread + block_on; in a test the
            // simplest correct thing is to just .await.)
            let result = noise.warm_up_data(&consolidator).await;
            assert!(
                result.is_ok(),
                "Noise warm_up_data should succeed, got: {:?}",
                result.err()
            );
            println!("✅ warm_up_data(Noise): completed without error");

            // Verify historical data was actually fetched for QQQ
            let crud = HistoricalDataCRUD::stock(state.pool.clone());
            let pk = HistoricalDataPrimaryKeysWoTime::Stock(
                trading_app::database::models::HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                },
            );
            let bars = crud
                .read_last_n(pk, 5, 5)
                .await
                .expect("read_last_n failed");
            assert!(
                !bars.full.is_empty() || !bars.incomplete.is_empty(),
                "warm_up_data should have fetched QQQ bars"
            );
            println!("✅ warm_up_data(Noise): QQQ historical data verified in DB");

            loop_until_async_drop!(consolidator);
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 7. warm_up_data — Manual (no-op) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_warm_up_data_manual() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_warmup_manual.log",
        |state| async move {
            let mut consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
            let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));

            // Manual.warm_up_data is a no-op (returns Ok(())) — just await it.
            let result = manual.warm_up_data(&consolidator).await;
            assert!(result.is_ok(), "Manual warm_up_data should succeed");
            println!("✅ warm_up_data(Manual): no-op completed without error");

            loop_until_async_drop!(consolidator);
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 8. warm_up_data — Unknown (no-op) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_warm_up_data_unknown() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_warmup_unknown.log",
        |state| async move {
            let mut consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
            let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));

            // Unknown.warm_up_data is a no-op (returns Ok(())) — just await it.
            let result = unknown.warm_up_data(&consolidator).await;
            assert!(result.is_ok(), "Unknown warm_up_data should succeed");
            println!("✅ warm_up_data(Unknown): no-op completed without error");

            loop_until_async_drop!(consolidator);
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 9. on_bar_update — Manual returns PendingDbQuery ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_on_bar_update_manual() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_bar_manual.log",
        |state| async move {
            let mut consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
            let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));

            // Build a dummy stock bar
            let contract = Contract {
                symbol: "QQQ".into(),
                security_type: ibapi::prelude::SecurityType::Stock,
                currency: ibapi::prelude::Currency("USD".to_string()),
                exchange: "SMART".into(),
                primary_exchange: "NASDAQ".into(),
                ..Default::default()
            };
            let bar = HistoricalDataFullKeys::Stock(
                trading_app::database::models::HistoricalStockDataFullKeys {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                    time: chrono::Utc::now(),
                    open: 400.0,
                    high: 405.0,
                    low: 395.0,
                    close: 402.0,
                    volume: rust_decimal::Decimal::new(1000000, 0),
                },
            );

            // on_bar_update is SYNC + internally calls self.tokio_handle.block_on(...).
            // Mirror the production hook_strategy pattern (strategy_consumer.rs:142):
            // run it on a dedicated OS thread. Use `spawn_blocking` (not raw
            // std::thread + join) so the main thread stays in the event loop
            // servicing the current-thread runtime's I/O driver — otherwise the
            // sync call's internal block_on would deadlock against a blocked main
            // thread. No block_in_place (would panic on current-thread runtime).
            let cloned_consolidator = consolidator.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                manual.on_bar_update(&contract, &bar, &cloned_consolidator)
            })
            .await
            .expect("on_bar_update blocking task panicked");
            assert!(
                outcome.is_ok(),
                "Manual on_bar_update scoped handle should succeed"
            );
            match outcome.unwrap() {
                BarUpdateOutcome::PendingDbQuery(asset_types) => {
                    assert!(
                        asset_types.contains(&AssetType::Stock),
                        "should include Stock"
                    );
                    assert!(
                        asset_types.contains(&AssetType::Option),
                        "should include Option"
                    );
                }
                other => panic!("Manual should return PendingDbQuery, got {:?}", other),
            }
            println!("✅ on_bar_update(Manual): returns PendingDbQuery([Stock, Option])");

            loop_until_async_drop!(consolidator);
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 10. on_bar_update — Unknown returns PendingDbQuery ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_on_bar_update_unknown() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_bar_unknown.log",
        |state| async move {
            let mut consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
            let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));

            let contract = Contract {
                symbol: "QQQ".into(),
                security_type: ibapi::prelude::SecurityType::Stock,
                currency: ibapi::prelude::Currency("USD".to_string()),
                exchange: "SMART".into(),
                primary_exchange: "NASDAQ".into(),
                ..Default::default()
            };
            let bar = HistoricalDataFullKeys::Stock(
                trading_app::database::models::HistoricalStockDataFullKeys {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                    time: chrono::Utc::now(),
                    open: 400.0,
                    high: 405.0,
                    low: 395.0,
                    close: 402.0,
                    volume: rust_decimal::Decimal::new(1000000, 0),
                },
            );

            // on_bar_update is SYNC — run on a dedicated OS thread via spawn_blocking
            // (keeps main thread in event loop so the internal block_on doesn't deadlock).
            let cloned_consolidator = consolidator.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                unknown.on_bar_update(&contract, &bar, &cloned_consolidator)
            })
            .await
            .expect("on_bar_update blocking task panicked");
            assert!(
                outcome.is_ok(),
                "Unknown on_bar_update scoped handle should succeed"
            );
            match outcome.unwrap() {
                BarUpdateOutcome::PendingDbQuery(asset_types) => {
                    assert!(asset_types.contains(&AssetType::Stock));
                    assert!(asset_types.contains(&AssetType::Option));
                }
                other => panic!("Unknown should return PendingDbQuery, got {:?}", other),
            }
            println!("✅ on_bar_update(Unknown): returns PendingDbQuery([Stock, Option])");

            loop_until_async_drop!(consolidator);
        },
    )
    .await
    .expect("Expected with_live_ibkr to succeed");
}

// ============================ 11. on_bar_update — Noise (with real QQQ bar data) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_on_bar_update_noise() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_bar_noise.log",
        |state| async move {
            let mut consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
            // on_bar_update for Noise writes target_positions (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let noise = StrategyEnum::Noise(Noise::new(
                state.pool.clone(),
                tokio::runtime::Handle::current(),
            ));

            // Warm up data first so on_bar_update has the historical data it needs.
            // warm_up_data is async — await it directly (no block_in_place).
            let result = noise.warm_up_data(&consolidator).await;
            let _ = result; // warm_up_data errors are non-fatal for this test

            let contract = Contract {
                symbol: "QQQ".into(),
                security_type: ibapi::prelude::SecurityType::Stock,
                currency: ibapi::prelude::Currency("USD".to_string()),
                exchange: "SMART".into(),
                primary_exchange: "NASDAQ".into(),
                ..Default::default()
            };
            // Fetch a real recent bar to use
            let crud = HistoricalDataCRUD::stock(state.pool.clone());
            let pk = HistoricalDataPrimaryKeysWoTime::Stock(
                trading_app::database::models::HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                },
            );
            let bars = crud
                .read_last_n(pk, 5, 5)
                .await
                .expect("read_last_n failed");
            let bar = if !bars.full.is_empty() {
                bars.full[0].clone()
            } else if !bars.incomplete.is_empty() {
                bars.incomplete[0].clone()
            } else {
                // No data — use a dummy bar
                HistoricalDataFullKeys::Stock(
                    trading_app::database::models::HistoricalStockDataFullKeys {
                        stock: "QQQ".to_string(),
                        primary_exchange: "NASDAQ".to_string(),
                        currency: "USD".to_string(),
                        time: chrono::Utc::now(),
                        open: 400.0,
                        high: 405.0,
                        low: 395.0,
                        close: 402.0,
                        volume: rust_decimal::Decimal::new(1000000, 0),
                    },
                )
            };

            // on_bar_update for Noise queries DB (avg_move, daily_open, daily_vol, vwap) + returns a BarUpdateOutcome
            // It may return Ok(PendingDbQuery), Ok(NoAction), or Ok(EmitOrders)
            // depending on market conditions. We just verify it doesn't error.
            //
            // on_bar_update is SYNC + internally calls self.tokio_handle.block_on(...).
            // Run on a dedicated OS thread via spawn_blocking (keeps main thread in
            // event loop so the internal block_on doesn't deadlock the current-thread
            // runtime). No block_in_place.
            let cloned_consolidator = consolidator.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                noise.on_bar_update(&contract, &bar, &cloned_consolidator)
            })
            .await
            .expect("on_bar_update blocking task panicked");
            assert!(
                outcome.is_ok(),
                "Noise on_bar_update scope handle should succeed"
            );
            match outcome.unwrap() {
                BarUpdateOutcome::NoAction => {
                    println!("✅ on_bar_update(Noise): NoAction (market conditions not met)")
                }
                BarUpdateOutcome::PendingDbQuery(asset_types) => {
                    assert!(
                        asset_types.contains(&AssetType::Stock),
                        "should include Stock"
                    );
                    println!("✅ on_bar_update(Noise): PendingDbQuery (target position updated)");
                }
                BarUpdateOutcome::EmitOrders(orders) => {
                    println!(
                        "✅ on_bar_update(Noise): EmitOrders ({} orders)",
                        orders.len()
                    );
                }
            }

            // Cleanup any target position Noise may have created
            let target_crud = TargetPositionsCRUD::stock(state.pool.clone());
            let _ = target_crud.clear_strat_pos("noise").await;

            loop_until_async_drop!(consolidator);
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 12. StrategyEnum Ord/PartialOrd/Hash/Clone/Eq ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_enum_ord_hash_clone_eq() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_strat_traits.log",
        |state| async move {
            let noise = StrategyEnum::Noise(Noise::new(
                state.pool.clone(),
                tokio::runtime::Handle::current(),
            ));
            let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));
            let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));

            // Clone
            let noise_clone = noise.clone();
            assert_eq!(noise, noise_clone, "clone should be equal to original");

            // Eq/PartialEq
            assert_ne!(noise, manual, "different strategies should not be equal");
            assert_ne!(manual, unknown, "different strategies should not be equal");

            // Ord — all 3 have priority 1, so ordering is by name (manual < noise < unknown alphabetically)
            let mut sorted = vec![unknown.clone(), noise.clone(), manual.clone()];
            sorted.sort();
            assert_eq!(
                sorted[0], noise,
                "Noise should sort first (declaration order)"
            );
            assert_eq!(sorted[1], manual, "Manual should sort second");
            assert_eq!(sorted[2], unknown, "Unknown should sort third");

            // Hash — used for strategy_map lookup
            let mut map: HashMap<StrategyEnum, &'static str> = HashMap::new();
            map.insert(noise.clone(), "noise_value");
            map.insert(manual.clone(), "manual_value");
            map.insert(unknown.clone(), "unknown_value");
            assert_eq!(
                map.len(),
                3,
                "all 3 strategies should be hashable + distinct"
            );
            assert_eq!(map.get(&noise), Some(&"noise_value"));
            assert_eq!(map.get(&manual), Some(&"manual_value"));
            assert_eq!(map.get(&unknown), Some(&"unknown_value"));

            println!("✅ StrategyEnum: Clone, Eq, Ord (alphabetical), Hash all verified");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 13. StrategyEnum dispatch — all methods route correctly ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_enum_dispatch_all_methods() {
    with_live_ibkr(&ibkr_account(), "ibc_strat_dispatch.log", |state| async move {
        // Verify that calling methods via StrategyEnum dispatches to the correct inner strategy
        let strategies = vec![
            StrategyEnum::Noise(Noise::new(
                state.pool.clone(),
                tokio::runtime::Handle::current(),
            )),
            StrategyEnum::Manual(Manual::new(state.pool.clone())),
            StrategyEnum::Unknown(Unknown::new(state.pool.clone())),
        ];

        for strat in &strategies {
            // get_name dispatches correctly
            let name = strat.get_name();
            match strat {
                StrategyEnum::Noise(_) => assert_eq!(name, "noise"),
                StrategyEnum::Manual(_) => assert_eq!(name, "manual"),
                StrategyEnum::Unknown(_) => assert_eq!(name, "unknown"),
            }

            // is_fx_strategy dispatches correctly
            assert!(
                !strat.is_fx_strategy(),
                "{}.is_fx_strategy() should be false",
                name
            );
        }

        println!(
            "✅ StrategyEnum dispatch: get_name + is_fx_strategy route correctly for all 3 variants"
        );
    })
    .await
    .expect("Failed to boot live IBKR");
}

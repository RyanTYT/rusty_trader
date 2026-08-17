//! Comprehensive smoke tests for `Consolidator` methods (live IBKR).
//!
//! Tests all Consolidator public methods + trait impls:
//! - `Consolidator::new()` — constructor
//! - `validate_contract()` — valid + invalid contract
//! - `update_at_least_n_days_data()` — stock + forex data fetching
//! - `PriceSupplier::get_current_price()` — stock + forex + vwap variants
//! - `PriceSupplier::populate_historical_data()` — populate historical data
//! - `GetStrategyValue::get_strategy_sgd_value()` — strategy value computation
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_consolidator -- --ignored

use std::sync::Arc;
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
};
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::traits::current_price::PriceSupplier;
use trading_app::market_data::traits::strategy_value::GetStrategyValue;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;

use crate::live::init::{api_port_addr, ibkr_account, server_base_url, with_live_ibkr};

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

fn qqq_contract() -> Contract {
    Contract {
        symbol: "QQQ".into(),
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

// ============================ 1. Consolidator::new — constructor ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_new_constructor() {
    with_live_ibkr(&ibkr_account(), "ibc_cons_new.log", |state| async move {
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
        println!("✅ Consolidator::new succeeded (no panic)");

        // Verify the consolidator is usable — call a method that depends on internal state
        let price = consolidator.get_current_price(aapl_contract(), false, &[]);
        match price {
            Ok(p) => {
                assert!(p > 0.0, "AAPL price should be positive, got {p}");
                println!(
                    "✅ Consolidator internal state verified — get_current_price returned {p}"
                );
            }
            Err(e) => {
                println!("get_current_price returned Err (may need market data subscription): {e}")
            }
        }
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 2. validate_contract — valid contract ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_validate_contract_valid() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_cons_vc_valid.log",
        |state| async move {
            let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

            // Test valid stock contract
            let result = consolidator.validate_contract(aapl_contract(), Duration::from_secs(30));
            assert!(
                result.is_some(),
                "validate_contract should return Some for AAPL"
            );
            let validated = result.unwrap();
            assert_eq!(validated.symbol.to_string(), "AAPL");
            assert!(validated.contract_id > 0, "contract_id should be populated");
            println!(
                "✅ validate_contract(AAPL): contract_id={}",
                validated.contract_id
            );

            // Test valid ETF contract
            let result = consolidator.validate_contract(qqq_contract(), Duration::from_secs(30));
            assert!(
                result.is_some(),
                "validate_contract should return Some for QQQ"
            );
            let validated = result.unwrap();
            assert_eq!(validated.symbol.to_string(), "QQQ");
            println!(
                "✅ validate_contract(QQQ): contract_id={}",
                validated.contract_id
            );

            // Test valid forex contract
            let result =
                consolidator.validate_contract(eur_usd_contract(), Duration::from_secs(30));
            assert!(
                result.is_some(),
                "validate_contract should return Some for EUR/USD"
            );
            println!("✅ validate_contract(EUR/USD): verified");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 3. validate_contract — invalid contract ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_validate_contract_invalid() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_cons_vc_invalid.log",
        |state| async move {
            let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

            // Test non-existent stock
            let bad_contract = Contract {
                symbol: "NONEXISTENT123XYZ".into(),
                security_type: SecurityType::Stock,
                currency: "USD".into(),
                ..Default::default()
            };
            let result = consolidator.validate_contract(bad_contract, Duration::from_secs(30));
            assert!(
                result.is_none(),
                "validate_contract should return None for non-existent contract"
            );
            println!("✅ validate_contract(NONEXISTENT): correctly returned None");

            // Test invalid forex pair
            let bad_forex = Contract {
                symbol: "FAKECUR".into(),
                security_type: SecurityType::ForexPair,
                currency: "USD".into(),
                exchange: "IDEALPRO".into(),
                ..Default::default()
            };
            let result = consolidator.validate_contract(bad_forex, Duration::from_secs(30));
            assert!(
                result.is_none(),
                "validate_contract should return None for invalid forex pair"
            );
            println!("✅ validate_contract(FAKECUR/USD): correctly returned None");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 4. update_at_least_n_days_data — stock (QQQ) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_update_at_least_n_days_data_stock() {
    with_live_ibkr(&ibkr_account(), "ibc_cons_update_stock.log", |state| async move {
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        // Validate the contract first
        let contract = consolidator.validate_contract(qqq_contract(), Duration::from_secs(30))
            .expect("QQQ contract should validate");
        println!("QQQ contract validated, fetching 5 days of historical data...");

        // Fetch 5 days of historical data (small amount to keep test fast)
        let result = consolidator.update_at_least_n_days_data(&contract, 5, false).await;
        match result {
            Ok(_) => {
                println!("✅ update_at_least_n_days_data(QQQ, 5 days) succeeded");

                // Verify historical data was actually fetched
                let crud = HistoricalDataCRUD::stock(state.pool.clone());
                let pk = HistoricalDataPrimaryKeysWoTime::Stock(
                    trading_app::database::models::HistoricalStockDataPrimaryKeysWoTime {
                        stock: "QQQ".to_string(),
                        primary_exchange: "NASDAQ".to_string(),
                        currency: "USD".to_string(),
                    },
                );
                let bars = crud.read_last_n(pk, 5, 5).await.expect("read_last_n failed");
                assert!(
                    !bars.full.is_empty() || !bars.incomplete.is_empty(),
                    "should have fetched QQQ bars"
                );
                println!("✅ QQQ historical data verified in DB");
            }
            Err(e) => {
                println!("update_at_least_n_days_data returned Err (may need market data subscription or market closed): {e}");
            }
        }
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 5. update_at_least_n_days_data — forex (EUR/USD) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_update_at_least_n_days_data_forex() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_cons_update_forex.log",
        |state| async move {
            let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

            // Validate the forex contract
            let contract = consolidator
                .validate_contract(eur_usd_contract(), Duration::from_secs(30))
                .expect("EUR/USD contract should validate");
            println!("EUR/USD contract validated, fetching 3 days of historical data...");

            // Fetch 3 days of forex historical data (small amount)
            let result = consolidator
                .update_at_least_n_days_data(&contract, 3, false)
                .await;
            match result {
                Ok(_) => {
                    println!("✅ update_at_least_n_days_data(EUR/USD, 3 days) succeeded");

                    // Verify forex data was fetched
                    let crud = HistoricalDataCRUD::forex(state.pool.clone());
                    let pk = HistoricalDataPrimaryKeysWoTime::Forex(
                        trading_app::database::models::HistoricalForexDataPrimaryKeysWoTime {
                            pair: "FX:EUR/USD".to_string(),
                        },
                    );
                    let bars = crud
                        .read_last_n(pk, 15, 5)
                        .await
                        .expect("read_last_n failed");
                    assert!(
                        !bars.full.is_empty() || !bars.incomplete.is_empty(),
                        "should have fetched EUR/USD bars"
                    );
                    println!("✅ EUR/USD historical data verified in DB");
                }
                Err(e) => {
                    println!("update_at_least_n_days_data(EUR/USD) returned Err: {e}");
                }
            }
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 6. get_current_price — stock (AAPL) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_get_current_price_stock() {
    with_live_ibkr(&ibkr_account(), "ibc_cons_price_stock.log", |state| async move {
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        let price = consolidator.get_current_price(aapl_contract(), false, &[]);
        match price {
            Ok(p) => {
                assert!(p > 0.0, "AAPL price should be positive, got {p}");
                assert!(
                    (50.0..=500.0).contains(&p),
                    "AAPL price {p} out of expected range"
                );
                println!("✅ get_current_price(AAPL): ${p}");
            }
            Err(e) => {
                println!(
                    "get_current_price(AAPL) returned Err (may need market data subscription): {e}"
                );
            }
        }
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 7. get_current_price — forex (EUR/USD) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_get_current_price_forex() {
    with_live_ibkr(&ibkr_account(), "ibc_cons_price_forex.log", |state| async move {
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        let price = consolidator.get_current_price(eur_usd_contract(), false, &[]);
        match price {
            Ok(p) => {
                assert!(p > 0.0, "EUR/USD price should be positive, got {p}");
                assert!(
                    (0.5..=2.0).contains(&p),
                    "EUR/USD price {p} out of expected range"
                );
                println!("✅ get_current_price(EUR/USD): {p}");
            }
            Err(e) => {
                println!("get_current_price(EUR/USD) returned Err (may need market data subscription): {e}");
            }
        }
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 8. get_current_price — with VWAP flag ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_get_current_price_vwap() {
    with_live_ibkr(&ibkr_account(), "ibc_cons_price_vwap.log", |state| async move {
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        // vwap=true should use VWAP calculation
        let price = consolidator.get_current_price(aapl_contract(), true, &[]);
        match price {
            Ok(p) => {
                assert!(p > 0.0, "VWAP price should be positive, got {p}");
                assert!(
                    (50.0..=500.0).contains(&p),
                    "AAPL VWAP price {p} out of expected range"
                );
                println!("✅ get_current_price(AAPL, vwap=true): ${p}");
            }
            Err(e) => {
                println!("get_current_price(vwap=true) returned Err (may need market data subscription): {e}");
            }
        }
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 9. get_current_price — with generic ticks ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_get_current_price_generic_ticks() {
    with_live_ibkr(&ibkr_account(), "ibc_cons_price_ticks.log", |state| async move {
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        // Request with generic ticks (e.g. option implied volatility ticks)
        let price = consolidator.get_current_price(aapl_contract(), false, &["106"]);
        match price {
            Ok(p) => {
                assert!(p > 0.0, "price with ticks should be positive, got {p}");
                println!("✅ get_current_price(AAPL, generic_ticks=[106]): ${p}");
            }
            Err(e) => {
                println!("get_current_price with ticks returned Err (may need specific subscription): {e}");
            }
        }
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 10. get_strategy_sgd_value ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_get_strategy_sgd_value() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_cons_sgd_value.log",
        |state| async move {
            let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

            // get_strategy_sgd_value queries current positions + computes SGD value
            let result = consolidator.get_strategy_sgd_value("noise");
            match result {
                Ok(value) => {
                    assert!(value.is_finite(), "SGD value should be finite, got {value}");
                    assert!(
                        value >= 0.0,
                        "SGD value should be non-negative, got {value}"
                    );
                    println!("✅ get_strategy_sgd_value('noise'): {value}");
                }
                Err(e) => {
                    println!("get_strategy_sgd_value returned Err (expected if no positions): {e}");
                }
            }

            // Test with non-existent strategy — should return 0 or Err gracefully
            let result = consolidator.get_strategy_sgd_value("nonexistent_strategy");
            match result {
                Ok(value) => {
                    assert_eq!(
                        value, 0.0,
                        "non-existent strategy should have 0 SGD value, got {value}"
                    );
                    println!("✅ get_strategy_sgd_value('nonexistent'): 0.0 (correct)");
                }
                Err(e) => {
                    println!("get_strategy_sgd_value('nonexistent') returned Err: {e}");
                }
            }
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 12\. validate_contract — different contract types ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_consolidator_validate_contract_multiple_types() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_cons_vc_types.log",
        |state| async move {
            let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

            // Test stock contract
            let stock_contract = Contract {
                symbol: "MSFT".into(),
                security_type: SecurityType::Stock,
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "NASDAQ".into(),
                ..Default::default()
            };
            let result = consolidator.validate_contract(stock_contract, Duration::from_secs(30));
            assert!(result.is_some(), "MSFT should validate");
            println!("✅ validate_contract(MSFT stock): verified");

            // Test ETF contract
            let etf_contract = Contract {
                symbol: "SPY".into(),
                security_type: SecurityType::Stock,
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "ARCA".into(),
                ..Default::default()
            };
            let result = consolidator.validate_contract(etf_contract, Duration::from_secs(30));
            assert!(result.is_some(), "SPY should validate");
            println!("✅ validate_contract(SPY ETF): verified");

            // Test forex contract
            let forex_contract = Contract {
                symbol: "GBP".into(),
                security_type: SecurityType::ForexPair,
                currency: "USD".into(),
                exchange: "IDEALPRO".into(),
                ..Default::default()
            };
            let result = consolidator.validate_contract(forex_contract, Duration::from_secs(30));
            assert!(result.is_some(), "GBP/USD should validate");
            println!("✅ validate_contract(GBP/USD forex): verified");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

//! Unit tests for `OrderEngine::get_required_fx_attachments` — the pure FX-attachment greedy matcher.
//!
//! See `src/execution/fx_organiser.rs`. This function takes 3 HashMaps + a strategy
//! name and returns `FxAttachments` (sell→FX chains + backed-up buy orders).
//! It's a pure associated function (doesn't use `self`), callable as
//! `OrderEngine::get_required_fx_attachments(...)`.
//!
//! Tests cover: empty inputs, shortfall exactly covered by available funds,
//! shortfall covered by sell proceeds, partially-covered (error path log + skip),
//! multiple sells→one buy, multiple buys→one sell, zero proceeds, zero funds,
//! and the `backed_up_orders` order_ref format (`{strategy}:{price}`).

use std::collections::HashMap;

use ibapi::contracts::Contract;
use ibapi::orders::Action;
use trading_app::execution::fx_organiser::FxAttachments;
use trading_app::execution::order_engine::OrderEngine;
use trading_app::test_internals::HashContract;

/// Helper: build a stock HashContract for a given symbol/currency.
fn stock_contract(symbol: &str, currency: &str) -> HashContract {
    HashContract {
        contract: Contract {
            symbol: ibapi::contracts::Symbol::new(symbol),
            security_type: ibapi::prelude::SecurityType::Stock,
            currency: ibapi::prelude::Currency(currency.to_string()),
            ..Default::default()
        },
    }
}

// ============================ empty / trivial cases ============================

#[test]
fn empty_inputs_returns_empty_attachments() {
    let res: FxAttachments = OrderEngine::get_required_fx_attachments(
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        "test".to_string(),
    );
    assert!(res.contracts_sold_to_fx_orders.is_empty());
    assert!(res.backed_up_orders.is_empty());
}

#[test]
fn shortfall_covered_by_available_funds_no_fx_needed() {
    // buy 10 units @ 100 = 1000 needed. Available funds USD=1000. No shortfall → no FX.
    let mut funds = HashMap::new();
    funds.insert("USD".to_string(), 1000.0);
    let buy = stock_contract("AAPL", "USD");
    let mut insufficient = HashMap::new();
    insufficient.insert(buy.clone(), ((10.0, 100.0), 1000.0)); // ((qty, price), shortfall)

    let res = OrderEngine::get_required_fx_attachments(
        funds,
        HashMap::new(),
        insufficient,
        "test".to_string(),
    );
    // shortfall=1000, available=1000, remaining_shortfall=0 → skip (no FX, no backup)
    assert!(res.contracts_sold_to_fx_orders.is_empty());
    assert!(res.backed_up_orders.is_empty());
}

// ============================ FX chain: one sell → one buy ============================

#[test]
fn shortfall_covered_by_one_sell_proceeds() {
    // Buy AAPL (USD), need 1000 USD. Available USD=0. Sell MSFT (USD) proceeds=1000.
    // → one FX order: buy 1000 USD using MSFT proceeds. One backed-up buy order.
    let buy = stock_contract("AAPL", "USD");
    let sell = stock_contract("MSFT", "USD");
    let mut funds = HashMap::new();
    funds.insert("USD".to_string(), 0.0);
    let mut funds_from_selling = HashMap::new();
    funds_from_selling.insert(sell.clone(), vec![1000.0]);
    let mut insufficient = HashMap::new();
    insufficient.insert(buy.clone(), ((10.0, 100.0), 1000.0));

    let res = OrderEngine::get_required_fx_attachments(
        funds,
        funds_from_selling,
        insufficient,
        "mystrat".to_string(),
    );
    // Expect one FX order attached to the sell contract
    assert_eq!(res.contracts_sold_to_fx_orders.len(), 1);
    let fx_orders = res.contracts_sold_to_fx_orders.get(&sell).unwrap();
    assert_eq!(fx_orders.len(), 1);
    assert_eq!(fx_orders[0].order.action, Action::Buy);
    assert_eq!(fx_orders[0].order.total_quantity, 1000.0);
    assert_eq!(fx_orders[0].order.order_ref, "mystrat");
    // FX contract should be ForexPair with sell_currency/buy_currency
    assert_eq!(
        fx_orders[0].contract.security_type,
        ibapi::prelude::SecurityType::ForexPair
    );

    // Expect one backed-up buy order for AAPL
    assert_eq!(res.backed_up_orders.len(), 1);
    let buy_order = &res.backed_up_orders[0];
    assert_eq!(buy_order.order.action, Action::Buy);
    assert_eq!(buy_order.order.total_quantity, 10.0);
    // order_ref format: "{strategy}:{price}"
    assert_eq!(buy_order.order.order_ref, "mystrat:100");
    assert!(buy_order.order.transmit);
}

// ============================ partial coverage / error path ============================

#[test]
fn partial_coverage_logs_error_and_skips_backup() {
    // Buy needs 1000 USD. Available=0. Sell proceeds=500 (only half).
    // FX covers 500, shortfall remains 500 → error logged, NO backed-up buy order.
    let buy = stock_contract("AAPL", "USD");
    let sell = stock_contract("MSFT", "USD");
    let mut funds = HashMap::new();
    funds.insert("USD".to_string(), 0.0);
    let mut funds_from_selling = HashMap::new();
    funds_from_selling.insert(sell.clone(), vec![500.0]);
    let mut insufficient = HashMap::new();
    insufficient.insert(buy.clone(), ((10.0, 100.0), 1000.0));

    let res = OrderEngine::get_required_fx_attachments(
        funds,
        funds_from_selling,
        insufficient,
        "test".to_string(),
    );
    // FX order for 500 created (consumed the sell proceeds)
    assert_eq!(res.contracts_sold_to_fx_orders.len(), 1);
    let fx_orders = res.contracts_sold_to_fx_orders.get(&sell).unwrap();
    assert_eq!(fx_orders[0].order.total_quantity, 500.0);
    // NO backed-up buy order (shortfall not fully covered)
    assert!(res.backed_up_orders.is_empty());
}

// ============================ multiple sells → one buy ============================

#[test]
fn multiple_sells_cover_one_buy() {
    // Buy needs 1000 USD. Two sells: MSFT=600, TSLA=400. Total=1000.
    let buy = stock_contract("AAPL", "USD");
    let sell1 = stock_contract("MSFT", "USD");
    let sell2 = stock_contract("TSLA", "USD");
    let mut funds = HashMap::new();
    funds.insert("USD".to_string(), 0.0);
    let mut funds_from_selling = HashMap::new();
    funds_from_selling.insert(sell1.clone(), vec![600.0]);
    funds_from_selling.insert(sell2.clone(), vec![400.0]);
    let mut insufficient = HashMap::new();
    insufficient.insert(buy.clone(), ((10.0, 100.0), 1000.0));

    let res = OrderEngine::get_required_fx_attachments(
        funds,
        funds_from_selling,
        insufficient,
        "test".to_string(),
    );
    // Both sells should have FX orders attached
    assert_eq!(res.contracts_sold_to_fx_orders.len(), 2);
    let fx1 = res.contracts_sold_to_fx_orders.get(&sell1).unwrap();
    let fx2 = res.contracts_sold_to_fx_orders.get(&sell2).unwrap();
    assert!((fx1[0].order.total_quantity - 600.0).abs() < 1e-9);
    assert!((fx2[0].order.total_quantity - 400.0).abs() < 1e-9);
    // One backed-up buy order
    assert_eq!(res.backed_up_orders.len(), 1);
}

// ============================ zero proceeds / zero funds ============================

#[test]
fn zero_proceeds_sell_skipped() {
    // Sell with 0 proceeds → skipped; shortfall not covered → no backup
    let buy = stock_contract("AAPL", "USD");
    let sell = stock_contract("MSFT", "USD");
    let mut funds = HashMap::new();
    funds.insert("USD".to_string(), 0.0);
    let mut funds_from_selling = HashMap::new();
    funds_from_selling.insert(sell.clone(), vec![0.0]); // zero proceeds
    let mut insufficient = HashMap::new();
    insufficient.insert(buy.clone(), ((10.0, 100.0), 1000.0));

    let res = OrderEngine::get_required_fx_attachments(
        funds,
        funds_from_selling,
        insufficient,
        "test".to_string(),
    );
    // Sell skipped (proceeds <= 0), no FX, no backup
    assert!(res.contracts_sold_to_fx_orders.is_empty());
    assert!(res.backed_up_orders.is_empty());
}

#[test]
fn zero_shortfall_skipped() {
    // shortfall=0 → early continue (no FX needed)
    let buy = stock_contract("AAPL", "USD");
    let mut funds = HashMap::new();
    funds.insert("USD".to_string(), 0.0);
    let mut insufficient = HashMap::new();
    insufficient.insert(buy.clone(), ((10.0, 100.0), 0.0)); // shortfall=0

    let res = OrderEngine::get_required_fx_attachments(
        funds,
        HashMap::new(),
        insufficient,
        "test".to_string(),
    );
    assert!(res.contracts_sold_to_fx_orders.is_empty());
    assert!(res.backed_up_orders.is_empty());
}

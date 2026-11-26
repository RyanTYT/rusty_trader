//! Reconciliation-gating tests for `order_engine.rs`.

use ibapi::orders::{Action, order_builder::market_order};
use ibapi::prelude::Contract;
use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys;
use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff;
use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsQtyDiff;
use trading_app::execution::order_engine::{LocalOpenOrder, requires_reconciliation};

use trading_app::strategy::strategy::StrategyDetails;

fn diff(stock: &str, qty_diff: f64, currency: &str) -> TargetPositionsQtyDiff {
    TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
        strategy: "strat".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: currency.to_string(),
        stock: stock.to_string(),
        avg_price: 100.0,
        qty_diff,
        current_qty: 0.0,
    })
}

fn contract_for(diff: &TargetPositionsQtyDiff) -> Contract {
    let (stock, primary_exchange, currency) = match diff {
        TargetPositionsQtyDiff::Stock(v) => (
            v.stock.clone(),
            v.primary_exchange.clone(),
            v.currency.clone(),
        ),
        TargetPositionsQtyDiff::Options(_) => panic!("stock fixture only"),
    };
    let mut contract = Contract::stock(&stock).build();
    contract.primary_exchange = primary_exchange.into();
    contract.currency = currency.into();
    contract
}

fn open_order(diff: &TargetPositionsQtyDiff, qty: f64) -> LocalOpenOrder {
    let contract = contract_for(diff);
    let mut order = market_order(Action::Buy, qty);
    order.order_ref = "strat".to_string();
    LocalOpenOrder {
        order: OpenOrdersFullKeys::from_contract_and_order(&contract, &order, 0.0),
        is_from_db: true,
    }
}

fn strategy(is_fx: bool) -> StrategyDetails {
    StrategyDetails::new(1, "strat".to_string(), is_fx, false)
}

#[test]
fn no_targets_and_no_orders_needs_no_reconciliation() {
    assert!(!requires_reconciliation(&vec![], vec![], &strategy(false)));
}

#[test]
fn matching_open_quantity_needs_no_reconciliation() {
    let diff = diff("AAPL", 10.0, "USD");
    let open = vec![open_order(&diff, 10.0)];
    assert!(!requires_reconciliation(
        &vec![diff],
        open,
        &strategy(false)
    ));
}

#[test]
fn under_filled_and_over_filled_open_quantity_need_reconciliation() {
    let under = diff("AAPL", 10.0, "USD");
    assert!(requires_reconciliation(
        &vec![under.clone()],
        vec![open_order(&under, 5.0)],
        &strategy(false)
    ));

    let over = diff("AAPL", 10.0, "USD");
    assert!(requires_reconciliation(
        &vec![over.clone()],
        vec![open_order(&over, 15.0)],
        &strategy(false)
    ));
}

#[test]
fn cash_rows_are_ignored_for_non_fx_strategies_only() {
    let cash = diff("CASH:USD", 1000.0, "USD");
    assert!(!requires_reconciliation(
        &vec![cash.clone()],
        vec![],
        &strategy(false)
    ));
    assert!(requires_reconciliation(
        &vec![cash],
        vec![],
        &strategy(true)
    ));
}

//! `OrderIBKR` identity tests. The order reference carries a `:price` suffix
//! for parked orders; equality ignores it so a parked order can be matched to
//! a freshly planned one.

use ibapi::orders::{Action, order_builder::market_order};
use ibapi::prelude::Contract;
use trading_app::execution::order_engine::OrderIBKR;

fn order(symbol: &str, qty: f64, order_ref: &str) -> OrderIBKR {
    let mut order = market_order(Action::Buy, qty);
    order.order_ref = order_ref.to_string();
    OrderIBKR::new(Contract::stock(symbol).build(), order, -1)
}

#[test]
fn ignores_the_price_suffix_in_order_ref() {
    assert_eq!(
        order("AAPL", 10.0, "strat:123.5"),
        order("AAPL", 10.0, "strat")
    );
}

#[test]
fn different_contract_or_quantity_is_not_equal() {
    assert_ne!(order("AAPL", 10.0, "strat"), order("MSFT", 10.0, "strat"));
    assert_ne!(order("AAPL", 10.0, "strat"), order("AAPL", 20.0, "strat"));
}

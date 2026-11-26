//! `OrderStore` tests. The store is a redb file, so each test gets its own
//! temporary path; `ORDERS_FILE_PATH` is process-global and the tests are
//! serialized with a mutex.

use std::collections::HashMap;

use ibapi::orders::{Action, order_builder::market_order};
use ibapi::prelude::Contract;
use trading_app::execution::order_engine::OrderIBKR;

use crate::support::open_store;

fn order(symbol: &str, qty: f64, price_suffix: Option<f64>) -> OrderIBKR {
    let mut order = market_order(Action::Buy, qty);
    order.order_ref = match price_suffix {
        Some(price) => format!("strat:{price}"),
        None => "strat".to_string(),
    };
    OrderIBKR::new(Contract::stock(symbol).build(), order, -1)
}

#[test]
fn store_load_and_delete_roundtrip() {
    let (_dir, _guard, store) = open_store();
    let stored = order("AAPL", 10.0, None);

    store.store_orders("s", &[stored]).unwrap();
    let loaded = store.load_orders("s").unwrap().expect("the key exists");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0], order("AAPL", 10.0, None));

    assert!(store.delete_orders("s").unwrap());
    assert!(store.load_orders("s").unwrap().is_none());
    assert!(!store.delete_orders("s").unwrap());
}

#[test]
fn insert_or_update_dedupes_on_contract() {
    let (_dir, _guard, store) = open_store();
    store
        .insert_or_update_orders("s", &[order("AAPL", 10.0, None)])
        .unwrap();
    store
        .insert_or_update_orders("s", &[order("AAPL", 20.0, None), order("MSFT", 5.0, None)])
        .unwrap();

    let loaded = store.load_orders("s").unwrap().unwrap();
    assert_eq!(loaded.len(), 2);
    let mut quantities: Vec<f64> = loaded.iter().map(|o| o.order.total_quantity).collect();
    quantities.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(quantities, vec![5.0, 20.0]);
}

#[test]
fn remove_orders_uses_suffix_insensitive_equality() {
    let (_dir, _guard, store) = open_store();
    store
        .store_orders("s", &[order("AAPL", 10.0, Some(100.0))])
        .unwrap();

    store
        .remove_orders("s", &[order("AAPL", 10.0, None)])
        .unwrap();
    let kept = store.load_orders("s").unwrap().unwrap();
    assert!(kept.is_empty());
}

#[test]
fn remove_orders_on_a_missing_key_is_ok() {
    let (_dir, _guard, store) = open_store();
    store
        .remove_orders("missing", &[order("AAPL", 10.0, None)])
        .unwrap();
    assert!(store.load_orders("missing").unwrap().is_none());
}

#[test]
fn remove_all_valid_orders_respects_the_budget() {
    let (_dir, _guard, store) = open_store();
    store
        .store_orders(
            "s",
            &[
                order("AAPL", 5.0, Some(10.0)),
                order("MSFT", 5.0, Some(20.0)),
            ],
        )
        .unwrap();

    let ready = store.remove_all_valid_orders("s", "USD", 60.0).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].order.order_ref, "strat:10");

    let kept = store.load_orders("s").unwrap().unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].order.order_ref, "strat:20");
}

#[test]
fn remove_all_valid_orders_does_nothing_without_budget() {
    let (_dir, _guard, store) = open_store();
    store
        .store_orders("s", &[order("AAPL", 5.0, Some(10.0))])
        .unwrap();

    assert!(
        store
            .remove_all_valid_orders("s", "USD", 0.0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.load_orders("s").unwrap().unwrap().len(), 1);
}

#[test]
fn sync_full_map_replaces_the_whole_store() {
    let (_dir, _guard, store) = open_store();
    let first = HashMap::from([
        ("a".to_string(), vec![order("AAPL", 1.0, None)]),
        ("b".to_string(), vec![order("MSFT", 1.0, None)]),
    ]);
    store.sync_full_map(&first).unwrap();
    assert_eq!(store.load_all_to_map().unwrap().len(), 2);

    let second = HashMap::from([("b".to_string(), vec![order("MSFT", 2.0, None)])]);
    store.sync_full_map(&second).unwrap();

    let loaded = store.load_all_to_map().unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded.contains_key("b"));
    assert!(!loaded.contains_key("a"));
}

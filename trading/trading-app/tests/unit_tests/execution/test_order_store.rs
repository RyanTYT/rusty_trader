//! Unit tests for `OrderStore` — redb-backed order persistence.
//!
//! See `src/execution/fx_backed_up_order.rs`. Tests cover:
//! - `store_orders` → `load_orders` roundtrip
//! - `load_all_to_map`
//! - `delete_orders`
//! - empty-key behavior
//! - overwrite behavior
//!
//! NOTE: `OrderStore::open()` reads the `ORDERS_FILE_PATH` env var. Tests
//! set this to a tempfile path. A mutex serializes tests that touch the env var.

use std::sync::Mutex;

use ibapi::contracts::Contract;
use ibapi::orders::order_builder::market_order;
use ibapi::prelude::SecurityType;
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::OrderIBKR;

/// Serialize tests that touch the ORDERS_FILE_PATH env var.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

/// Helper: create an OrderStore backed by a fresh tempfile.
fn fresh_store() -> (tempfile::TempPath, OrderStore) {
    let _guard = ENV_MUTEX.lock().unwrap();
    let temp = tempfile::NamedTempFile::new().unwrap();
    let path = temp.into_temp_path();
    // SAFETY: ENV_MUTEX serializes all tests that touch this env var.
    unsafe {
        std::env::set_var("ORDERS_FILE_PATH", path.to_str().unwrap());
    }
    let store = OrderStore::open().expect("Expected OrderStore::open to succeed");
    // Don't keep the env var set for other tests
    unsafe {
        std::env::remove_var("ORDERS_FILE_PATH");
    }
    (path, store)
}

fn sample_order(symbol: &str) -> OrderIBKR {
    OrderIBKR::new(
        Contract {
            symbol: symbol.into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            ..Default::default()
        },
        market_order(ibapi::orders::Action::Buy, 10.0),
        -1,
    )
}

// ============================ store/load roundtrip ============================

#[test]
fn store_and_load_orders_roundtrip() {
    let (_path, store) = fresh_store();
    let orders = vec![sample_order("AAPL"), sample_order("MSFT")];
    store
        .store_orders("test_key", &orders)
        .expect("store_orders failed");

    let loaded = store
        .load_orders("test_key")
        .expect("load_orders failed")
        .expect("expected Some orders");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].contract.symbol.to_string(), "AAPL");
    assert_eq!(loaded[1].contract.symbol.to_string(), "MSFT");
}

// ============================ load missing key returns None ============================

#[test]
fn load_missing_key_returns_none() {
    let (_path, store) = fresh_store();
    let result = store.load_orders("nonexistent").expect("load_orders failed");
    assert!(result.is_none(), "missing key should return None");
}

// ============================ overwrite ============================

#[test]
fn store_orders_overwrites_existing_key() {
    let (_path, store) = fresh_store();
    let orders1 = vec![sample_order("AAPL")];
    store.store_orders("key", &orders1).unwrap();
    let loaded1 = store.load_orders("key").unwrap().unwrap();
    assert_eq!(loaded1.len(), 1);

    let orders2 = vec![sample_order("MSFT"), sample_order("GOOG")];
    store.store_orders("key", &orders2).unwrap();
    let loaded2 = store.load_orders("key").unwrap().unwrap();
    assert_eq!(loaded2.len(), 2, "should be overwritten with 2 orders");
    assert_eq!(loaded2[0].contract.symbol.to_string(), "MSFT");
}

// ============================ delete_orders ============================

#[test]
fn delete_orders_removes_key() {
    let (_path, store) = fresh_store();
    store.store_orders("key", &[sample_order("AAPL")]).unwrap();
    assert!(store.load_orders("key").unwrap().is_some());

    let deleted = store.delete_orders("key").expect("delete_orders failed");
    assert!(deleted, "should return true for deleted key");

    assert!(store.load_orders("key").unwrap().is_none());
}

#[test]
fn delete_nonexistent_key_returns_false() {
    let (_path, store) = fresh_store();
    let deleted = store.delete_orders("nonexistent").expect("delete_orders failed");
    assert!(!deleted, "deleting nonexistent key should return false");
}

// ============================ empty key ============================

#[test]
fn store_empty_orders_vec() {
    let (_path, store) = fresh_store();
    let empty: Vec<OrderIBKR> = vec![];
    store.store_orders("empty_key", &empty).unwrap();
    let loaded = store.load_orders("empty_key").unwrap().unwrap();
    assert!(loaded.is_empty(), "empty vec should roundtrip to empty vec");
}

// ============================ load_all_to_map ============================

#[test]
fn load_all_to_map_returns_all_keys() {
    let (_path, store) = fresh_store();
    store.store_orders("key1", &[sample_order("AAPL")]).unwrap();
    store.store_orders("key2", &[sample_order("MSFT")]).unwrap();

    let map = store.load_all_to_map().expect("load_all_to_map failed");
    assert_eq!(map.len(), 2);
    assert!(map.contains_key("key1"));
    assert!(map.contains_key("key2"));
}

#[test]
fn load_all_to_map_empty_store() {
    let (_path, store) = fresh_store();
    let map = store.load_all_to_map().expect("load_all_to_map failed");
    assert!(map.is_empty(), "empty store should return empty map");
}

//! Tests for `src/market_data/traits/contracts.rs`.
//!
//! Covered: `impl<T> ContractBroker for Arc<T>` (:14).
//! Not covered: the `ContractBroker for Consolidator` impl (:23) and
//! `Consolidator::_get_contract_details` (:59) — both require a live
//! `ibapi::Client` (contract-details round trip), i.e. integration-only.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use ibapi::contracts::Contract;
use trading_app::market_data::traits::contracts::ContractBroker;

fn stock(symbol: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        ..Default::default()
    }
}

struct FakeBroker {
    result: Option<Contract>,
    calls: AtomicUsize,
    seen: Mutex<Vec<Contract>>,
}

impl FakeBroker {
    fn returning(result: Option<Contract>) -> Self {
        Self {
            result,
            calls: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl ContractBroker for FakeBroker {
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(contract);
        self.result.clone()
    }
}

#[test]
fn arc_contract_broker_forwards_some_result_to_inner() {
    let validated = Contract {
        contract_id: 265598,
        ..stock("AAPL")
    };
    let inner = Arc::new(FakeBroker::returning(Some(validated.clone())));
    let input = stock("AAPL");

    let out =
        <Arc<FakeBroker> as ContractBroker>::get_validated_contract(&inner, input.clone());

    assert_eq!(out, Some(validated));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(*inner.seen.lock().unwrap(), vec![input]);
}

#[test]
fn arc_contract_broker_forwards_none_result_to_inner() {
    let inner = Arc::new(FakeBroker::returning(None));
    let input = stock("NOT_REAL");

    let out =
        <Arc<FakeBroker> as ContractBroker>::get_validated_contract(&inner, input.clone());

    assert_eq!(out, None);
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(*inner.seen.lock().unwrap(), vec![input]);
}

#[test]
fn arc_contract_broker_forwards_every_call() {
    let validated = Contract {
        contract_id: 7,
        ..stock("MSFT")
    };
    let inner = Arc::new(FakeBroker::returning(Some(validated.clone())));

    let first =
        <Arc<FakeBroker> as ContractBroker>::get_validated_contract(&inner, stock("MSFT"));
    let second =
        <Arc<FakeBroker> as ContractBroker>::get_validated_contract(&inner, stock("MSFT"));

    assert_eq!(first, Some(validated.clone()));
    assert_eq!(second, Some(validated));
    // No caching or short-circuiting at the Arc layer.
    assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    assert_eq!(inner.seen.lock().unwrap().len(), 2);
}

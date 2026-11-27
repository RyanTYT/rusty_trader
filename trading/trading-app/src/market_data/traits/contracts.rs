//! Contract validation behind the [`ContractBroker`] port.
//!
//! Strategies and event handlers hold a raw [`Contract`] (often built from a
//! symbol alone). Before using it for an order or a position key, it must be
//! resolved against IBKR, which fills in `contract_id`, `local_symbol`,
//! trading hours and the exact exchange. [`ContractBroker`] is that seam:
//! live code validates through [`Consolidator`], tests and the backtester
//! substitute a fake that returns a canned contract or `None`.

use std::{sync::Arc, time::Duration};

use ibapi::contracts::ContractDetails;
use ibapi::{Client, contracts::Contract};
use moka::sync::Cache;

use crate::{helpers::sync_timeout::timeout_panic, market_data::consolidator::Consolidator};

/// Resolves a raw contract into the broker's canonical one.
///
/// Implementations must be cheap to call repeatedly: this sits on the fill and
/// open-order paths. Return `None` when the contract cannot be resolved (unknown
/// symbol, ambiguous option, IBKR error); callers treat that as "reject".
pub trait ContractBroker {
    /// Returns the validated contract, or `None` if it cannot be resolved.
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract>;
}

impl<T> ContractBroker for Arc<T>
where
    T: ContractBroker,
{
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract> {
        (**self).get_validated_contract(contract)
    }
}

#[cfg(not(feature = "backtest"))]
impl ContractBroker for Consolidator {
    /// Same as [`Consolidator::validate_contract`] with a 10 s timeout.
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract> {
        self.validate_contract(contract, Duration::from_secs(10))
    }
}

impl Consolidator {
    /// Full contract details (not just the contract) with an explicit timeout.
    ///
    /// # Panics
    ///
    /// Panics if the IBKR request does not complete within `timeout_duration`;
    /// the helper used is `timeout_panic`, which is deliberate on this path so a
    /// hung gateway surfaces loudly rather than silently returning `None`.
    #[cfg(not(feature = "backtest"))]
    pub fn get_contract_details(
        &self,
        contract: Contract,
        timeout_duration: Duration,
    ) -> Option<ContractDetails> {
        Self::_get_contract_details(
            self.client.clone(),
            Some(self.memoised_contract_details.clone()),
            contract,
            timeout_duration,
        )
    }

    /// Resolves a contract to its canonical form with an explicit timeout.
    ///
    /// Thin wrapper over [`Consolidator::get_contract_details`] returning only
    /// `ContractDetails::contract`.
    ///
    /// # Panics
    ///
    /// Same as [`Consolidator::get_contract_details`].
    #[cfg(not(feature = "backtest"))]
    pub fn validate_contract(
        &self,
        contract: Contract,
        timeout_duration: Duration,
    ) -> Option<Contract> {
        Self::_get_contract_details(
            self.client.clone(),
            Some(self.memoised_contract_details.clone()),
            contract,
            timeout_duration,
        )
        .map(|v| v.contract)
    }

    /// Shared resolution used by both public entry points; memoised by
    /// `contract_id`.
    ///
    /// - A non-zero `contract_id` already present in `memoised_contracts` is
    ///   returned without a network call.
    /// - Otherwise IBKR is asked for contract details; an empty response maps to
    ///   `None`, and the **first** returned detail is cached under its
    ///   `contract_id` and returned. (An ambiguous symbol therefore resolves to
    ///   the first match rather than an error.)
    /// - IBKR errors are logged and mapped to `None`.
    ///
    /// # Panics
    ///
    /// Panics when the request exceeds `timeout_duration` or the worker thread
    /// panics (see `timeout_panic`).
    pub(crate) fn _get_contract_details(
        client: Arc<Client>,
        memoised_contracts: Option<Arc<Cache<i32, ContractDetails>>>,
        contract: Contract,
        timeout_duration: Duration,
    ) -> Option<ContractDetails> {
        if contract.contract_id != 0
            && memoised_contracts
                .as_ref()
                .is_some_and(|v| v.contains_key(&contract.contract_id))
        {
            return memoised_contracts
                .unwrap()
                .get(&contract.contract_id)
                .map(|v| v.clone());
        }

        let symbol = contract.symbol.clone();
        match timeout_panic(timeout_duration, move || client.contract_details(&contract)) {
            Ok(validated_contracts) => {
                if validated_contracts.len() == 0 {
                    return None;
                }
                let contract = validated_contracts.into_iter().take(1).next().unwrap();
                if let Some(v) = memoised_contracts {
                    v.insert(contract.contract.contract_id, contract.clone());
                }
                return Some(contract);
            }
            Err(e) => {
                tracing::error!(
                    message=%format!(
                        "Error occurred requesting contract details for {}: {}",
                        symbol,
                        e
                    )
                );
                return None;
            }
        }
    }
}

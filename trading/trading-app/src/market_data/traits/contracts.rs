use std::{sync::Arc, time::Duration};

use ibapi::contracts::ContractDetails;
use ibapi::{Client, contracts::Contract};
use moka::sync::Cache;

use crate::{helpers::sync_timeout::timeout_panic, market_data::consolidator::Consolidator};

pub trait ContractBroker {
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
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract> {
        self.validate_contract(contract, Duration::from_secs(10))
    }
}

impl Consolidator {
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

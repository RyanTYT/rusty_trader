use std::collections::HashMap;

use async_trait::async_trait;
use ibapi::prelude::SecurityType;

use crate::{
    database::models_crud::current_stock_positions::get_specific_current_stock_positions_crud,
    helpers::contract::{HashContract, get_contract_from_local_symbol},
    market_data::consolidator::Consolidator,
};

#[async_trait]
pub trait GetStrategyValue {
    async fn get_strategy_sgd_value(&self, strategy: &str) -> Result<f64, String>;
}

#[async_trait]
impl GetStrategyValue for Consolidator {
    async fn get_strategy_sgd_value(&self, strategy: &str) -> Result<f64, String> {
        if self.past_strategy_data.contains_key(strategy) {
            return Ok(self.past_strategy_data.get(strategy).unwrap());
        }

        let current_stock_positions_crud =
            get_specific_current_stock_positions_crud(self.pool.clone());
        let positions = current_stock_positions_crud
            .get_pos_by_strat(strategy)
            .await?;
        // let consolidator = extract_application_state(state).await?.consolidator;

        let mut sgd_value = 0.0;
        let mut exchange_rates = HashMap::new();
        for position in positions {
            let contract = get_contract_from_local_symbol(
                &position.stock,
                &position.primary_exchange,
                &position.currency,
            );
            if position.quantity == 0.0 {
                continue;
            }
            if contract.security_type == SecurityType::ForexPair {
                let hash_contract = HashContract {
                    contract: contract.clone(),
                };
                if !exchange_rates.contains_key(&hash_contract) {
                    if position.stock == "CASH:SGD" {
                        exchange_rates.insert(hash_contract.clone(), 1.0);
                    } else {
                        exchange_rates.insert(
                            hash_contract.clone(),
                            self.get_current_price(&contract, &false, &[])?,
                        );
                    }
                }
                sgd_value += exchange_rates.get(&hash_contract).unwrap() * position.quantity;
                continue;
            }

            if position.currency != "SGD" {
                let fx_contract = get_contract_from_local_symbol(
                    &format!("FX:{}/SGD", position.currency),
                    "",
                    "SGD",
                );
                let hash_contract = HashContract {
                    contract: fx_contract.clone(),
                };
                if !exchange_rates.contains_key(&hash_contract) {
                    exchange_rates.insert(
                        hash_contract.clone(),
                        self.get_current_price(&fx_contract, &false, &[])?,
                    );
                }

                // Market Value
                let mkt_value = self.get_current_price(&contract, &false, &[])? * position.quantity;
                sgd_value += exchange_rates.get(&hash_contract).unwrap() * mkt_value;
            } else {
                let mkt_value = self.get_current_price(&contract, &false, &[])? * position.quantity;
                sgd_value += mkt_value;
            }
        }

        self.past_strategy_data
            .insert(strategy.to_string(), sgd_value);
        Ok(sgd_value)
    }
}

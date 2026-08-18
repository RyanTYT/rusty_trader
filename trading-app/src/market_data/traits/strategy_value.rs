use std::{collections::HashMap, sync::Arc};

use ibapi::{Client, contracts::Contract, prelude::SecurityType};
use sqlx::PgPool;

use crate::{
    database::{
        models::AssetType,
        models_crud::current_positions::current_positions::{
            CurrentPositionsCRUD, CurrentPositionsFullKeys, CurrentPositionsOps,
        },
    },
    helpers::contract::{HashContract, LocalContractTypes, get_contract_from},
    market_data::{
        consolidator::{Consolidator, MemoisedConsolidatorFns},
        memoise::AnyMemoized,
    },
};

pub trait GetStrategyValue {
    fn get_strategy_sgd_value(&self, strategy: &str) -> Result<f64, String>;
}

impl GetStrategyValue for Consolidator {
    fn get_strategy_sgd_value(&self, strategy: &str) -> Result<f64, String> {
        let entry = self
            .memoisers
            .get(&MemoisedConsolidatorFns::GetStrategyValue)
            .expect("all MemoisedConsolidatorFns variants must be registered");

        // `contract` is already owned by this fn — no need to clone it,
        // just move it straight into the tuple.
        let result = entry.call_any(Box::new(strategy.to_string()))?;

        Ok(*result
            .downcast::<f64>()
            .unwrap_or_else(|_| panic!("AnyMemoized: return type mismatch for GetPrice")))
    }
}

impl Consolidator {
    // Ignore Options contracts for this for the time being
    pub(crate) fn _get_strategy_sgd_value(
        client: &Arc<Client>,
        get_current_price_fn: &Box<dyn AnyMemoized>,
        handle: &tokio::runtime::Handle,
        pool: &PgPool,
        strategy: &str,
    ) -> Result<f64, String> {
        let current_stock_positions_crud =
            CurrentPositionsCRUD::from(&AssetType::Stock, pool.clone());

        let positions = handle.block_on(async move {
            current_stock_positions_crud
                .get_pos_by_strat(strategy)
                .await
        })?;

        tracing::info!("Retrieved current positions for get_strategy_sgd_value");

        let mut sgd_value = 0.0;
        let mut exchange_rates: HashMap<HashContract, f64> = HashMap::new();

        // Typed wrapper around call_any so the Box::new + downcast boilerplate
        // and the client clone only happen in one place. Captures
        // `get_current_price_fn`/`client` by reference — fine since it's
        // only ever called synchronously within this loop, never stored.
        let call_price = |contract: Contract,
                          vwap: bool,
                          generic_ticks: Vec<String>,
                          is_second_try: bool|
         -> Result<f64, String> {
            let boxed = get_current_price_fn.call_any(Box::new((
                client.clone(),
                contract,
                vwap,
                generic_ticks,
                is_second_try,
            )))?;
            boxed
                .downcast::<f64>()
                .map(|v| *v)
                .map_err(|_| "GetPrice memoiser: unexpected return type".to_string())
        };

        for position in positions {
            let (stock, currency, quantity) = match &position {
                CurrentPositionsFullKeys::Stock(v) => {
                    (v.stock.clone(), v.currency.clone(), v.quantity)
                }
                CurrentPositionsFullKeys::Options(v) => {
                    (v.stock.clone(), v.currency.clone(), v.quantity)
                }
            };

            if quantity == 0.0 {
                continue;
            }

            let contract = get_contract_from(&LocalContractTypes::CurrentPosFk(position));

            if contract.security_type == SecurityType::ForexPair {
                let hash_contract = HashContract {
                    contract: contract.clone(),
                };
                if !exchange_rates.contains_key(&hash_contract) {
                    let rate = if stock == "CASH:SGD" {
                        1.0
                    } else {
                        call_price(contract.clone(), false, vec![], false)?
                    };
                    exchange_rates.insert(hash_contract.clone(), rate);
                }
                sgd_value += exchange_rates.get(&hash_contract).unwrap() * quantity;
                continue;
            }

            if currency != "SGD" {
                let fx_contract = Contract {
                    symbol: currency.into(),
                    security_type: ibapi::prelude::SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: "SGD".into(),
                    ..Default::default()
                };
                let hash_contract = HashContract {
                    contract: fx_contract.clone(),
                };
                if !exchange_rates.contains_key(&hash_contract) {
                    let rate = call_price(fx_contract, false, vec![], false)?;
                    exchange_rates.insert(hash_contract.clone(), rate);
                }

                let mkt_value = call_price(contract, false, vec![], false)? * quantity;
                sgd_value += exchange_rates.get(&hash_contract).unwrap() * mkt_value;
            } else {
                let mkt_value = call_price(contract, false, vec![], false)? * quantity;
                sgd_value += mkt_value;
            }

            tracing::info!("Got one additional sgd position");
        }

        Ok(sgd_value)
    }
}

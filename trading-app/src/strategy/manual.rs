use std::{
    cmp::Ordering,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use ibapi::prelude::{Contract, Symbol};
use sqlx::PgPool;

use crate::{
    database::{crud::CRUDTrait, models::AssetType, models_crud::strategy::get_strategy_crud},
    helpers::contract::get_contract_from_local_symbol,
    market_data::consolidator::Consolidator,
    strategy::strategy::StrategyExecutor,
};

#[derive(Debug, Clone)]
pub struct Manual {
    priority: u32,
    name: String,
    _pool: PgPool,
    contract: Contract,
}

impl PartialEq for Manual {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.name == other.name
    }
}

impl Eq for Manual {}

impl PartialOrd for Manual {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Manual {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.name.cmp(&other.name),
            other => other,
        }
    }
}

impl Manual {
    pub fn new(pool: PgPool, weak_consolidator: Weak<Consolidator>) -> Self {
        let contract = weak_consolidator
            .upgrade()
            .expect("Expected consolidator not to be dead while init noise")
            .validate_contract(
                &Contract {
                    symbol: Symbol::new("GBP"),
                    security_type: ibapi::prelude::SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: ibapi::prelude::Currency("USD".to_string()),
                    ..Default::default()
                },
                Duration::from_secs(10),
            )
            .expect("Expected forex_momentum contract not to be None");
        let cloned_pool = pool.clone();
        tokio::spawn(async move {
            let strategy_crud = get_strategy_crud(cloned_pool);
            if let Err(e) = strategy_crud
                .create_or_ignore(&crate::database::models::StrategyFullKeys {
                    strategy: "manual".to_string(),
                    status: crate::database::models::Status::Active,
                })
                .await
            {
                tracing::error!("Error occurred trying to create new Noise strategy: {e:?}")
            }
        });
        Self {
            priority: 1,
            name: "manual".to_string(),
            _pool: pool,
            contract,
        }
    }
}

#[async_trait]
impl StrategyExecutor for Manual {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn uses_data_type(&self) -> AssetType {
        AssetType::Stock
    }

    fn is_fx_strategy(&self) -> bool {
        return false;
    }

    async fn on_bar_update(
        &self,
        _contract: &Contract,
        _consolidator: &Arc<Consolidator>,
    ) -> Result<(bool, bool), String> {
        Ok((true, true))
    }

    fn get_contracts(&self) -> Vec<Contract> {
        let mut contracts = Vec::new();
        contracts.push(self.contract.clone());
        contracts
    }

    fn get_contract(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
        consolidator: &Arc<Consolidator>,
    ) -> Option<Contract> {
        consolidator.validate_contract(
            &get_contract_from_local_symbol(stock, primary_exchange, currency),
            Duration::from_secs(5),
        )
    }

    async fn warm_up_data(&self, _consolidator: &Arc<Consolidator>) -> Result<(), String> {
        Ok(())
    }
}

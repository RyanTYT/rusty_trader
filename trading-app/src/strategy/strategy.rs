use std::sync::Arc;

use async_trait::async_trait;
use ibapi::prelude::Contract;

use crate::market_data::consolidator::Consolidator;

#[async_trait]
pub trait StrategyExecutor: Ord + PartialOrd + Eq + PartialEq + Clone + Send + Sync {
    /// Usually for initialisation and storing of the relevant contracts for each strategy
    // fn new(pool: PgPool) -> Self;
    /// Should return a unique name for the DB table for coordination and tracking - the main
    /// reason for this whole app
    fn get_name(&self) -> String;
    /// Should update all relevant TargetPositions for the strategy
    /// - assume always that data in DB is fully updated
    async fn on_bar_update(&self, contract: &Contract) -> Result<(bool, bool), String>;
    /// Should return all associated contracts with this strategy
    fn get_contracts(&self) -> Vec<Contract>;
    /// Should return the associated contract given by the stock - used when determining contracts
    /// to place orders for in TargetPositions
    fn get_contract(&self, stock: String, primary_exchange: String) -> Option<Contract>;
    /// Warm up the data given the consolidator - get all data required up till now for the
    /// strategy
    async fn warm_up_data<T>(&self, consolidator: Arc<Consolidator<T>>) -> Result<(), String>
    where
        T: StrategyExecutor + 'static;
}

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum StrategyEnum {
    StratA(dummy1),
    StratB(dummy2),
}

#[async_trait]
impl StrategyExecutor for StrategyEnum {
    // /// Usually for initialisation and storing of the relevant contracts for each strategy
    // fn new(pool: PgPool) -> Self {
    //     Self {}
    // }
    /// Should return a unique name for the DB table for coordination and tracking - the main
    /// reason for this whole app
    fn get_name(&self) -> String {
        match self {
            StrategyEnum::StratA(s) => s.get_name(),
            StrategyEnum::StratB(s) => s.get_name(),
        }
    }
    /// Should update all relevant TargetPositions for the strategy
    /// - assume always that data in DB is fully updated
    async fn on_bar_update(&self, contract: &Contract) -> Result<(bool, bool), String> {
        match self {
            StrategyEnum::StratA(s) => s.on_bar_update(contract).await,
            StrategyEnum::StratB(s) => s.on_bar_update(contract).await,
        }
    }
    /// Should return all associated contracts with this strategy
    fn get_contracts(&self) -> Vec<Contract> {
        match self {
            StrategyEnum::StratA(s) => s.get_contracts(),
            StrategyEnum::StratB(s) => s.get_contracts(),
        }
    }
    /// Should return the associated contract given by the stock - used when determining contracts
    /// to place orders for in TargetPositions
    fn get_contract(&self, stock: String, primary_exchange: String) -> Option<Contract> {
        match self {
            StrategyEnum::StratA(s) => s.get_contract(stock, primary_exchange),
            StrategyEnum::StratB(s) => s.get_contract(stock, primary_exchange),
        }
    }
    /// Warm up the data given the consolidator - get all data required up till now for the
    /// strategy
    async fn warm_up_data<T>(&self, consolidator: Arc<Consolidator<T>>) -> Result<(), String>
    where
        T: StrategyExecutor + 'static,
    {
        match self {
            StrategyEnum::StratA(s) => s.warm_up_data(consolidator).await,
            StrategyEnum::StratB(s) => s.warm_up_data(consolidator).await,
        }
    }
}

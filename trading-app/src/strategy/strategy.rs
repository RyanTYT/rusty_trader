use std::{hash::Hash, sync::Arc};

use async_trait::async_trait;
use ibapi::prelude::Contract;

use crate::{
    database::models::AssetType,
    market_data::consolidator::Consolidator,
    strategy::{manual::Manual, noise::Noise, unknown::Unknown},
};

#[async_trait]
pub trait StrategyExecutor: Ord + PartialOrd + Eq + PartialEq + Clone + Send + Sync {
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Usually for initialisation and storing of the relevant contracts for each strategy
    // fn new(pool: PgPool) -> Self;
    /// Should return a unique name for the DB table for coordination and tracking - the main
    /// reason for this whole app
    fn get_name(&self) -> String;
    /// Returns the asset type being tracked/traded - used for consolidator to determine how to
    /// process data
    fn uses_data_type(&self) -> AssetType;
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Should update all relevant TargetPositions for the strategy
    /// - assume always that data in DB is fully updated
    /// - Result(bool, bool): (is positions updated, is all contracts of strategy used / ignore
    /// contract for strategy)
    async fn on_bar_update(
        &self,
        contract: &Contract,
        consolidator: &Arc<Consolidator>,
    ) -> Result<(bool, bool), String>;
    /// Should return all associated contracts with this strategy
    fn get_contracts(&self) -> Vec<Contract>;
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Should return the associated contract given by the stock - used when determining contracts
    /// to place orders for in TargetPositions
    fn get_contract(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
        consolidator: &Arc<Consolidator>,
    ) -> Option<Contract>;
    /// Warm up the data given the consolidator - get all data required up till now for the
    /// strategy
    async fn warm_up_data(&self, consolidator: &Arc<Consolidator>) -> Result<(), String>;
    fn is_fx_strategy(&self) -> bool;
}

// Define the macro to generate the enum and impl
macro_rules! strategy_enum {
    ($($variant:ident($type:ty)),* $(,)?) => {
        #[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
        pub enum StrategyEnum {
            $($variant($type)),*
        }

        impl Hash for StrategyEnum {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_name().hash(state)),*
                }
            }
        }

        #[async_trait]
        impl StrategyExecutor for StrategyEnum {
            fn get_name(&self) -> String {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_name()),*
                }
            }

            fn uses_data_type(&self) -> AssetType {
                match self {
                    $(StrategyEnum::$variant(s) => s.uses_data_type()),*
                }
            }

            async fn on_bar_update(
                &self,
                contract: &Contract,
                consolidator: &Arc<Consolidator>,
            ) -> Result<(bool, bool), String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.on_bar_update(contract, consolidator).await),*
                }
            }

            fn get_contracts(&self) -> Vec<Contract> {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_contracts()),*
                }
            }

            fn get_contract(&self, stock: &str, primary_exchange: &str, currency: &str, consolidator: &Arc<Consolidator>) -> Option<Contract> {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_contract(stock, primary_exchange, currency, consolidator)),*
                }
            }

            async fn warm_up_data(&self, consolidator: &Arc<Consolidator>) -> Result<(), String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.warm_up_data(consolidator).await),*
                }
            }

            fn is_fx_strategy(&self) -> bool {
                match self {
                    $(StrategyEnum::$variant(s) => s.is_fx_strategy()),*
                }
            }
        }
    };
}

// Now adding a new strategy is just one line!
strategy_enum! {
    Noise(Noise),
    Manual(Manual),
    Unknown(Unknown)
}

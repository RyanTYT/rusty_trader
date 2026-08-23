use std::{hash::Hash, sync::Arc};

use ibapi::{Client, prelude::Contract};

use crate::{
    database::{
        models::AssetType, models_crud::historical_data::historical_data::HistoricalDataFullKeys,
    },
    execution::order_engine::OrderIBKR,
    market_data::consolidator::Consolidator,
    strategy::{manual::Manual, noise::Noise, unknown::Unknown},
};

#[derive(Debug, Clone)]
pub enum BarUpdateOutcome {
    /// Fast Path: In-memory evaluation ready to submit orders immediately.
    EmitOrders(Vec<OrderIBKR>),
    /// Slow Path: Strategy requires target position V current position
    /// to determine market action.
    PendingDbQuery(Vec<AssetType>),
    /// No action required for this bar tick.
    NoAction,
}

#[async_trait::async_trait]
pub trait StrategyExecutor: Ord + PartialOrd + Eq + PartialEq + Clone + Send + Sync {
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Usually for initialisation and storing of the relevant contracts for each strategy
    // fn new(pool: PgPool) -> Self;
    /// Should return a unique name for the DB table for coordination and tracking - the main
    /// reason for this whole app
    fn get_name(&self) -> String;
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Should update all relevant TargetPositions for the strategy
    /// - assume always that data in DB is fully updated
    /// - Result(bool, bool): (is positions updated, is all contracts of strategy used / ignore
    /// contract for strategy)
    fn on_bar_update(
        &self,
        contract: &Contract,
        bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, String>;
    /// Should return all associated contracts with this strategy
    fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract>;
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

        #[async_trait::async_trait]
        impl StrategyExecutor for StrategyEnum {
            fn get_name(&self) -> String {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_name()),*
                }
            }

            fn on_bar_update(
                &self,
                contract: &Contract,
                bar: &HistoricalDataFullKeys,
                consolidator: &Arc<Consolidator>,
            ) -> Result<BarUpdateOutcome, String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.on_bar_update(contract, bar, consolidator)),*
                }
            }

            fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract> {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_contracts(client)),*
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


use std::{hash::Hash, sync::Arc};

#[cfg(feature = "backtest")]
use chrono::{DateTime, Utc};
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StrategyDetails {
    priority: i32,
    pub name: String,
    pub is_fx_strategy: bool,
}

impl StrategyDetails {
    pub fn new(priority: i32, name: String, is_fx_strategy: bool) -> Self {
        Self {
            priority,
            name,
            is_fx_strategy,
        }
    }
}

#[async_trait::async_trait]
pub trait StrategyExecutor: Send + Sync {
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Usually for initialisation and storing of the relevant contracts for each strategy
    // fn new(pool: PgPool) -> Self;
    /// Should return a unique name for the DB table for coordination and tracking - the main
    /// reason for this whole app
    fn get_name(&self) -> String;
    /// Should return all associated contracts with this strategy
    fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract>;
    /// Should return StrategyDetails Struct which consists of
    /// StrategyDetails {
    ///     // name of strategy
    ///     name: String,
    ///     // whether strategy deals with FX
    ///     // true -> strategy actively trades FX pairs
    ///     // false -> strategy does not but may be multi-currency
    ///     is_fx_strategy: bool
    /// }
    fn get_strategy_details(&self) -> StrategyDetails;

    /// NEEDS TO BE DEFINED CORRECTLY
    /// Should update all relevant TargetPositions for the strategy
    /// - assume always that data in DB is fully updated
    /// - Result(bool, bool): (is positions updated, is all contracts of strategy used / ignore
    /// contract for strategy)
    fn on_bar_update(
        &mut self,
        contract: &Contract,
        bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, String>;
    /// Warm up the data given the consolidator - get all data required up till now for the
    /// strategy
    async fn warm_up_data(
        &mut self,
        consolidator: &Arc<Consolidator>,
        #[cfg(feature = "backtest")] bar_time: DateTime<Utc>,
    ) -> Result<(), String>;

    /// The SQL queries to pre-compute per bar (for the in-memory backtest
    /// cache). Default: none. The strategy declares its queries (with the
    /// lookback params read from `backtest_params`). The cache module does
    /// NOT interpret the queries — it just calls `q.run()` per bar.
    #[cfg(feature = "backtest")]
    fn cache_queries(
        &self,
    ) -> Vec<std::sync::Arc<dyn crate::backtester::methods::in_memory::historical_cache::CacheQuery>>
    {
        Vec::new()
    }
}

// Define the macro to generate the enum and impl
macro_rules! strategy_enum {
    ($($variant:ident($type:ty)),* $(,)?) => {
        #[derive(Debug)]
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

            fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract> {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_contracts(client)),*
                }
            }

            fn get_strategy_details(&self) -> StrategyDetails {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_strategy_details()),*
                }
            }

            fn on_bar_update(
                &mut self,
                contract: &Contract,
                bar: &HistoricalDataFullKeys,
                consolidator: &Arc<Consolidator>,
            ) -> Result<BarUpdateOutcome, String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.on_bar_update(contract, bar, consolidator)),*
                }
            }

            async fn warm_up_data(
                &mut self,
                consolidator: &Arc<Consolidator>,
                #[cfg(feature = "backtest")] bar_time: DateTime<chrono::Utc>
            ) -> Result<(), String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.warm_up_data(
                            consolidator,
                            #[cfg(feature = "backtest")] bar_time
                     ).await),*
                }
            }
        }
    };
}

// Now adding a new strategy is just one line!
strategy_enum! {
    Noise(Noise),
    // FractionalMomentum(FractionalMomentum),
    // ForexMeanReversion(ForexMeanReversion),
    // ForexMomentum(ForexMomentum),
    // GoldMomentum(GoldMomentum),
    Manual(Manual),
    Unknown(Unknown)
}

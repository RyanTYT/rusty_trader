use std::{cmp::Ordering, sync::Arc, time::Duration};

#[cfg(feature = "backtest")]
use chrono::{DateTime, Utc};
use ibapi::{
    Client,
    prelude::{Contract, Symbol},
};
use sqlx::PgPool;

use crate::{
    database::{
        models::AssetType, models_crud::historical_data::historical_data::HistoricalDataFullKeys,
    },
    market_data::consolidator::Consolidator,
    strategy::strategy::{BarUpdateOutcome, StrategyDetails, StrategyExecutor},
};

#[derive(Debug, Clone)]
pub struct Manual {
    priority: u32,
    name: String,
}

// impl PartialEq for Manual {
//     fn eq(&self, other: &Self) -> bool {
//         self.priority == other.priority && self.name == other.name
//     }
// }
//
// impl Eq for Manual {}
//
// impl PartialOrd for Manual {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
//
// impl Ord for Manual {
//     fn cmp(&self, other: &Self) -> Ordering {
//         match self.priority.cmp(&other.priority) {
//             Ordering::Equal => self.name.cmp(&other.name),
//             other => other,
//         }
//     }
// }

impl Manual {
    pub fn new(_pool: PgPool) -> Self {
        Self {
            priority: 1,
            name: "manual".to_string(),
        }
    }
}

#[hotpath::measure_all]
#[async_trait::async_trait]
impl StrategyExecutor for Manual {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_strategy_details(&self) -> StrategyDetails {
        StrategyDetails::new(1, self.name.clone(), false)
    }

    fn on_bar_update(
        &mut self,
        _contract: &Contract,
        _bar: &HistoricalDataFullKeys,
        _consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, String> {
        // Because there r only Stock n Option DB -> this is actually sufficient
        Ok(BarUpdateOutcome::PendingDbQuery(vec![
            AssetType::Stock,
            AssetType::Option,
        ]))
    }

    fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract> {
        vec![
            Consolidator::_validate_contract(
                client,
                Contract {
                    symbol: Symbol::new("GBP"),
                    security_type: ibapi::prelude::SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: ibapi::prelude::Currency("USD".to_string()),
                    ..Default::default()
                },
                Duration::from_secs(10),
            )
            .expect("Expected forex_momentum contract not to be None"),
        ]
    }

    async fn warm_up_data(
        &mut self,
        _consolidator: &Arc<Consolidator>,
        #[cfg(feature = "backtest")] bar_time: DateTime<Utc>,
    ) -> Result<(), String> {
        Ok(())
    }
}

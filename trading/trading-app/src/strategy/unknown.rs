use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use ibapi::{
    Client,
    orders::ExecutionData,
    prelude::{Contract, Symbol},
};
use sqlx::PgPool;

use crate::{
    database::{
        models::AssetType, models_crud::historical_data::historical_data::HistoricalDataFullKeys,
    },
    market_data::consolidator::Consolidator,
    strategy::strategy::{BarUpdateOutcome, StrategyDetails, StrategyExecutor, StrategyFund},
};

#[derive(Debug, Clone)]
pub struct Unknown {
    name: String,
}

// impl PartialEq for Unknown {
//     fn eq(&self, other: &Self) -> bool {
//         self.priority == other.priority && self.name == other.name
//     }
// }
//
// impl Eq for Unknown {}
//
// impl PartialOrd for Unknown {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
//
// impl Ord for Unknown {
//     fn cmp(&self, other: &Self) -> Ordering {
//         match self.priority.cmp(&other.priority) {
//             Ordering::Equal => self.name.cmp(&other.name),
//             other => other,
//         }
//     }
// }

impl Unknown {
    pub fn new(_pool: PgPool) -> Self {
        Self {
            name: "unknown".to_string(),
        }
    }
}

#[hotpath::measure_all]
#[async_trait::async_trait]
impl StrategyExecutor for Unknown {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn fund_strategy(&mut self) -> StrategyFund {
        StrategyFund {
            priority: 1,
            to_reallocate: true,
            weight: 0.0,
            min_capital: 0.0,
            cash_prefs: vec![],
        }
    }

    fn get_strategy_details(&self) -> StrategyDetails {
        StrategyDetails::new(1, self.name.clone(), false, false)
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

    fn get_contracts(&mut self, client: Arc<Client>) -> Vec<Contract> {
        vec![
            Consolidator::_get_contract_details(
                client,
                None,
                Contract {
                    symbol: Symbol::new("GBP"),
                    security_type: ibapi::prelude::SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: ibapi::prelude::Currency("USD".to_string()),
                    ..Default::default()
                },
                Duration::from_secs(10),
            )
            .map(|v| v.contract)
            .expect("Expected forex_momentum contract not to be None"),
        ]
    }

    fn warmup_bars_required(&self) -> usize {
        0
    }

    async fn warm_up_data(
        &mut self,
        _consolidator: &Arc<Consolidator>,
        #[cfg(feature = "backtest")] _bar_time: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String> {
        Ok(Utc::now())
    }

    fn on_fill(&mut self, _: ExecutionData) -> Result<(), String> {
        Ok(())
    }
}

// use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc};
// use chrono_tz::Tz;
// use ibapi::prelude::Contract;
//
// use crate::{
//     helpers::contract::HashContract, init_app::StrategyParameters,
//     market_data::consolidator::Consolidator, schedule::contract_scheduler::ContractScheduler,
//     strategy::strategy::StrategyExecutor,
// };
//
// pub trait StrategyScheduler {
//     // fn add_contract_schedules(&self, strat_params: &[StrategyParameters]) -> Result<(), String>;
//     // fn add_contract_schedules_late(
//     //     &self,
//     //     strategy: &str,
//     //     contracts: &[Contract],
//     // ) -> Result<(), String>;
//     // fn is_strategy_active(&self, strategy: &str, dt: &DateTime<Utc>) -> Result<bool, String>;
//     // fn get_next_strategy_active(
//     //     &self,
//     //     strategy: &str,
//     //     dt: &DateTime<Utc>,
//     // ) -> Result<DateTime<Utc>, String>;
//     // fn get_next_strategy_inactive(
//     //     &self,
//     //     strategy: &str,
//     //     dt: &DateTime<Utc>,
//     // ) -> Result<DateTime<Utc>, String>;
//     fn is_fx_trading_datetime(dt: &DateTime<Tz>) -> bool;
//     fn fx_trading_day_start(date: &NaiveDate, tz: &Tz) -> DateTime<Tz>;
// }
//
// impl StrategyScheduler for Consolidator {
//     // fn add_contract_schedules(&self, strat_params: &[StrategyParameters]) -> Result<(), String> {
//     //     {
//     //         self.contract_coordinator.add_all_schedules(
//     //             strat_params
//     //                 .iter()
//     //                 .flat_map(|sp| sp.subscribed_contracts.iter().map(|cs| cs.contract.clone())),
//     //         )?;
//     //     }
//     //     {
//     //         strat_params.iter().for_each(|strat_param| {
//     //             self.strat_contracts
//     //                 .write()
//     //                 .expect("Failed to acquire write lock for strat_contracts")
//     //                 .insert(
//     //                     strat_param.strategy.get_name(),
//     //                     strat_param
//     //                         .subscribed_contracts
//     //                         .iter()
//     //                         .map(|sub_contract| HashContract {
//     //                             contract: sub_contract.contract.clone(),
//     //                         })
//     //                         .collect(),
//     //                 );
//     //         });
//     //     }
//     //     Ok(())
//     // }
//     //
//     // /// Will override previous key-value pairs if alr existed previously
//     // fn add_contract_schedules_late(
//     //     &self,
//     //     strategy: &str,
//     //     contracts: &[Contract],
//     // ) -> Result<(), String> {
//     //     {
//     //         self.contract_coordinator
//     //             .add_all_schedules(contracts.iter().cloned())?;
//     //     }
//     //     {
//     //         self.strat_contracts
//     //             .write()
//     //             .expect("Failed to acquire write lock for strat_contracts")
//     //             .insert(
//     //                 strategy.to_string(),
//     //                 contracts
//     //                     .iter()
//     //                     .map(|contract| HashContract {
//     //                         contract: contract.clone(),
//     //                     })
//     //                     .collect(),
//     //             );
//     //     }
//     //     Ok(())
//     // }
//     //
//     // fn is_strategy_active(&self, strategy: &str, dt: &DateTime<Utc>) -> Result<bool, String> {
//     //     let strat_contracts = self
//     //         .strat_contracts
//     //         .read()
//     //         .expect("Failed to acquire strat_contracts lock");
//     //     let contracts = strat_contracts.get(strategy).ok_or_else(|| {
//     //         format!("Couldn't find strategy {strategy:?} in consolidator - not pre-registered")
//     //     })?;
//     //
//     //     {
//     //         for contract in contracts {
//     //             if self.contract_coordinator.is_trading(contract, &dt)? {
//     //                 return Ok(true);
//     //             }
//     //         }
//     //     }
//     //
//     //     Ok(false)
//     // }
//     //
//     // fn get_next_strategy_active(
//     //     &self,
//     //     strategy: &str,
//     //     dt: &DateTime<Utc>,
//     // ) -> Result<DateTime<Utc>, String> {
//     //     let strat_contracts = self
//     //         .strat_contracts
//     //         .read()
//     //         .expect("Failed to acquire strat_contracts lock");
//     //     let contracts = strat_contracts.get(strategy).ok_or_else(|| {
//     //         format!("Couldn't find strategy in consolidator - not pre-registered")
//     //     })?;
//     //     self.contract_coordinator
//     //         .get_next_earliest_available_data(contracts, dt)
//     // }
//     //
//     // fn get_next_strategy_inactive(
//     //     &self,
//     //     strategy: &str,
//     //     dt: &DateTime<Utc>,
//     // ) -> Result<DateTime<Utc>, String> {
//     //     let strat_contracts = self
//     //         .strat_contracts
//     //         .read()
//     //         .expect("Failed to acquire strat_contracts lock");
//     //     let contracts = strat_contracts.get(strategy).ok_or_else(|| {
//     //         format!("Couldn't find strategy in consolidator - not pre-registered")
//     //     })?;
//     //     self.contract_coordinator
//     //         .get_next_latest_unavailable_data(contracts, dt)
//     // }
//
//     
// }

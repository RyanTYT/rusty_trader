use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use ibapi::prelude::Contract;

use crate::{
    helpers::contract::HashContract, init_app::StrategyParameters,
    market_data::consolidator::Consolidator, schedule::contract_scheduler::ContractScheduler,
    strategy::strategy::StrategyExecutor,
};

pub trait StrategyScheduler {
    fn add_contract_schedules(&self, strat_params: &[StrategyParameters]) -> Result<(), String>;
    fn add_contract_schedules_late(
        &self,
        strategy: &str,
        contracts: &[Contract],
    ) -> Result<(), String>;
    fn is_strategy_active(&self, strategy: &str, dt: &DateTime<Utc>) -> Result<bool, String>;
    fn get_next_strategy_active(
        &self,
        strategy: &str,
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String>;
    fn get_next_strategy_inactive(
        &self,
        strategy: &str,
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String>;
    fn is_fx_trading_datetime(dt: &DateTime<Tz>) -> bool;
    fn fx_trading_day_start(date: &NaiveDate, tz: &Tz) -> DateTime<Tz>;
}

impl StrategyScheduler for Consolidator {
    fn add_contract_schedules(&self, strat_params: &[StrategyParameters]) -> Result<(), String> {
        {
            self.contract_coordinator.add_all_schedules(
                strat_params
                    .iter()
                    .flat_map(|sp| sp.subscribed_contracts.iter().map(|cs| cs.contract.clone())),
            )?;
        }
        {
            strat_params.iter().for_each(|strat_param| {
                self.strat_contracts
                    .write()
                    .expect("Failed to acquire write lock for strat_contracts")
                    .insert(
                        strat_param.strategy.get_name(),
                        strat_param
                            .subscribed_contracts
                            .iter()
                            .map(|sub_contract| HashContract {
                                contract: sub_contract.contract.clone(),
                            })
                            .collect(),
                    );
            });
        }
        Ok(())
    }

    /// Will override previous key-value pairs if alr existed previously
    fn add_contract_schedules_late(
        &self,
        strategy: &str,
        contracts: &[Contract],
    ) -> Result<(), String> {
        {
            self.contract_coordinator
                .add_all_schedules(contracts.iter().cloned())?;
        }
        {
            self.strat_contracts
                .write()
                .expect("Failed to acquire write lock for strat_contracts")
                .insert(
                    strategy.to_string(),
                    contracts
                        .iter()
                        .map(|contract| HashContract {
                            contract: contract.clone(),
                        })
                        .collect(),
                );
        }
        Ok(())
    }

    fn is_strategy_active(&self, strategy: &str, dt: &DateTime<Utc>) -> Result<bool, String> {
        let strat_contracts = self
            .strat_contracts
            .read()
            .expect("Failed to acquire strat_contracts lock");
        let contracts = strat_contracts.get(strategy).ok_or_else(|| {
            format!("Couldn't find strategy {strategy:?} in consolidator - not pre-registered")
        })?;

        {
            for contract in contracts {
                if self.contract_coordinator.is_trading(contract, &dt)? {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn get_next_strategy_active(
        &self,
        strategy: &str,
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String> {
        let strat_contracts = self
            .strat_contracts
            .read()
            .expect("Failed to acquire strat_contracts lock");
        let contracts = strat_contracts.get(strategy).ok_or_else(|| {
            format!("Couldn't find strategy in consolidator - not pre-registered")
        })?;
        self.contract_coordinator
            .get_next_earliest_available_data(contracts, dt)
    }

    fn get_next_strategy_inactive(
        &self,
        strategy: &str,
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String> {
        let strat_contracts = self
            .strat_contracts
            .read()
            .expect("Failed to acquire strat_contracts lock");
        let contracts = strat_contracts.get(strategy).ok_or_else(|| {
            format!("Couldn't find strategy in consolidator - not pre-registered")
        })?;
        self.contract_coordinator
            .get_next_latest_unavailable_data(contracts, dt)
    }

    fn is_fx_trading_datetime(dt: &DateTime<Tz>) -> bool {
        fn is_fx_market_holiday(date: NaiveDate) -> bool {
            let year = date.year();

            // New Year's Day (observed)
            if is_observed_holiday(date, year, 1, 1) {
                return true;
            }

            // Christmas Day (observed)
            if is_observed_holiday(date, year, 12, 25) {
                return true;
            }

            // Good Friday (Easter-based, varies each year)
            if date == calculate_good_friday(year) {
                return true;
            }

            false
        }

        /// Check if a holiday is observed on this date (handles weekend adjustments)
        fn is_observed_holiday(date: NaiveDate, year: i32, month: u32, day: u32) -> bool {
            if let Some(holiday) = NaiveDate::from_ymd_opt(year, month, day) {
                match holiday.weekday() {
                    // If holiday falls on Saturday, observed on Friday
                    chrono::Weekday::Sat => date == holiday - chrono::Days::new(1),
                    // If holiday falls on Sunday, observed on Monday
                    chrono::Weekday::Sun => date == holiday + chrono::Days::new(1),
                    // Weekday holiday observed on actual day
                    _ => date == holiday,
                }
            } else {
                false
            }
        }

        /// Calculate Good Friday for a given year using the Computus algorithm
        fn calculate_good_friday(year: i32) -> NaiveDate {
            // Meeus/Jones/Butcher algorithm for Gregorian calendar
            let a = year % 19;
            let b = year / 100;
            let c = year % 100;
            let d = b / 4;
            let e = b % 4;
            let f = (b + 8) / 25;
            let g = (b - f + 1) / 3;
            let h = (19 * a + b - d - g + 15) % 30;
            let i = c / 4;
            let k = c % 4;
            let l = (32 + 2 * e + 2 * i - h - k) % 7;
            let m = (a + 11 * h + 22 * l) / 451;
            let month = (h + l - 7 * m + 114) / 31;
            let day = ((h + l - 7 * m + 114) % 31) + 1;

            // Easter Sunday
            let easter = NaiveDate::from_ymd_opt(year, month as u32, day as u32).unwrap();

            // Good Friday is 2 days before Easter
            easter - chrono::Days::new(2)
        }

        // FX constants (New York time)
        const FX_WEEK_OPEN_HOUR: u32 = 17; // Sunday 17:00 NY
        const FX_WEEK_CLOSE_HOUR: u32 = 17; // Friday 17:00 NY

        // Check if it's a FX market holiday
        if is_fx_market_holiday(dt.date_naive()) {
            return false;
        }

        let weekday = dt.weekday();
        match weekday {
            chrono::Weekday::Sun => dt.hour() >= FX_WEEK_OPEN_HOUR,
            chrono::Weekday::Fri => dt.hour() < FX_WEEK_CLOSE_HOUR,
            chrono::Weekday::Sat => false,
            _ => true, // Mon–Thu always trading
        }
    }

    fn fx_trading_day_start(date: &NaiveDate, tz: &Tz) -> DateTime<Tz> {
        const FX_WEEK_OPEN_HOUR: u32 = 17; // Sunday 17:00 NY
        //
        let prev_day = date.pred_opt().unwrap();
        tz.from_local_datetime(&prev_day.and_hms_opt(FX_WEEK_OPEN_HOUR, 0, 0).unwrap())
            .single()
            .unwrap()
    }
}

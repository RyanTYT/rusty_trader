use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{Arc, RwLock},
    time::Duration,
};

use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Tz;
use ibapi::{Client, prelude::Contract};

use crate::helpers::{contract::HashContract, sync_timeout::timeout};

#[derive(Debug, Clone)]
pub struct TradingHours {
    open: DateTime<Tz>,
    close: DateTime<Tz>,
}

#[derive(Debug, Clone)]
pub struct Schedule {
    time_zone: Tz,
    schedule: BTreeMap<NaiveDate, Option<TradingHours>>,
}

pub trait ContractScheduler {
    fn add_schedule(&self, contract: &Contract) -> Result<(), String>;
    fn add_all_schedules<I>(&self, contracts: I) -> Result<(), String>
    where
        I: IntoIterator<Item = Contract>;
    fn get_schedule(
        &self,
        contract: &HashContract,
        dt: &DateTime<Utc>,
    ) -> Result<(Tz, Option<TradingHours>), String>;
    fn is_trading(&self, contract: &HashContract, dt: &DateTime<Utc>) -> Result<bool, String>;

    fn get_next_latest_unavailable_data(
        &self,
        contracts: &[HashContract],
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String>;
    fn get_next_earliest_available_data(
        &self,
        contracts: &[HashContract],
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String>;
}

pub struct IbkrContractScheduler {
    client: Arc<Client>,
    schedules: Arc<RwLock<HashMap<HashContract, Schedule>>>,
}

impl IbkrContractScheduler {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            schedules: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl IbkrContractScheduler {
    pub fn contains_contract(&self, contract: &HashContract) -> bool {
        self.schedules
            .read()
            .expect("Expected schedules lock not to be poisoned")
            .contains_key(&contract)
    }
}

/// must set option in global config api settings to return 1 month of trading hours
/// option: 'Expose whole trading schedule to api ...'
impl ContractScheduler for IbkrContractScheduler {
    fn add_schedule(&self, contract: &Contract) -> Result<(), String> {
        let hashed_contract = HashContract {
            contract: contract.clone(),
        };
        // =====================
        // skip if alr have data
        // =====================
        {
            if self
                .schedules
                .read()
                .expect("Expected schedules lock not to be poisoned")
                .contains_key(&hashed_contract)
            {
                return Ok(());
            }
        }

        let client = self.client.clone();
        let cloned_contract = contract.clone();
        match timeout(Duration::from_secs(1), move || {
            client.contract_details(&cloned_contract)
        }) {
            Ok(all_contract_details) => {
                let contract_details = {
                    if all_contract_details.is_empty() {
                        tracing::error!("No contracts found matching contract: {contract:?}");
                    }
                    if all_contract_details.len() > 1 {
                        tracing::warn!(
                            "Multiple contract definitions found! Defaulting to first definition received"
                        );
                    }
                    all_contract_details.first().unwrap()
                };
                let tz = {
                    let tz_res = chrono_tz::Tz::from_str(&contract_details.time_zone_id);
                    if let Err(e) = tz_res {
                        return Err(format!("Counldn't convert time_zone_id to string: {e:?}"));
                    }
                    tz_res.unwrap()
                };
                let mut schedule = BTreeMap::new();
                let trading_hours = {
                    if contract_details.liquid_hours.is_empty() {
                        &contract_details.trading_hours
                    } else {
                        &contract_details.liquid_hours
                    }
                };
                tracing::info!("Received: {trading_hours:?}");
                for day in trading_hours {
                    // there are empty strings at beginning sometimes for some reason, wtf ibkr
                    if day.is_empty() {
                        continue;
                    }

                    if day.contains("-") {
                        let split_dt = day.split_once("-");
                        if let None = split_dt {
                            return Err(format!(
                                "Couldn't split dt string in schedule by -: {day:?}"
                            ));
                        }
                        let (open_dt, close_dt) = split_dt.unwrap();
                        let open_formatted = {
                            let dt = NaiveDateTime::parse_from_str(&open_dt, "%Y%m%d:%H%M");
                            match dt {
                                Ok(dt) => dt,
                                Err(e) => {
                                    return Err(format!(
                                        "Failed to parse opening datetime {open_dt:?}: {e:?}"
                                    ));
                                }
                            }
                        };
                        let close_formatted = {
                            let dt = NaiveDateTime::parse_from_str(&close_dt, "%Y%m%d:%H%M");
                            match dt {
                                Ok(dt) => dt,
                                Err(e) => {
                                    return Err(format!(
                                        "Failed to parse opening datetime {open_dt:?}: {e:?}"
                                    ));
                                }
                            }
                        };
                        let trading_hours = TradingHours {
                            open: open_formatted.and_local_timezone(tz).unwrap(),
                            close: close_formatted.and_local_timezone(tz).unwrap(),
                        };
                        schedule.insert(open_formatted.date(), Some(trading_hours));
                    } else if day.contains(":CLOSED") {
                        match NaiveDate::parse_from_str(
                            &day.strip_suffix(":CLOSED").unwrap(),
                            "%Y%m%d",
                        ) {
                            Ok(naive_date) => {
                                schedule.insert(naive_date, None);
                            }
                            Err(e) => {
                                return Err(format!(
                                    "Could not parse datetime from supposed closed string of {day:?}: {e:?}"
                                ));
                            }
                        }
                    } else {
                        tracing::warn!(
                            "Patterm for days returned in contract details not found: {day:?}"
                        )
                    }
                }

                // Impute missing days
                // - occurs mainly for FX contracts whereby schedule says time ends Sat 06:00
                //   so Sat is not technically closed, and it only reports for Sun next - so
                //   missing 1 day in between
                // - this implementation should correctly handle the issues - i.e. if on Sat open,
                //   fn will check for prev day data first - if open return that, else check tdy's
                //   data
                let (last_day, _) = schedule
                    .last_key_value()
                    .expect("Expected at least one day of schedule in data by IBKR");
                let mut missing_days = Vec::new();
                for day in schedule.keys() {
                    let mut next_day = *day;
                    loop {
                        next_day = next_day
                            .checked_add_days(Days::new(1))
                            .expect("Expected not to overflow");
                        if !schedule.contains_key(&next_day) && &next_day < last_day {
                            missing_days.push(next_day);
                        } else {
                            break;
                        }
                    }
                }
                for day in missing_days {
                    schedule.insert(day, None);
                }

                {
                    self.schedules
                        .write()
                        .expect("Expected write lock for schedules not to be poisoned")
                        .insert(
                            hashed_contract,
                            Schedule {
                                time_zone: tz,
                                schedule,
                            },
                        );
                }
                Ok(())
            }
            Err(_) => return Err("Request for contract details to IBKR timed out!".to_string()),
        }
    }

    fn add_all_schedules<I>(&self, contracts: I) -> Result<(), String>
    where
        I: IntoIterator<Item = Contract>,
    {
        let mut cum_err = Vec::new();
        for contract in contracts {
            if let Err(e) = self.add_schedule(&contract) {
                cum_err.push(e);
            }
        }
        if cum_err.is_empty() {
            return Ok(());
        }
        Err(format!("{}", cum_err.join("\n")))
    }

    fn get_schedule(
        &self,
        contract: &HashContract,
        dt: &DateTime<Utc>,
    ) -> Result<(Tz, Option<TradingHours>), String> {
        if !self
            .schedules
            .read()
            .expect("Expected read lock for schedules not to be poisoned")
            .contains_key(contract)
        {
            return Err(format!(
                "Schedule in Scheduler doesn't contain key for contract: {contract:?}"
            ));
        }

        let schedules = self
            .schedules
            .read()
            .expect("Expected read lock for schedules not to be poisoned");
        let schedule = schedules.get(&contract).unwrap();
        let date_tdy = dt.with_timezone(&schedule.time_zone).date_naive();
        match schedule.schedule.get(&date_tdy) {
            Some(trading_hours) => Ok((schedule.time_zone, trading_hours.clone())),
            None => Err(format!("Date not found under schedules: {date_tdy:?}")),
        }
    }

    fn is_trading(&self, contract: &HashContract, dt: &DateTime<Utc>) -> Result<bool, String> {
        let potential_res = match self.get_schedule(contract, &(*dt - TimeDelta::days(1))) {
            Ok(trading_hours_opt_w_tz) => {
                let (tz, trading_hours_opt) = trading_hours_opt_w_tz;
                let dt_now = dt.with_timezone(&tz);
                match trading_hours_opt {
                    Some(trading_hours) => {
                        tracing::info!("HWOT: {dt_now:?}, {trading_hours:?}");
                        Ok(trading_hours.open <= dt_now && dt_now <= trading_hours.close)
                    }
                    None => Ok(false),
                }
            }
            Err(e) => Err(e),
        };
        if potential_res.is_ok_and(|is_trading_rn| is_trading_rn) {
            return Ok(true);
        }
        match self.get_schedule(contract, dt) {
            Ok(trading_hours_opt_w_tz) => {
                let (tz, trading_hours_opt) = trading_hours_opt_w_tz;
                let dt_now = dt.with_timezone(&tz);
                match trading_hours_opt {
                    Some(trading_hours) => {
                        tracing::info!("HWOT: {dt_now:?}, {trading_hours:?}");
                        Ok(trading_hours.open <= dt_now && dt_now <= trading_hours.close)
                    }
                    None => Ok(false),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn get_next_latest_unavailable_data(
        &self,
        contracts: &[HashContract],
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String> {
        // 1. Collect all intervals in UTC
        let mut intervals: Vec<(DateTime<Utc>, DateTime<Utc>)> = {
            let schedules = self
                .schedules
                .read()
                .expect("Expected read lock of schedules not to be poisoned");
            contracts
                .iter()
                .filter_map(|contract| schedules.get(contract))
                .flat_map(|schedule| {
                    schedule.schedule.iter().filter_map(|(_, session)| {
                        let session = session.as_ref()?;
                        if *dt > session.close {
                            return None;
                        }
                        Some((session.open.to_utc(), session.close.to_utc()))
                    })
                })
                .collect()
        };

        if intervals.is_empty() {
            return Err("No intervals available".to_string());
        }

        // 2. Sort by open time
        intervals.sort_by_key(|(open, _)| *open);

        // 3. Merge allowing max 5 min gap
        let max_gap = Duration::from_secs(5 * 60);

        let (_, mut current_end) = intervals[0];

        for (open, close) in intervals.into_iter().skip(1) {
            if open <= current_end + max_gap {
                // Extend coverage
                current_end = current_end.max(close);
            } else {
                // Gap too large → stop here
                break;
            }
        }

        Ok(current_end)
    }

    fn get_next_earliest_available_data(
        &self,
        contracts: &[HashContract],
        dt: &DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String> {
        let earliest_dt = {
            let schedules = self
                .schedules
                .read()
                .expect("Expected read lock for schedules not to be poisoned");
            contracts
                .iter()
                .filter_map(|contract| {
                    let schedule = schedules
                        .get(contract)
                        .ok_or_else(|| format!("Couldn't find contract in schedules: {contract:?}"))
                        .ok()?;

                    schedule.schedule.iter().find_map(|(_, session)| {
                        let session = session.as_ref()?;

                        if dt.with_timezone(&schedule.time_zone) < session.close {
                            Some(session.open)
                        } else {
                            None
                        }
                    })
                })
                .min()
        };

        earliest_dt
            .map(|dt| dt.to_utc())
            .ok_or_else(|| "Could not get earliest datetime available for contracts!".to_string())
    }
}

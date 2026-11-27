//! Per-contract trading-hours schedules, answered from IBKR contract details.
//!
//! [`IbkrContractScheduler`] lazily fetches a contract's schedule on first use
//! (one IBKR request, 10 s timeout) and keeps a queue of `[open, close]`
//! intervals in UTC. Callers ask "is this contract trading now?" or "when do
//! these contracts next become available/unavailable?".
//!
//! The IBKR gateway must have "Expose whole trading schedule to API" enabled;
//! otherwise only today's hours are returned and the queue drains rapidly.

use std::{
    cell::UnsafeCell,
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use chrono::{DateTime, NaiveDateTime, Utc};
use ibapi::{Client, prelude::Contract};

use crate::helpers::sync_timeout::timeout;

// #[derive(Debug, Clone)]
// pub struct TradingHours {
//     open: i64,
//     close: i64,
// }
//
// #[derive(Debug, Clone)]
// pub struct Schedule {
//     time_zone: Tz,
//     schedule: BTreeMap<NaiveDate, Option<TradingHours>>,
// }

/// Trading-hours questions answered per contract.
///
/// Implementations are queried both from the bar producers (should I poll?) and
/// from the align/prime helpers (what is the next boundary?), so they must be
/// cheap after warm-up and safe to call concurrently.
pub trait ContractScheduler {
    /// Register one contract and fetch its schedule up front.
    fn add_schedule(&mut self, contract: &Contract) -> Result<(), String>;
    /// Register several contracts, fetching their schedules in parallel threads.
    fn add_all_schedules<I>(&mut self, contracts: I) -> Result<(), String>
    where
        I: IntoIterator<Item = Contract>;
    /// `true` when `now` is inside the current `[open, close)` interval.
    fn is_trading(&self, contract: &Contract) -> Result<bool, String>;
    /// Latest close across the given contracts that are currently trading.
    ///
    /// Returns "now" when none of them is trading.
    fn get_next_latest_unavailable_data<'a>(
        &self,
        contracts: impl IntoIterator<Item = &'a Contract>,
    ) -> Result<DateTime<Utc>, String>;
    /// Earliest open among the given contracts.
    ///
    /// Returns "now" as soon as any contract is already trading.
    fn get_next_earliest_available_data<'a>(
        &self,
        contracts: impl IntoIterator<Item = &'a Contract>,
    ) -> Result<DateTime<Utc>, String>;
}

/// One `[open, close)` window, as UTC epoch seconds.
#[derive(Debug, Clone, Copy)]
pub struct Interval {
    open: i64,
    close: i64,
}

/// IBKR-backed scheduler: schedules keyed by `contract_id`.
///
/// Each contract has a queue of intervals plus an `AtomicBool` "under edit"
/// spin lock; the queue lives behind `UnsafeCell` and is only touched by the
/// lock holder.
#[derive(Debug)]
pub struct IbkrContractScheduler {
    client: Arc<Client>,
    is_under_editing: Arc<HashMap<i32, AtomicBool>>,
    // Contract id -> Schedule
    schedules: Arc<HashMap<i32, UnsafeCell<VecDeque<Interval>>>>,
}

// SAFETY: every access to a `VecDeque` goes through its contract's
// `is_under_editing` flag (acquired with a CAS spin loop, released by an RAII
// guard). The maps themselves are only mutated while `&mut self` is held and
// no clones exist (`Arc::get_mut` asserts that).
unsafe impl Send for IbkrContractScheduler {}
unsafe impl Sync for IbkrContractScheduler {}

impl IbkrContractScheduler {
    /// Empty scheduler bound to an IBKR client; schedules are added explicitly.
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            is_under_editing: Arc::new(HashMap::new()),
            schedules: Arc::new(HashMap::new()),
        }
    }

    /// Whether a schedule is already tracked for this contract id.
    pub fn contains_contract(&self, contract: &Contract) -> bool {
        self.schedules.contains_key(&contract.contract_id)
    }

    /// Ask IBKR for the contract's hours and convert them to UTC intervals.
    ///
    /// Prefers `liquid_hours` when present, else `trading_hours`; skips empty
    /// entries and `:CLOSED` days; parses `YYYYMMDD:HHMM` in the contract's
    /// timezone.
    ///
    /// # Errors
    ///
    /// The 10 s details request times out, the timezone id is unknown, or a day
    /// string cannot be parsed.
    ///
    /// # Panics
    ///
    /// IBKR returned no contract details (`.first().unwrap()`).
    fn fetch_schedule(&self, contract: &Contract) -> Result<VecDeque<Interval>, String> {
        let client = self.client.clone();
        let cloned_contract = contract.clone();
        match timeout(Duration::from_secs(10), move || {
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

                let mut schedule = VecDeque::new();
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
                        let (open, close) = (
                            open_formatted.and_local_timezone(tz).unwrap(),
                            close_formatted.and_local_timezone(tz).unwrap(),
                        );
                        schedule.push_back(Interval {
                            open: open.timestamp(),
                            close: close.timestamp(),
                        });
                    } else if day.contains(":CLOSED") {
                    } else {
                        tracing::warn!(
                            "Pattern for days returned in contract details not found: {day:?}"
                        )
                    }
                }

                Ok(schedule)
            }
            Err(_) => return Err("Request for contract details to IBKR timed out!".to_string()),
        }
    }

    /// Fetch and store the schedule when it is still empty.
    ///
    /// Takes the contract's edit flag with a CAS spin lock (RAII-released),
    /// double-checks emptiness, fetches, sorts by open time and drops intervals
    /// that have already closed.
    ///
    /// # Errors
    ///
    /// The contract is unknown, the IBKR fetch fails, or the response is empty.
    fn update_schedule(&self, contract: &Contract) -> Result<(), String> {
        // =====================
        // skip if alr have data
        // =====================
        let schedule_cell = self
            .schedules
            .get(&contract.contract_id)
            .ok_or_else(|| "schedules tracked does not contain contract's schedule".to_string())?;

        let contract_under_edit = self
            .is_under_editing
            .get(&contract.contract_id)
            .expect("Expected is_under_editing to also have contract");

        // Atomic test-and-set instead of load-then-store — closes the TOCTOU
        // window where two threads could both pass the check and both proceed.
        while contract_under_edit
            .compare_exchange_weak(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            std::hint::spin_loop();
        }

        // RAII guard: releases the lock on every exit path, including panics.
        struct LockGuard<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for LockGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, std::sync::atomic::Ordering::Release);
            }
        }
        let _guard = LockGuard(contract_under_edit);

        // Safe to dereference now: the lock guarantees no other thread is
        // concurrently reading or writing this cell.
        let orig_schedule = unsafe {
            schedule_cell
                .get()
                .as_mut()
                .expect("Expected to be able to get schedule for contract")
        };
        if !orig_schedule.is_empty() {
            return Ok(());
        }

        // Fetch Schedule
        let mut schedule = self.fetch_schedule(contract)?;
        if schedule.is_empty() {
            return Err("Empty schedule returned!".to_string());
        }
        schedule
            .make_contiguous()
            .sort_by(|interval_a, interval_b| interval_a.open.cmp(&interval_b.open));

        let now = Utc::now().timestamp();
        while schedule.front().unwrap().close < now {
            schedule.pop_front();
        }
        orig_schedule.extend(schedule);

        Ok(())
    }

    /// Current time and the first not-yet-closed interval.
    ///
    /// Lazily refetches when the queue is empty and advances past expired
    /// intervals under the edit flag.
    ///
    /// # Errors
    ///
    /// The contract is unknown or a refetch fails.
    ///
    /// # Panics
    ///
    /// The contract has no entry in the scheduler, or a queue entry vanished
    /// while locked.
    fn get_interval(&self, contract: &Contract) -> Result<(i64, Interval), String> {
        let schedule_cell = self
            .schedules
            .get(&contract.contract_id)
            .expect(
                format!(
                    "Expected contract id entry to be in contract scheduler: ({},{},{})",
                    contract.symbol, contract.primary_exchange, contract.currency
                )
                .as_str(),
            )
            .get();
        unsafe {
            let schedule = schedule_cell
                .as_ref()
                .expect("Expected schedule cell to not be None");
            if schedule.is_empty() {
                self.update_schedule(contract)?;
                return self.get_interval(contract);
            }

            let now = Utc::now().timestamp();
            let interval = schedule.front().unwrap();
            if now < interval.close {
                return Ok((now, *interval));
            }

            let contract_under_edit = self
                .is_under_editing
                .get(&contract.contract_id)
                .expect("Expected is_under_editing to also have contract");

            // Atomic test-and-set instead of load-then-store — closes the TOCTOU
            // window where two threads could both pass the check and both proceed.
            while contract_under_edit
                .compare_exchange_weak(
                    false,
                    true,
                    std::sync::atomic::Ordering::Acquire,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_err()
            {
                std::hint::spin_loop();
            }

            let schedule_mut = schedule_cell
                .as_mut()
                .expect("Expected schedule cell to not be null");
            loop {
                if schedule_mut.is_empty() {
                    contract_under_edit.store(false, std::sync::atomic::Ordering::Release);
                    return self.get_interval(contract);
                }
                if now > schedule_mut.front().unwrap().close {
                    schedule_mut.pop_front();
                    continue;
                }

                contract_under_edit.store(false, std::sync::atomic::Ordering::Release);
                return Ok((now, *schedule_mut.front().unwrap()));
            }
        }
    }
}

/// must set option in global config api settings to return 1 month of trading hours
/// option: 'Expose whole trading schedule to api ...'
impl ContractScheduler for IbkrContractScheduler {
    fn add_schedule(&mut self, contract: &Contract) -> Result<(), String> {
        if self.schedules.contains_key(&contract.contract_id) {
            return Ok(());
        }

        {
            Arc::get_mut(&mut self.schedules)
                .expect("Expected there to only be one strong reference to schedules")
                .insert(contract.contract_id, UnsafeCell::new(VecDeque::new()));
            Arc::get_mut(&mut self.is_under_editing)
                .expect("Expected there to only be one strong reference to schedules")
                .insert(contract.contract_id, AtomicBool::new(false));
        }

        self.update_schedule(contract)?;

        Ok(())
    }

    fn add_all_schedules<I>(&mut self, contracts: I) -> Result<(), String>
    where
        I: IntoIterator<Item = Contract>,
    {
        let mut contracts_vec = Vec::new();
        for contract in contracts {
            if self.schedules.contains_key(&contract.contract_id) {
                continue;
            }

            {
                Arc::get_mut(&mut self.schedules)
                    .expect("Expected there to only be one strong reference to schedules")
                    .insert(contract.contract_id, UnsafeCell::new(VecDeque::new()));
                Arc::get_mut(&mut self.is_under_editing)
                    .expect("Expected there to only be one strong reference to schedules")
                    .insert(contract.contract_id, AtomicBool::new(false));
            }
            contracts_vec.push(contract);
        }

        let handle = std::thread::scope(|s| {
            let thread_handles = contracts_vec
                .iter()
                .map(|contract| s.spawn(|| self.update_schedule(contract)));
            let cum_err = thread_handles
                .filter_map(move |handle| {
                    let res = handle
                        .join()
                        .map_err(|e| format!("Thread for updating schedule panicked: {e:?}"))
                        .and_then(|update_schedule_res| update_schedule_res);
                    if let Err(e) = res { Some(e) } else { None }
                })
                .collect::<Vec<String>>();
            cum_err.join("\n")
        });

        if handle.is_empty() {
            Ok(())
        } else {
            Err(handle)
        }
    }

    fn is_trading(&self, contract: &Contract) -> Result<bool, String> {
        let (now, interval) = self.get_interval(contract)?;
        Ok(now >= interval.open)
    }

    /// returns current time if no contracts being traded currently
    fn get_next_latest_unavailable_data<'a>(
        &self,
        contracts: impl IntoIterator<Item = &'a Contract>
    ) -> Result<DateTime<Utc>, String> {
        let mut latest_time = Utc::now().timestamp();
        for contract in contracts {
            let (now, interval) = self.get_interval(contract)?;
            // skip contracts that are currently alr unavailable
            if now < interval.open {
                continue;
            }
            latest_time = latest_time.max(interval.close);
        }
        Ok(DateTime::from_timestamp(latest_time, 0).expect(
            "Expected Datetime utc to be constructible from get_next_latest_unavailable_data",
        ))
    }

    fn get_next_earliest_available_data<'a>(
        &self,
        contracts: impl IntoIterator<Item = &'a Contract>
    ) -> Result<DateTime<Utc>, String> {
        let mut earliest_time = i64::MAX;
        for contract in contracts {
            let (now, interval) = self.get_interval(contract)?;
            // skip contracts that are currently alr unavailable
            if now >= interval.open {
                return Ok(Utc::now());
            }
            earliest_time = earliest_time.min(interval.open);
        }
        Ok(DateTime::from_timestamp(earliest_time, 0).expect(
            "Expected Datetime utc to be constructible from get_next_latest_unavailable_data",
        ))
    }
}

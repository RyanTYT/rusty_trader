use chrono::{DateTime, Datelike, Days, NaiveTime, Utc};
use chrono_tz::Tz;
use chrono_tz::US::Eastern;

pub trait BrokerScheduler {
    /// should only be called when broker is currently unavailable
    fn get_next_broker_available(&self) -> Result<DateTime<Tz>, String>;
    /// should only be called when broker is currently available
    fn get_next_broker_unavailable(&self) -> Result<DateTime<Tz>, String>;
}

pub enum BrokerState {
    Available,
    Unavailable,
}

pub trait BrokerStateChecker {
    fn get_current_state(&self) -> BrokerState;
}

#[derive(Clone, Debug)]
pub enum IbkrRegion {
    NorthAmerica,
    Europe,
    Apac,
}

pub struct MaintenanceWindow {
    pub start_day: chrono::Weekday,
    pub start_time: NaiveTime,
    pub end_day: chrono::Weekday,
    pub end_time: NaiveTime,
}

impl IbkrRegion {
    /// Defines the schedule (Fri 23:00 - Sat 03:00 ET)
    pub fn schedule(&self) -> MaintenanceWindow {
        MaintenanceWindow {
            start_day: chrono::Weekday::Fri,
            start_time: NaiveTime::from_hms_opt(23, 0, 0).unwrap(),
            end_day: chrono::Weekday::Sat,
            end_time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
        }
    }

    /// Checks if a specific UTC time falls within the maintenance window
    pub fn is_in_maintenance(&self, time_utc: DateTime<Utc>) -> bool {
        let et_time = time_utc.with_timezone(&Eastern);
        let weekday = et_time.weekday();
        let time = et_time.time();

        let s = self.schedule();

        // Logic for window crossing midnight (Friday night into Saturday morning)
        match weekday {
            chrono::Weekday::Fri => time >= s.start_time,
            chrono::Weekday::Sat => time < s.end_time,
            _ => false,
        }
    }
}

// ===============
// Implementations
// ===============

#[derive(Clone, Debug)]
pub struct IbkrStateService {
    pub ibkr_region: IbkrRegion,
}

impl BrokerStateChecker for IbkrStateService {
    fn get_current_state(&self) -> BrokerState {
        let now = Utc::now();
        if self.ibkr_region.is_in_maintenance(now) {
            BrokerState::Unavailable
        } else {
            BrokerState::Available
        }
    }
}

impl BrokerScheduler for IbkrRegion {
    fn get_next_broker_unavailable(&self) -> Result<DateTime<Tz>, String> {
        let dt_now = Utc::now();
        let schedule = self.schedule();
        let mut next_open_day = dt_now.with_timezone(&Eastern);
        while next_open_day.weekday() != schedule.start_day {
            next_open_day = next_open_day
                .checked_add_days(Days::new(1))
                .ok_or("Date overflow")?;
        }
        Ok(next_open_day.with_time(schedule.start_time).unwrap())
    }

    fn get_next_broker_available(&self) -> Result<DateTime<Tz>, String> {
        let dt_now = Utc::now();
        let schedule = self.schedule();
        let open_day = if dt_now.with_timezone(&Eastern).weekday() == schedule.start_day {
            let tmr = dt_now
                .with_timezone(&Eastern)
                .checked_add_days(Days::new(1))
                .ok_or("Date overflow")?;
            tmr
        } else {
            dt_now.with_timezone(&Eastern)
        };

        let open_res = open_day
            .with_time(schedule.end_time)
            .single()
            .ok_or("Couldn't get time of maintenance window open")?;
        Ok(open_res)
    }
}

impl BrokerScheduler for IbkrStateService {
    fn get_next_broker_unavailable(&self) -> Result<DateTime<Tz>, String> {
        self.ibkr_region.get_next_broker_unavailable()
    }
    fn get_next_broker_available(&self) -> Result<DateTime<Tz>, String> {
        self.ibkr_region.get_next_broker_available()
    }
}

// /// Sleeps until the Stock market opens (09:30 ET on a business day).
// /// If the market is currently open, returns immediately.
// async fn sleep_until_next_stock_market_open() {
//     let now = Utc::now().with_timezone(&New_York);
//
//     // Define Market Hours
//     let open_time = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
//     let close_time = NaiveTime::from_hms_opt(16, 5, 0).unwrap();
//
//     let today_date = now.date_naive();
//     let is_today_busday = today_date
//         .is_busday()
//         .expect("Expected short lookahead to not cause RangeError for nyse_holiday_cal");
//     let current_time = now.time();
//
//     // 1. If currently open, return immediately.
//     // Condition: Today is busday AND time is between 09:30 and 16:05
//     if is_today_busday && current_time >= open_time && current_time < close_time {
//         return;
//     }
//
//     // 2. Calculate the target Open Datetime.
//     // Case A: Today is a business day and we are simply early (before 09:30).
//     // Target -> Today at 09:30.
//     let target_date = if is_today_busday && current_time < open_time {
//         today_date
//     }
//     // Case B: We are past close, or today is a holiday/weekend.
//     // Target -> The Next Valid Business Day at 09:30.
//     else {
//         let mut candidate = today_date + chrono::Duration::days(1);
//         // "Iterator" approach: Loop days until we hit a valid business day
//         while !candidate
//             .is_busday()
//             .expect("Expected not to hit RangeError when determining next valid business day")
//         {
//             candidate += chrono::Duration::days(1);
//         }
//         candidate
//     };
//
//     // 3. Construct the exact target Datetime
//     let target_dt = New_York
//         .with_ymd_and_hms(
//             target_date.year(),
//             target_date.month(),
//             target_date.day(),
//             9,
//             30,
//             0,
//         )
//         .unwrap();
//
//     // 4. Sleep
//     let duration = target_dt - now;
//     if duration.num_seconds() > 0 {
//         sleep(std::time::Duration::from_secs(duration.num_seconds() as u64)).await;
//     }
// }
//
// async fn sleep_until_stock_market_close() {
//     let now_eastern = Utc::now().with_timezone(&New_York);
//     let close_time = New_York
//         .with_ymd_and_hms(
//             now_eastern.year(),
//             now_eastern.month(),
//             now_eastern.day(),
//             16,
//             5,
//             0,
//         )
//         .unwrap();
//
//     tracing::info!("check if is in this fn");
//     if now_eastern < close_time {
//         let duration = close_time - now_eastern;
//         let duration = Duration::from_secs(duration.num_seconds() as u64 + 30);
//         println!(
//             "Sleeping until market close in {} seconds...",
//             duration.as_secs()
//         );
//         tokio::time::sleep(duration).await;
//     } else {
//         println!("Market already closed.");
//     }
// }
//
// /// Sleeps until the Forex market opens.
// /// If currently open, returns immediately.
// async fn sleep_until_next_forex_market_open() {
//     let now = Utc::now().with_timezone(&New_York);
//
//     // 1. If already open, no need to sleep
//     if Consolidator::is_fx_trading_datetime(&now) {
//         return;
//     }
//
//     // 2. Seek forward to find the next open minute
//     // We start checking from 'now' and advance by 1 minute until valid.
//     // This handles weekends and holidays automatically via your robust function.
//     let mut target_dt = now;
//     while !Consolidator::is_fx_trading_datetime(&target_dt) {
//         // Advance by 1 minute (robust enough for trading schedules)
//         target_dt = target_dt + chrono::Duration::minutes(1);
//     }
//
//     // 3. Sleep
//     let duration = target_dt - now;
//     if duration.num_seconds() > 0 {
//         sleep(std::time::Duration::from_secs(duration.num_seconds() as u64)).await;
//     }
// }
//
// /// Sleeps until the Forex market closes.
// /// If currently closed, sleeps until the next Forex market close
// async fn sleep_until_forex_market_close() {
//     let now = Utc::now().with_timezone(&New_York);
//
//     // 1. If currently closed, find next time that it is open
//     let mut target_dt = now;
//     while !Consolidator::is_fx_trading_datetime(&target_dt) {
//         target_dt = target_dt + chrono::Duration::minutes(1);
//     }
//
//     // 2. Seek forward to find when it stops being valid
//     while Consolidator::is_fx_trading_datetime(&target_dt) {
//         target_dt = target_dt + chrono::Duration::minutes(1);
//     }
//
//     // 3. Sleep
//     let duration = target_dt - now;
//     if duration.num_seconds() > 0 {
//         sleep(std::time::Duration::from_secs(
//             duration.num_seconds() as u64 + 30,
//         ))
//         .await;
//     }
// }
//
// #[derive(Debug, Clone)]
// enum IBCState {
//     Stopped,
//     AutoRestarting,
//     Running,
// }
//
// /// Sleeps until BOTH Stock and Forex markets are closed.
// ///
// /// Logic:
// /// - Uses `is_fx_trading_datetime` for Forex.
// /// - Uses `nyse_holiday_cal` logic for Stocks (checking business days + hours).
// /// - If currently closed -> Sleeps until they open, then sleeps until they close.
// async fn sleep_until_all_markets_closed(interrupt_rcx: &mut Receiver<ConnectionAlert>) -> IBCState {
//     let now = Utc::now().with_timezone(&New_York);
//     let stock_close_time = NaiveTime::from_hms_opt(16, 5, 0).unwrap();
//
//     // Helper: Check if stocks are trading based on NYSE calendar + Time
//     let is_stock_open = |dt: DateTime<Tz>| -> bool {
//         let date = dt.date_naive();
//         let time = dt.time();
//
//         // Check 1: Is today a valid business day? (skips weekends & holidays)
//         // Adjust 'nyse_holiday_cal::is_business_day' to match your exact API signature
//         let is_busday = date
//             .is_busday()
//             .expect("Expected to be able to determine if is_busday for a short lookahead");
//
//         // Check 2: Are we within trading hours? (assuming 09:30 start, but strictly checking < close here)
//         // If you need strictly between 09:30 and 16:05, add: && time >= NaiveTime::from_hms(9,30,0)
//         is_busday && time < stock_close_time
//     };
//
//     // Helper: Is ANY market currently active?
//     let is_any_open = |dt: DateTime<Tz>| -> bool {
//         Consolidator::is_fx_trading_datetime(&dt) || is_stock_open(dt)
//     };
//
//     let mut target_dt = now;
//
//     // PHASE 1: Escape the "Currently Closed" trap.
//     // If we are currently fully closed (e.g., Saturday), we must first fast-forward
//     // to when markets actually wake up (e.g., Sunday 17:00).
//     if !is_any_open(target_dt) {
//         // Seek forward until at least one market opens
//         while !is_any_open(target_dt) {
//             target_dt = target_dt + chrono::Duration::minutes(1);
//         }
//     }
//
//     // PHASE 2: Find the actual close.
//     // Now that we are (or will be) in an open state, seek forward until
//     // BOTH markets report as closed.
//     while is_any_open(target_dt) {
//         target_dt = target_dt + chrono::Duration::minutes(1);
//     }
//     target_dt += chrono::Duration::seconds(30);
//
//     // Calculate and Sleep
//     loop {
//         let now = Utc::now().with_timezone(&New_York);
//         let remaining = target_dt - now;
//
//         if remaining.num_seconds() <= 0 {
//             tracing::info!("Completed sleeping until all markets closed!");
//             return IBCState::Running;
//         }
//         tracing::info!("Sleeping for {remaining:?}s");
//
//         if interrupt_rcx.is_closed() {
//             sleep(std::time::Duration::from_secs(
//                 remaining.num_seconds() as u64
//             ))
//             .await;
//             return IBCState::Running;
//         } else {
//             tokio::select! {
//                 _ = sleep(std::time::Duration::from_secs(remaining.num_seconds() as u64)) => {
//                     tracing::info!("Completed sleeping until all markets closed!");
//                     return IBCState::Running;
//                 }
//                 msg = interrupt_rcx.recv() => {
//                     match msg {
//                         Some(alert) => {
//                             match alert {
//                                 ConnectionAlert::UnstableConnectionDuringMarketHours {
//                                     first_event_time,
//                                     timeout_occurred
//                                 } => {
//                                     tracing::warn!(
//                                         message=%format!(
//                                             "🚨 CRITICAL: Unstable connection during market hours at {}",
//                                             first_event_time
//                                         )
//                                     );
//                                     if timeout_occurred {
//                                         eprintln!("   Timeout and re-subscription occurred - data quality compromised!");
//                                         return IBCState::Running;
//                                     }
//                                     // Handle the interruption - maybe exit, send notification, etc.
//                                     tracing::warn!(
//                                         "🚨 CRITICAL: No timeout occurred, cautiously proceeding",
//                                     );
//                                     continue;
//                                 }
//                                 ConnectionAlert::UnstableConnectionOutsideMarketHours {
//                                     first_event_time
//                                 } => {
//                                     tracing::warn!(
//                                         message=%format!(
//                                             "Connection instability detected outside market hours at {}, will be restarting in case",
//                                             first_event_time
//                                         )
//                                     );
//                                     return IBCState::Running;
//                                 }
//                                 ConnectionAlert::BrokenPipe {
//                                     first_event_time
//                                 } => {
//                                     if first_event_time.to_utc().hour() == 12 && first_event_time.to_utc().minute() < 3 {
//                                         tracing::warn!("Broken pipe encountered at {first_event_time:?}, during zone of IBC Autorestart - disconnecting and restarting now");
//                                         return IBCState::AutoRestarting;
//                                     }
//                                     tracing::warn!("Broken pipe encountered at {first_event_time:?}, outside of IBC Autorestart - disconnecting completely and booting IBC again");
//                                     return IBCState::Running;
//                                 }
//                                 ConnectionAlert::APACRESET {
//                                     first_event_time
//                                 } => {
//                                     tracing::warn!("Disconnected during APAC reset region, restarting now: {first_event_time:?}");
//                                     return IBCState::Running;
//                                 }
//                             }
//                         }
//                         None => continue, // channel closed
//                     }
//                 }
//             }
//         }
//     }
// }
//
// async fn sleep_thru_system_maintenance() {
//     let now = Utc::now().with_timezone(&New_York);
//     let is_weekend_maintenance = (now.weekday() == Weekday::Fri && now.hour() == 22 && now.minute() >= 55) // 5 min leeway
//             || (now.weekday() == Weekday::Fri && now.hour() == 23)
//             || (now.weekday() == Weekday::Sat && now.hour() < 3);
//     if is_weekend_maintenance {
//         let sleep_till_next_day = {
//             let is_until_next_day = now.weekday() == Weekday::Fri;
//             if is_until_next_day {
//                 Duration::from_secs(60 * 60 * 24)
//             } else {
//                 Duration::from_secs(0)
//             }
//         };
//         let sleep_until = (now + sleep_till_next_day)
//             .with_time(NaiveTime::from_hms_nano_opt(2, 30, 0, 0).unwrap())
//             .unwrap();
//         let sleep_duration = sleep_until - now;
//         if sleep_until > now {
//             sleep(sleep_duration.to_std().expect("Failed to convert sleep duration calculated in sleep_thru_system_maintenance to Duration")).await;
//             return;
//         }
//         return;
//     }
// }

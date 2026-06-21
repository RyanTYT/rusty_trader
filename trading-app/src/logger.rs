use chrono::{DateTime, Datelike, NaiveTime, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use chrono_tz::Asia::Hong_Kong;
use chrono_tz::Tz;
use nyse_holiday_cal::HolidayCal;
use sqlx::PgPool;
use std::fmt::{Debug, Write};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::{self};
use std::time::Instant;
use tokio::sync::mpsc::{self};
use tokio::task;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

use crate::market_data::consolidator::Consolidator;
use crate::market_data::strategy_scheduler::StrategyScheduler;

#[allow(dead_code)]
struct FieldVisitor {
    pub output: String,
}

#[allow(dead_code)]
impl FieldVisitor {
    fn new() -> Self {
        FieldVisitor {
            output: String::new(),
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.output, "{}={:?} ", field.name(), value);
    }
}

#[allow(dead_code)]
#[derive(Default)]
struct IbMessageVisitor {
    pub ib_message: String,
}

impl Visit for IbMessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.ib_message, "{:?}", value);
        }
    }
}

#[allow(dead_code)]
// Helper functions
fn is_stock_open_hard(dt: &DateTime<Tz>) -> bool {
    let date = dt.date_naive();
    let time = dt.time();

    let stock_open_time = chrono::NaiveTime::from_hms_opt(9, 25, 0).unwrap();
    let stock_close_time = chrono::NaiveTime::from_hms_opt(16, 5, 0).unwrap();

    let is_busday = date
        .is_busday()
        .expect("Expected to be able to determine if is_busday");

    is_busday && time < stock_close_time && time > stock_open_time
}

#[allow(dead_code)]
fn is_autorestart(dt: DateTime<Utc>) -> bool {
    (dt.hour() == 23 && dt.minute() >= 55) || (dt.hour() == 0 && dt.minute() <= 5)
}

#[allow(dead_code)]
fn is_apac_reset_now(now_utc: &DateTime<chrono::Utc>) -> bool {
    let now_hkt = now_utc.with_timezone(&Hong_Kong);
    let weekday = now_hkt.weekday();

    // ±5 minute lax
    let lax = chrono::Duration::minutes(5);

    // -------------------------------------------------
    // Daily APAC resets (Sunday–Friday, HKT)
    // -------------------------------------------------
    let daily_resets = [
        // First reset: 04:45–06:05 HKT
        (4, 40, 6, 10),
        // Second reset: 20:15–21:15 HKT
        (20, 10, 21, 20),
    ];

    let is_daily_day = matches!(
        weekday,
        Weekday::Sun | Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri
    );

    if is_daily_day {
        for (sh, sm, eh, em) in daily_resets {
            let start = now_hkt
                .with_time(NaiveTime::from_hms_opt(sh, sm, 0).unwrap())
                .unwrap()
                - lax;

            let end = now_hkt
                .with_time(NaiveTime::from_hms_opt(eh, em, 0).unwrap())
                .unwrap()
                + lax;

            if now_hkt >= start && now_hkt <= end {
                return true;
            }
        }
    }

    // -------------------------------------------------
    // Weekend reset (ET): Friday 23:00 → Saturday 03:00
    // Convert dynamically to HKT
    // -------------------------------------------------
    let now_et = now_utc.with_timezone(&New_York);

    let is_weekend_window = {
        let et_weekday = now_et.weekday();

        // Friday window start
        let fri_start = if et_weekday == Weekday::Fri {
            Some(
                now_et
                    .with_time(NaiveTime::from_hms_opt(23, 0, 0).unwrap())
                    .unwrap()
                    - lax,
            )
        } else {
            None
        };

        // Saturday window end
        let sat_end = if et_weekday == Weekday::Sat {
            Some(
                now_et
                    .with_time(NaiveTime::from_hms_opt(3, 0, 0).unwrap())
                    .unwrap()
                    + lax,
            )
        } else {
            None
        };

        match (fri_start, sat_end) {
            (Some(start), _) => now_et >= start,
            (_, Some(end)) => now_et <= end,
            _ => false,
        }
    };

    is_weekend_window
}

fn is_any_open(dt: &DateTime<Tz>) -> bool {
    Consolidator::is_fx_trading_datetime(dt) || is_stock_open_hard(dt)
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ConnectionAlert {
    UnstableConnectionDuringMarketHours {
        first_event_time: DateTime<Tz>,
        timeout_occurred: bool,
    },
    UnstableConnectionOutsideMarketHours {
        first_event_time: DateTime<Tz>,
    },
    BrokenPipe {
        first_event_time: DateTime<Tz>,
    },
    APACRESET {
        first_event_time: DateTime<Tz>,
    },
    AutoRestarting,
}

#[derive(Clone)]
pub(crate) struct ConnectionState {
    // Track 1100/1102 pattern
    saw_1100: Option<Instant>,
    saw_1102: Option<Instant>,
    saw_111: Option<Instant>,

    // Track if we're waiting for timeout log
    waiting_for_timeout: Option<Instant>,
    // timeout_log_received: bool,
    timeout_log_tx: Arc<Mutex<Option<Sender<bool>>>>,
    is_timeout_log_channel_open: Arc<Mutex<Option<bool>>>,

    // Alert channel
    alert_tx: Option<mpsc::Sender<ConnectionAlert>>,
}

#[allow(dead_code)]
impl ConnectionState {
    pub(crate) fn new(alert_tx: mpsc::Sender<ConnectionAlert>) -> Self {
        Self {
            saw_1100: None,
            saw_1102: None,
            saw_111: None,
            waiting_for_timeout: None,
            // timeout_log_received: false,
            timeout_log_tx: Arc::new(Mutex::new(None)),
            is_timeout_log_channel_open: Arc::new(Mutex::new(None)),
            alert_tx: Some(alert_tx),
        }
    }

    fn reset(&mut self) {
        self.saw_1100 = None;
        self.saw_1102 = None;
        self.saw_111 = None;
        self.waiting_for_timeout = None;
        self.timeout_log_tx
            .lock()
            .expect("Expected to be able to acquire lock for timeout_log_tx")
            .take();
        self.is_timeout_log_channel_open
            .lock()
            .expect("Expected to be able to acquire lock for is_timeout_log_channel_open")
            .take();
    }

    fn wait_for_timeout(
        state_clone: Arc<Mutex<ConnectionState>>,
        event_time: DateTime<Tz>,
        rx: std::sync::mpsc::Receiver<bool>,
    ) {
        thread::spawn(move || {
            // wait for possible reconnection w fix
            std::thread::sleep(std::time::Duration::from_secs(60));
            match rx.recv_timeout(std::time::Duration::from_secs(5 * 60)) {
                Ok(_timeout_log_received) => {
                    println!("Received timeout_log => sending to alert_tx");
                    let alert_tx_opt = { state_clone.lock().unwrap().alert_tx.clone() };
                    if let Some(tx) = alert_tx_opt {
                        if let Err(e) =
                            tx.try_send(ConnectionAlert::UnstableConnectionDuringMarketHours {
                                first_event_time: event_time,
                                timeout_occurred: true,
                            })
                        {
                            println!("Error sending msg thru alert_tx due to {e:?}");
                        };
                    }
                    {
                        let mut state = state_clone.lock().unwrap();
                        state.reset();
                    }
                }
                Err(e) => {
                    let mut state = state_clone.lock().unwrap();
                    state.reset();
                    println!("Error trying to wait for receival of timeout log: {e:?}")
                }
            }
        });
    }

    fn check_pattern(
        &mut self,
        tz: &Tz,
        state_arc: Arc<Mutex<ConnectionState>>,
    ) -> Option<ConnectionAlert> {
        if let Some(t1100) = self.saw_1100 {
            // Pattern detected! Now check market hours
            if (Instant::now() - t1100) > std::time::Duration::from_secs(60) {
                return None;
            }

            let now = chrono::Utc::now().with_timezone(tz);
            if is_autorestart(now.to_utc()) {
                Some(ConnectionAlert::AutoRestarting)
            } else if is_apac_reset_now(&now.to_utc()) {
                // Market is closed - alert immediately
                let alert = ConnectionAlert::APACRESET {
                    first_event_time: now,
                };
                self.reset();
                Some(alert)
            } else if is_any_open(&now) {
                // Market is open - start waiting for timeout log
                self.waiting_for_timeout = Some(Instant::now());

                // Spawn a thread to check after 15 minutes
                let state_clone = state_arc.clone();
                let event_time = now;

                let mut timeout_log_tx = self
                    .timeout_log_tx
                    .lock()
                    .expect("Expected to be able to get timeout_log_rcx");
                let mut is_timeout_log_channel_open = self
                    .is_timeout_log_channel_open
                    .lock()
                    .expect("Expected to be able to get lock for is_timeout_log_channel_open");
                if timeout_log_tx.is_some()
                    && is_timeout_log_channel_open.is_some_and(|is_open| is_open)
                {
                    println!("Waiting for previous tx to expire!");
                } else {
                    let (tx, rx) = channel::<bool>();
                    is_timeout_log_channel_open.replace(true);
                    timeout_log_tx.replace(tx);
                    drop(timeout_log_tx);
                    drop(is_timeout_log_channel_open);
                    Self::wait_for_timeout(state_clone, event_time, rx);
                }

                None // Don't alert yet, wait for timeout check
            } else {
                // Market is closed - alert immediately
                let alert = ConnectionAlert::UnstableConnectionOutsideMarketHours {
                    first_event_time: now,
                };
                self.reset();
                Some(alert)
            }
        } else {
            None
        }
    }
}

pub(crate) struct IbConnectionLayer {
    state: Arc<Mutex<ConnectionState>>,
    tz: Tz,
}

#[allow(dead_code)]
impl IbConnectionLayer {
    pub(crate) fn new(state: Arc<Mutex<ConnectionState>>, tz: Tz) -> Self {
        Self { state, tz }
    }
}

impl<S> Layer<S> for IbConnectionLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        let mut visitor = IbMessageVisitor::default();
        event.record(&mut visitor);

        let mut state = self.state.lock().unwrap();

        if metadata.level() == &Level::ERROR
            && visitor
                .ib_message
                .to_lowercase()
                .contains("connection refused (os error 111)")
        {
            state.saw_111 = Some(Instant::now());
            println!("111 encountered");
            // Check if pattern is complete
            if let Some(tx) = &state.alert_tx {
                if let Err(e) = tx.try_send(ConnectionAlert::BrokenPipe {
                    first_event_time: Utc::now().with_timezone(&New_York),
                }) {
                    println!("Error sending msg thru alert_tx due to {e:?}");
                }
            }
        }

        // Check for IB message codes
        if metadata.level() == &Level::ERROR
            && visitor
                .ib_message
                .to_lowercase()
                .contains("error_code: 1100")
        {
            state.saw_1100 = Some(Instant::now());
            println!("1100 encountered");
            // Check if pattern is complete
            if let Some(alert) = state.check_pattern(&self.tz, self.state.clone()) {
                if let Some(tx) = &state.alert_tx {
                    if let Err(e) = tx.try_send(alert) {
                        println!("Error sending msg thru alert_tx due to {e:?}");
                    }
                }
            }
        }
        if metadata.level() == &Level::ERROR
            && visitor
                .ib_message
                .to_lowercase()
                .contains("error_code: 1102")
        {
            state.saw_1102 = Some(Instant::now());
            println!("1102 encountered");
            // Check if pattern is complete
            if let Some(alert) = state.check_pattern(&self.tz, self.state.clone()) {
                if let Some(tx) = &state.alert_tx {
                    if let Err(e) = tx.try_send(alert) {
                        println!("Error sending msg thru alert_tx due to {e:?}");
                    }
                }
            }
        }

        if metadata.level() == &Level::ERROR
            && visitor
                .ib_message
                .to_lowercase()
                .contains("Broken pipe: (os error 32)")
        {
            println!("broken pipe encountered");
            // Check if pattern is complete
            if let Some(tx) = &state.alert_tx {
                if let Err(e) = tx.try_send(ConnectionAlert::BrokenPipe {
                    first_event_time: Utc::now().with_timezone(&Tz::America__New_York),
                }) {
                    println!("Error sending msg thru alert_tx due to {e:?}");
                }
            }
        }

        // Check for timeout log message
        if metadata.level() == &Level::WARN {
            if visitor
                .ib_message
                .contains("timed out waiting for next bar for contract")
                && visitor.ib_message.contains("Trying a re-subscription")
            {
                println!("Timeout encountered");
                if state.waiting_for_timeout.is_some() {
                    println!("timeout reached and was waiting for timeout");
                    // state
                    let mut timeout_log_tx = state
                        .timeout_log_tx
                        .lock()
                        .expect("Expected to be able to retrieve lock for timeout_log_tx");
                    if timeout_log_tx.is_some() {
                        let tx = timeout_log_tx.take().unwrap();
                        let _ = tx.send(true);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    timestamp: DateTime<Tz>,
    level: String,
    target: String,
    message: String,
}

/// The channel writer that receives formatted logs
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ChannelLayer {
    pub(crate) sender: tokio::sync::mpsc::Sender<LogRecord>,
}

/// Custom Layer that extracts metadata and sends a LogRecord through the channel
impl<S> Layer<S> for ChannelLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();

        // Format the fields using the default formatter
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        // Don't log insertions into logs.logs
        let msg = visitor.output.to_lowercase();
        if msg.contains("insert into logs.logs") {
            return;
        }

        let now = chrono::Utc::now().with_timezone(&New_York);

        let record = LogRecord {
            timestamp: now,
            level: meta.level().to_string(),
            target: meta.target().to_string(),
            message: visitor.output.trim().to_string(),
        };

        let _ = self.sender.try_send(record);
    }
}

// pub fn init_logger() -> anyhow::Result<()> {
//     let (tx, mut rx) = mpsc::channel::<LogRecord>(1024);
//
//     // Spawn background task to write logs to DB
//     task::spawn(async move {
//         while let Some(record) = rx.recv().await {
//             // let _ = sqlx::query(
//             //     "INSERT INTO logs.logs (time, level, name, message) VALUES ($1, $2, $3, $4)",
//             // )
//             // .bind(record.timestamp)
//             // .bind(record.level)
//             // .bind(record.target)
//             // .bind(record.message)
//             // .execute(&pool)
//             // .await;
//             println!(
//                 "===========\nTime: {}\nLevel: {}\nTarget: {}\nMsg: {}\n==========",
//                 record.timestamp, record.level, record.target, record.message
//             );
//         }
//     });
//
//     let stdout_layer = fmt::layer().pretty().with_target(true); // show function/module name
//     // let db_layer = ChannelLayer { sender: tx };
//
//     tracing_subscriber::registry()
//         .with(stdout_layer)
//         // .with(db_layer)
//         .init();
//
//     Ok(())
// }

pub fn init_db_logger(pool: PgPool) -> tokio::sync::mpsc::Sender<LogRecord> {
    let (tx, mut rx) = mpsc::channel::<LogRecord>(1024);

    task::spawn(async move {
        while let Some(record) = rx.recv().await {
            let _ = sqlx::query(
                "INSERT INTO logs.logs (time, level, name, message) VALUES ($1, $2, $3, $4)",
            )
            .bind(record.timestamp)
            .bind(record.level)
            .bind(record.target)
            .bind(record.message)
            .execute(&pool)
            .await;
        }
    });

    tx
}

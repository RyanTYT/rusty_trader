use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use tokio::time::sleep;

use crate::{
    ibc::with_gateway_retry,
    init_app::ApplicationState,
    logger::ConnectionAlert,
    loop_until_async_drop,
    schedule::broker_scheduler::{
        BrokerScheduler, BrokerState, BrokerStateChecker, IbkrRegion, IbkrStateService,
    },
};

// #[derive(Debug, Clone)]
// enum IBCState {
//     Stopped,
//     AutoRestarting,
//     Running,
// }

async fn sleep_until(dt: &DateTime<Utc>) {
    let now = Utc::now();
    if now > *dt {
        return;
    }
    let sleep_duration_td = *dt - now;
    tracing::info!("Sleeping for {sleep_duration_td:?}");
    match sleep_duration_td.to_std() {
        Ok(sleep_duration) => sleep(sleep_duration).await,
        Err(e) => {
            tracing::warn!("Could not convert sleep duration timedelta to actual duration: {e:?}");
            return;
        }
    }
}

use tokio::signal::unix::{SignalKind, signal};

pub enum AppReturnState {
    BrokerDown,
    InitAppErr(String),
    NoBrokerSchedule(String),
    UnstableConnMktHours,
    UnstableConnOutsideHours(DateTime<Tz>),
    UnstableConnBrokenPipe,
    UnstableConnAPAC,
    SigintTerminalSignal,
    SigtermTerminalSignal,
}

impl AppReturnState {
    fn log_state(&self) {
        match self {
            Self::BrokerDown => {
                tracing::info!("IBKR system maintenance reached, tearing down now")
            }
            Self::InitAppErr(e) => {
                tracing::error!("Error trying to initialise application state: {e:?}")
            }
            Self::NoBrokerSchedule(e) => {
                tracing::error!("No broker schedule for some reason OR dateoverflow: {e:?}");
                panic!("No broker schedule for some reason OR dateoverflow: {e:?}")
            }
            Self::UnstableConnMktHours => {
                tracing::warn!(
                    "🚨 CRITICAL: Unstable connection during market hours and timeout occurred! Restarting now!"
                )
            }
            Self::UnstableConnOutsideHours(first_event_time) => {
                tracing::warn!(
                    "🚨 Unstable connection outside market hours at {first_event_time:?}, restarting just in case"
                )
            }
            Self::UnstableConnBrokenPipe => {
                tracing::warn!("🚨 Unstable connection because of broken pipe: restarting now")
            }
            Self::UnstableConnAPAC => {
                tracing::warn!("🚨 Unstable connection due to APACRESET! Restarting now!")
            }
            Self::SigintTerminalSignal => {
                tracing::error!("Received SIGNINT terminal signal: Shutting down now!")
            }
            Self::SigtermTerminalSignal => {
                tracing::error!("Received SIGTERM terminal signal: Shutting down now!")
            }
        }
    }

    fn is_terminal_state(&self) -> bool {
        match self {
            Self::SigintTerminalSignal | Self::SigtermTerminalSignal => true,
            _ => false,
        }
    }
}

pub async fn run_program<F, Fut>(
    init_application: F,
    interrupt_rcx: &mut tokio::sync::mpsc::Receiver<ConnectionAlert>,
    sender: tokio::sync::mpsc::Sender<Weak<ApplicationState>>,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<ApplicationState, String>>,
{
    let scheduler = IbkrStateService {
        ibkr_region: IbkrRegion::Apac,
    };

    // Register once outside the loop so we never miss a signal between iterations.
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");

    'outer: loop {
        match scheduler.get_current_state() {
            BrokerState::Available => {
                let cloned_scheduler = scheduler.clone();

                // Borrow references before passing into the closure to prevent moving values out of 'outer loop
                let init_application = &init_application;
                let sender = &sender;
                let interrupt_rcx = &mut *interrupt_rcx;
                let sigterm = &mut sigterm;
                let sigint = &mut sigint;

                let app_return_state_res = with_gateway_retry("...", 3, |_| async {
                    let mut app_state: Arc<ApplicationState> = match init_application().await {
                        Ok(app_state_res) => Arc::new(app_state_res),
                        Err(e) => {
                            return AppReturnState::InitAppErr(e.to_string());
                        }
                    };

                    if let Err(e) = sender.send(Arc::downgrade(&app_state)).await {
                        tracing::warn!("Failed to send app_state to server for use: {e:?}");
                    }

                    loop {
                        let next_unavailable = match cloned_scheduler.get_next_broker_unavailable() {
                            Ok(next_dt) => next_dt,
                            Err(e) => {
                                loop_until_async_drop!(app_state);
                                return AppReturnState::NoBrokerSchedule(e.to_string());
                            }
                        };

                        let next_unavailable_utc = next_unavailable.to_utc();
                        tokio::select! {
                            // Window expires
                            _ = sleep_until(&next_unavailable_utc) => {
                                loop_until_async_drop!(app_state);
                                return AppReturnState::BrokerDown;
                            }

                            // External interruption
                            Some(connection_alert) = interrupt_rcx.recv() => {
                                tracing::warn!("Interrupt Received: {connection_alert:?}");
                                match connection_alert {
                                    ConnectionAlert::UnstableConnectionDuringMarketHours {
                                        timeout_occurred,
                                        first_event_time: _
                                    } => {
                                        if timeout_occurred {
                                loop_until_async_drop!(app_state);
                                            return AppReturnState::UnstableConnMktHours;
                                        } else {
                                            tracing::warn!("🚨 Unstable connection during market hours but no timeout occurred so cautiously proceeding!");
                                            continue;
                                        }
                                    }
                                    ConnectionAlert::UnstableConnectionOutsideMarketHours { first_event_time } => {
                                loop_until_async_drop!(app_state);
                                        return AppReturnState::UnstableConnOutsideHours(first_event_time);
                                    }
                                    ConnectionAlert::BrokenPipe { first_event_time: _ } => {
                                loop_until_async_drop!(app_state);
                                        return AppReturnState::UnstableConnBrokenPipe;
                                    }
                                    ConnectionAlert::APACRESET { first_event_time: _ } => {
                                loop_until_async_drop!(app_state);
                                        return AppReturnState::UnstableConnAPAC;
                                    }
                                    ConnectionAlert::AutoRestarting => {
                                        tracing::warn!("IBKR is autorestarting! sleeping for a bit before continuing");
                                        sleep(Duration::from_secs(3 * 60)).await;
                                        continue;
                                    }
                                }
                            }

                            // Graceful shutdown
                            _ = sigterm.recv() => {
                                tracing::info!("SIGTERM received, producing final metrics report before exit");
                                loop_until_async_drop!(app_state);
                                return AppReturnState::SigtermTerminalSignal;
                            }
                            _ = sigint.recv() => {
                                tracing::info!("SIGINT received, producing final metrics report before exit");
                                loop_until_async_drop!(app_state);
                                return AppReturnState::SigintTerminalSignal;
                            }
                        };
                    }
                }).await;

                let app_return_state = {
                    match app_return_state_res {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::error!("Failed to initialise IBC: {e:?}");
                            continue;
                        }
                    }
                };

                app_return_state.log_state();
                if app_return_state.is_terminal_state() {
                    break 'outer;
                }
            }
            BrokerState::Unavailable => match scheduler.get_next_broker_available() {
                Ok(next_available) => {
                    let deadline = next_available.to_utc();
                    tokio::select! {
                        _ = sleep_until(&deadline) => {}
                        _ = sigterm.recv() => {
                            tracing::info!("SIGTERM received while broker unavailable, no state to report, exiting");
                            break 'outer;
                        }
                        _ = sigint.recv() => {
                            break 'outer;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error trying to get seconds to sleep for: {e:?}");
                    continue;
                }
            },
        }
    }

    tracing::info!("run_program exited cleanly after graceful shutdown");
}

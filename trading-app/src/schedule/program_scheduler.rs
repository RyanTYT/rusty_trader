use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::time::sleep;

use crate::{
    init_app::ApplicationState,
    logger::ConnectionAlert,
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
    loop {
        match scheduler.get_current_state() {
            BrokerState::Available => {
                let app_state = {
                    match init_application().await {
                        Ok(app_state_res) => Arc::new(app_state_res),
                        Err(e) => {
                            tracing::error!("Error trying to initialise application state: {e:?}");
                            continue;
                        }
                    }
                };
                if let Err(e) = sender.send(Arc::downgrade(&app_state)).await {
                    tracing::warn!("Failed to send app_state to server for use: {e:?}")
                }

                loop {
                    let next_unavailable = {
                        match scheduler.get_next_broker_unavailable() {
                            Ok(next_dt) => next_dt,
                            Err(e) => {
                                tracing::error!("Error trying to get seconds to sleep for: {e:?}");
                                continue;
                            }
                        }
                    };

                    let next_unavailable_utc = next_unavailable.to_utc();
                    tokio::select! {
                        // Window expires
                        _ = sleep_until(&next_unavailable_utc) => {
                            tracing::info!("IBKR system maintenance reached, tearing down now");
                            break;
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
                                        tracing::warn!("🚨 CRITICAL: Unstable connection during market hours and timeout occurred! Restarting now!");
                                        break;
                                    } else {
                                        tracing::warn!("🚨 Unstable connection during market hours but no timeout occurred so cautiously proceeding!");
                                        continue;
                                    }
                                }
                                // this shouldn't be reached actually
                                ConnectionAlert::UnstableConnectionOutsideMarketHours {
                                    first_event_time
                                }=> {
                                    tracing::warn!("🚨 Unstable connection outside market hours at {first_event_time:?}, restarting just in case");
                                    break;
                                }
                                ConnectionAlert::BrokenPipe {
                                    first_event_time: _
                                }=> {
                                    tracing::warn!("🚨 Unstable connection because of broken pipe: restarting now");
                                    break;
                                }
                                ConnectionAlert::APACRESET {
                                    first_event_time: _
                                }=> {
                                    tracing::warn!("🚨 Unstable connection due to APACRESET! Restarting now!");
                                    break;
                                }
                                ConnectionAlert::AutoRestarting => {
                                    tracing::warn!("IBKR is autorestarting! sleeping for a bit before continuing");
                                    sleep(Duration::from_secs(3 * 60)).await;
                                    continue;
                                }
                            }
                        }
                    };
                }

                drop(app_state);
            }
            BrokerState::Unavailable => match scheduler.get_next_broker_available() {
                Ok(next_available) => {
                    sleep_until(&next_available.to_utc()).await;
                }
                Err(e) => {
                    tracing::error!("Error trying to get seconds to sleep for: {e:?}");
                    continue;
                }
            },
        }
    }
}

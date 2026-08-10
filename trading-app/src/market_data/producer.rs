use std::{
    pin::Pin, sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    }, time::{Duration, Instant},
};

use chrono::Utc;
use ibapi::{
    Client,
    contracts::Contract,
    market_data::realtime::{Bar, WhatToShow},
};
use spmc_ring::{bench::RingBuffer, ring_buffer::spmc_ring_buffer::SpmcRingBuffer};

use crate::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};

const MAX_SUB_TRY_TIMES: usize = 50;

pub struct MarketDataProducer {
    is_alive: Arc<AtomicBool>,
}

impl Drop for MarketDataProducer {
    fn drop(&mut self) {
        self.is_alive.store(false, Ordering::Release);
    }
}

#[hotpath::measure]
pub fn subscribe_to_data<const BUFFER_SIZE: usize, const MAX_NO_OF_CONSUMERS: usize>(
    weak_client: Weak<Client>,
    contract: Contract,
    what_to_show: WhatToShow,
    contract_scheduler: Arc<IbkrContractScheduler>,
) -> (
    Pin<Box<SpmcRingBuffer<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>>>,
    MarketDataProducer,
) {
    let ring_buffer = Box::pin(SpmcRingBuffer::<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>::new());
    let producer = ring_buffer.get_new_producer().expect(
        "Expected to be able to get \
            producer for SPMC ring buffer",
    );
    let is_alive = Arc::new(AtomicBool::new(true));
    let cloned_is_alive = is_alive.clone();

    let symbol_key = format!("{}_{}", contract.symbol, contract.security_type);
    let metric_push_retries = format!("{symbol_key}_push_retries");
    let metric_is_trading = format!("{symbol_key}_is_trading");
    let metric_missed_bar = format!("{symbol_key}_missed_bars");
    let metric_sub_errors = format!("{symbol_key}_sub_errors");
    hotpath::gauge!(metric_push_retries.as_str()).set(0);
    hotpath::gauge!(metric_is_trading.as_str()).set(1.0);
    hotpath::gauge!(metric_missed_bar.as_str()).set(0);
    hotpath::gauge!(metric_sub_errors.as_str()).set(0);

    std::thread::Builder::new()
        .name(
            format!(
                "{}_{}_prod",
                contract.symbol, 
                contract.security_type
            )
        )
        .spawn(move || {
            let contracts = vec![contract.clone()];
            let mut last_sub = Instant::now() - Duration::from_secs(30);
            'sub_loop: loop {
                if !cloned_is_alive.load(Ordering::Acquire) {
                    break 'sub_loop;
                }
                // Mandate 20s Interval between subscription calls
                let time_since_last_sub = Instant::now().duration_since(last_sub);
                if time_since_last_sub < Duration::from_secs(20) {
                    std::thread::sleep(Duration::from_secs(20) - time_since_last_sub);
                }
                let subscription_res = hotpath::measure_block!("realtime_bars_subscribe", {
                    let client = weak_client.upgrade().expect("Expected client to be alive");
                    client.realtime_bars(
                        &contract,
                        ibapi::market_data::realtime::BarSize::Sec5,
                        what_to_show,
                        ibapi::market_data::TradingHours::Regular,
                    )
                });
                last_sub = Instant::now();

                // Subscription loop
                match subscription_res {
                    Ok(subscription) => {
                        'inner_loop: loop {
                            // If contract isn't trading yet, sleep until it is trading
                            if !contract_scheduler
                                .is_trading(&contract, &Utc::now())
                                    .expect(
                                    "Expected contract for producer sub to be in tracked contracts!",
                                )
                            {
                                hotpath::gauge!(metric_is_trading.as_str()).set(0.0);
                                let deadline = contract_scheduler
                                    .get_next_earliest_available_data(&contracts, &Utc::now())
                                    .expect(
                                        "Expected to be able to get_next_earliest_available_data, \
                                            maybe not enough data for tracked contract?",
                                    );
                                // Sleep until deadline at 20s intervals to check if still alive
                                loop {
                                    if !cloned_is_alive.load(Ordering::Acquire) {
                                        break 'sub_loop;
                                    }
                                    let secs_to_slp = (deadline - Utc::now()).num_seconds().min(20);
                                    if secs_to_slp > 0 {
                                        std::thread::sleep(Duration::from_secs(secs_to_slp as u64));
                                    }
                                }
                            }

                            hotpath::gauge!(metric_is_trading.as_str()).set(1.0);
                            match hotpath::measure_block!("bar_next_timeout_wait", {
                                subscription.next_timeout(Duration::from_secs(20))
                            }) {
                                Some(mut bar) => {
                                    // Basically try_push() MAX_SUB_TRY_TIMES, if not fail
                                    let mut try_times = 0;
                                    hotpath::measure_block!("try_push_bar_loop", {
                                        'try_push_loop: loop {
                                            tracing::error!("Bar: {bar:?}");
                                            match producer.try_push(bar) {
                                                Ok(()) => break 'try_push_loop,
                                                Err(returned_bar) => {
                                                    bar = returned_bar;
                                                    try_times += 1;
                                                    hotpath::gauge!(metric_push_retries.as_str()).inc(1);
                                                    if try_times == MAX_SUB_TRY_TIMES {
                                                        tracing::error!(
                                                            "Consumer either too slow \
                                                            or is stalled or something: \
                                                            Caused producer for ({}, {}) to miss",
                                                            contract.symbol,
                                                            contract.security_type
                                                        );
                                                        break 'try_push_loop;
                                                    }
                                                }
                                            }
                                        }
                                    });
                                }
                                None => {
                                    // Only Alert if contract is trading, else start of next 'inner_loop
                                    // will help check and sleep for contracts that are no longer
                                    // trading
                                    if contract_scheduler
                                        .is_trading(&contract, &Utc::now())
                                        .expect(
                                            "Expected contract for producer sub \
                                            to be in tracked contracts!",
                                        )
                                    {
                                        hotpath::gauge!(metric_missed_bar.as_str()).inc(1);
                                        if let Some(e) = subscription.error() {
                                            tracing::error!(
                                                "Subscription for ({}, {}) errored out ({e:?}): retrying...",
                                                contract.symbol,
                                                contract.security_type
                                            );
                                            hotpath::gauge!(metric_sub_errors.as_str()).inc(1);
                                            // go to outer loop to try to re-subscribe
                                            break 'inner_loop;
                                        }
                                        tracing::error!(
                                            "ALERT: Contract for ({}, {}) is currently \
                                            trading BUT missed subscription bar...",
                                            contract.symbol,
                                            contract.security_type
                                        );
                                    }
                                }
                            }

                            // Check before restarting 'inner_loop if still alive
                            if !cloned_is_alive.load(Ordering::Acquire) {
                                break 'sub_loop;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Realtime request for ({}, {}) failed ({e:?})\n: retrying again",
                            contract.symbol,
                            contract.security_type
                        );
                    }
                }
            }
        }).expect("Expected producer thread to be able to spawn");
    return (ring_buffer, MarketDataProducer { is_alive });
}

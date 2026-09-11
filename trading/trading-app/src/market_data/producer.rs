use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use chrono::Utc;
use ibapi::{
    Client,
    client::Subscription,
    contracts::Contract,
    market_data::realtime::{Bar, WhatToShow},
};
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferProducer;

use crate::{
    market_data::consumer::helper::next_boundary,
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
};

const BAR_INTERVAL: Duration = Duration::from_secs(5);
const HOT_WINDOW: Duration = Duration::from_millis(500);
const SPIN_BACKOFF: Duration = Duration::ZERO;

const MAX_SUB_TRY_TIMES: usize = 50;

pub struct IbkrBarProducer<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize> {
    pub contract: Contract,
    pub what_to_show: WhatToShow,
    producer: SpmcRingBufferProducer<Bar, BUFFER_CAPACITY, NUM_CONSUMERS>,
}

impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize>
    IbkrBarProducer<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    pub fn new(
        contract: Contract,
        what_to_show: WhatToShow,
        producer: SpmcRingBufferProducer<Bar, BUFFER_CAPACITY, NUM_CONSUMERS>,
    ) -> Self {
        Self {
            contract,
            what_to_show,
            producer,
        }
    }
}

pub struct MarketDataProducer {
    is_alive: Arc<AtomicBool>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl MarketDataProducer {
    pub async fn async_drop(&mut self) {
        self.is_alive.store(false, Ordering::Release);
        if let Some(handle) = self.thread_handle.take() {
            let drop_thread_handle = tokio::task::spawn_blocking(move || {
                if let Err(e) = handle.join() {
                    tracing::error!("Failed to end tear down producer thread properly: {e:?}");
                }
            })
            .await;
            if let Err(e) = drop_thread_handle {
                tracing::error!("Failed to drop producer thread properly: {e:?}");
            }
        }
    }
}

impl Drop for MarketDataProducer {
    fn drop(&mut self) {
        self.is_alive.store(false, Ordering::Release);
    }
}

pub fn begin_producer_thread_grouped<const BUFFER_SIZE: usize, const MAX_NO_OF_CONSUMERS: usize>(
    weak_client_borrowed: &Weak<Client>,
    contract_scheduler: Arc<IbkrContractScheduler>,
    producers: Vec<IbkrBarProducer<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>>,
) -> MarketDataProducer {
    let weak_client = weak_client_borrowed.clone();
    let is_alive = Arc::new(AtomicBool::new(true));
    let cloned_is_alive = is_alive.clone();
    let thread_handle = std::thread::Builder::new()
        .name("grp_prod".to_string())
        .spawn(move || {
            let mut subscriptions: Vec<(
                Subscription<Bar>,
                IbkrBarProducer<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>,
            )> = {
                let client = weak_client
                    .upgrade()
                    .expect("Expected client to still be alive when subscribing to data");
                producers
                    .into_iter()
                    .map(|producer| {
                        (
                            client
                                .realtime_bars(
                                    &producer.contract,
                                    ibapi::market_data::realtime::BarSize::Sec5,
                                    producer.what_to_show,
                                    ibapi::market_data::TradingHours::Regular,
                                )
                                .expect(
                                    "Expected to be able to make subscription for realtime_bars",
                                ),
                            producer,
                        )
                    })
                    .collect()
            };
            let mut consecutive_misses: Vec<u32> = subscriptions.iter().map(|_| 0).collect();
            let mut next_deadline = hotpath::measure_block!("align_and_prime_schedule", {
                align_and_prime_schedule_producers(&contract_scheduler, &subscriptions)
            });

            while cloned_is_alive.load(Ordering::Acquire) {
                let active_producers: Vec<usize> = subscriptions
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, (_, spmc_producer))| {
                        if contract_scheduler
                            .is_trading(&spmc_producer.contract)
                            .expect("Expected consumer contract to be in scheduler")
                        {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut received = vec![false; active_producers.len()];

                sleep_until_system_time(next_deadline - HOT_WINDOW);
                let spin_deadline = Instant::now() + HOT_WINDOW * 2; // one window either side of the boundary
                hotpath::measure_block!("producer_grouped_spin_loop", {
                    loop {
                        let mut all_done = true;

                        for (received_idx, ref_idx) in active_producers.iter().enumerate() {
                            if received[received_idx] {
                                continue;
                            }

                            let idx = *ref_idx;
                            let (ibapi_producer, spmc_producer) = subscriptions.get(idx).unwrap();
                            let mut did_pop = false;
                            loop {
                                match ibapi_producer.try_next() {
                                    Some(mut bar) => {
                                        did_pop = true;
                                        let mut is_pushed = false;
                                        for _ in 0..10 {
                                            match spmc_producer.producer.try_push(bar) {
                                                Ok(_) => {
                                                    is_pushed = true;
                                                    break;
                                                }
                                                Err(bar_returned) => {
                                                    bar = bar_returned;
                                                }
                                            }
                                        }
                                        if !is_pushed {
                                            tracing::error!(
                                                "Failed to push bar for {} into Ring Buffer",
                                                spmc_producer.contract.symbol
                                            )
                                        }
                                    }
                                    None => {
                                        if did_pop {
                                            received[received_idx] = true;
                                        } else {
                                            all_done = false
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        if all_done || Instant::now() >= spin_deadline {
                            break;
                        }

                        if SPIN_BACKOFF.is_zero() {
                            std::hint::spin_loop();
                        } else {
                            std::thread::sleep(SPIN_BACKOFF);
                        }
                    }
                });

                // Anything still marked not-received missed its window
                // this cycle — surface that instead of silently dropping it.
                for (idx, received_bool) in received.iter().enumerate() {
                    if *received_bool {
                        consecutive_misses[active_producers[idx]] = 0;
                    } else {
                        let num_misses = consecutive_misses[active_producers[idx]];
                        consecutive_misses[active_producers[idx]] += 1;
                        tracing::warn!(
                            "Failed to receive bar for {} from IBKR for {}-th time",
                            subscriptions[active_producers[idx]].1.contract.symbol,
                            num_misses + 1
                        );
                        if let Some(e) = subscriptions[active_producers[idx]].0.error() {
                            tracing::error!(
                                "Failed to receive bar for {} from IBKR because of error: {e:?}",
                                subscriptions[active_producers[idx]].1.contract.symbol,
                            );
                        }
                        // if miss a minute worth of bars, re-subscribe
                        if num_misses + 1 > 12 {
                            tracing::warn!(
                                "Re-subscribing to bar for {} from IBKR!",
                                subscriptions[active_producers[idx]].1.contract.symbol
                            );
                            let (_, spmc_producer) = &subscriptions[active_producers[idx]];
                            subscriptions[active_producers[idx]].0 = {
                                let client = weak_client.upgrade().expect(
                                    "Expected client to still be alive when subscribing to data",
                                );
                                client
                                .realtime_bars(
                                    &spmc_producer.contract,
                                    ibapi::market_data::realtime::BarSize::Sec5,
                                    spmc_producer.what_to_show,
                                    ibapi::market_data::TradingHours::Regular,
                                )
                                .expect(
                                    "Expected to be able to make subscription for realtime_bars",
                                )
                            };
                            consecutive_misses[active_producers[idx]] = 0;
                        }
                    }
                }

                next_deadline += BAR_INTERVAL;
            }
        })
        .expect("Expected to be able to spawn IBKR producer thread");

    MarketDataProducer {
        is_alive,
        thread_handle: Some(thread_handle),
    }
}

// #[hotpath::measure]
// pub fn subscribe_to_data<const BUFFER_SIZE: usize, const MAX_NO_OF_CONSUMERS: usize>(
//     weak_client: Weak<Client>,
//     contract: Contract,
//     what_to_show: WhatToShow,
//     contract_scheduler: Arc<IbkrContractScheduler>,
// ) -> (
//     Arc<SpmcRingBuffer<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>>,
//     MarketDataProducer,
// ) {
//     let ring_buffer = Arc::new(SpmcRingBuffer::<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>::new());
//     let producer = ring_buffer.get_new_producer().expect(
//         "Expected to be able to get \
//             producer for SPMC ring buffer",
//     );
//     let is_alive = Arc::new(AtomicBool::new(true));
//     let cloned_is_alive = is_alive.clone();
//
//     let symbol_key = format!("{}_{}", contract.symbol, contract.security_type);
//     let metric_push_retries = format!("{symbol_key}_push_retries");
//     let metric_is_trading = format!("{symbol_key}_is_trading");
//     let metric_missed_bar = format!("{symbol_key}_missed_bars");
//     let metric_sub_errors = format!("{symbol_key}_sub_errors");
//     hotpath::gauge!(metric_push_retries.as_str()).set(0);
//     hotpath::gauge!(metric_is_trading.as_str()).set(1.0);
//     hotpath::gauge!(metric_missed_bar.as_str()).set(0);
//     hotpath::gauge!(metric_sub_errors.as_str()).set(0);
//
//     let thread_handle = std::thread::Builder::new()
//         .name(
//             format!(
//                 "{}_{}_prod",
//                 contract.symbol,
//                 contract.security_type
//             )
//         )
//         .spawn(move || {
//             let contracts = vec![contract.clone()];
//             let mut last_sub = Instant::now() - Duration::from_secs(30);
//             'sub_loop: loop {
//                 if !cloned_is_alive.load(Ordering::Acquire) {
//                     break 'sub_loop;
//                 }
//                 // Mandate 20s Interval between subscription calls
//                 let time_since_last_sub = Instant::now().duration_since(last_sub);
//                 if time_since_last_sub < Duration::from_secs(20) {
//                     std::thread::sleep(Duration::from_secs(20) - time_since_last_sub);
//                 }
//                 let subscription_res = hotpath::measure_block!("realtime_bars_subscribe", {
//                     let client = weak_client.upgrade().expect("Expected client to be alive");
//                     client.realtime_bars(
//                         &contract,
//                         ibapi::market_data::realtime::BarSize::Sec5,
//                         what_to_show,
//                         ibapi::market_data::TradingHours::Regular,
//                     )
//                 });
//                 last_sub = Instant::now();
//
//                 // Subscription loop
//                 match subscription_res {
//                     Ok(subscription) => {
//                         'inner_loop: loop {
//                             // If contract isn't trading yet, sleep until it is trading
//                             if !contract_scheduler
//                                 .is_trading(&contract, &Utc::now())
//                                     .expect(
//                                     "Expected contract for producer sub to be in tracked contracts!",
//                                 )
//                             {
//                                 hotpath::gauge!(metric_is_trading.as_str()).set(0.0);
//                                 let deadline = contract_scheduler
//                                     .get_next_earliest_available_data(&contracts, &Utc::now())
//                                     .expect(
//                                         "Expected to be able to get_next_earliest_available_data, \
//                                             maybe not enough data for tracked contract?",
//                                     );
//                                 // Sleep until deadline at 20s intervals to check if still alive
//                                 loop {
//                                     if !cloned_is_alive.load(Ordering::Acquire) {
//                                         break 'sub_loop;
//                                     }
//                                     let secs_to_slp = (deadline - Utc::now()).num_seconds().min(20);
//                                     if secs_to_slp > 0 {
//                                         std::thread::sleep(Duration::from_secs(secs_to_slp as u64));
//                                     }
//                                 }
//                             }
//
//                             hotpath::gauge!(metric_is_trading.as_str()).set(1.0);
//                             match hotpath::measure_block!("bar_next_timeout_wait", {
//                                 subscription.next_timeout(Duration::from_secs(20))
//                             }) {
//                                 Some(mut bar) => {
//                                     // Basically try_push() MAX_SUB_TRY_TIMES, if not fail
//                                     let mut try_times = 0;
//                                     hotpath::measure_block!("try_push_bar_loop", {
//                                         'try_push_loop: loop {
//                                             match producer.try_push(bar) {
//                                                 Ok(()) => break 'try_push_loop,
//                                                 Err(returned_bar) => {
//                                                     bar = returned_bar;
//                                                     try_times += 1;
//                                                     hotpath::gauge!(metric_push_retries.as_str()).inc(1);
//                                                     if try_times == MAX_SUB_TRY_TIMES {
//                                                         tracing::error!(
//                                                             "Consumer either too slow \
//                                                             or is stalled or something: \
//                                                             Caused producer for ({}, {}) to miss",
//                                                             contract.symbol,
//                                                             contract.security_type
//                                                         );
//                                                         break 'try_push_loop;
//                                                     }
//                                                 }
//                                             }
//                                         }
//                                     });
//                                 }
//                                 None => {
//                                     // Only Alert if contract is trading, else start of next 'inner_loop
//                                     // will help check and sleep for contracts that are no longer
//                                     // trading
//                                     if contract_scheduler
//                                         .is_trading(&contract, &Utc::now())
//                                         .expect(
//                                             "Expected contract for producer sub \
//                                             to be in tracked contracts!",
//                                         )
//                                     {
//                                         hotpath::gauge!(metric_missed_bar.as_str()).inc(1);
//                                         if let Some(e) = subscription.error() {
//                                             tracing::error!(
//                                                 "Subscription for ({}, {}) errored out ({e:?}): retrying...",
//                                                 contract.symbol,
//                                                 contract.security_type
//                                             );
//                   hotpath::gauge!(metric_sub_errors.as_str()).inc(1);
//                                             // go to outer loop to try to re-subscribe
//                                             break 'inner_loop;
//                                         }
//                                         tracing::error!(
//                                             "ALERT: Contract for ({}, {}) is currently \
//                                             trading BUT missed subscription bar...",
//                                             contract.symbol,
//                                             contract.security_type
//                                         );
//                                     }
//                                 }
//                             }
//
//                             // Check before restarting 'inner_loop if still alive
//                             if !cloned_is_alive.load(Ordering::Acquire) {
//                                 break 'sub_loop;
//                             }
//                         }
//                     }
//                     Err(e) => {
//                         tracing::error!(
//                             "Realtime request for ({}, {}) failed ({e:?})\n: retrying again",
//                             contract.symbol,
//                             contract.security_type
//                         );
//                     }
//                 }
//             }
//         }).expect("Expected producer thread to be able to spawn");
//     return (
//         ring_buffer,
//         MarketDataProducer {
//             is_alive,
//             thread_handle: Some(thread_handle),
//         },
//     );
// }

/// Runs once at thread startup, before the steady-state loop. For every
/// currently-trading consumer, drains whatever backlog is sitting in its
/// ring buffer (dispatching each bar — nothing gets silently discarded),
/// and records the timestamp of the last bar it saw. The returned deadline
/// is derived from the latest observed `bar.time` rather than guessed from
/// local `SystemTime`, so the steady-state schedule is anchored to what the
/// producers are actually doing, not what we assume they're doing.
///
/// Consumers that aren't trading yet at startup are skipped here; they'll
/// simply be picked up by the active-set snapshot once they start trading,
/// on whatever schedule the group has already settled into.
pub fn align_and_prime_schedule_producers<
    const BUFFER_CAPACITY: usize,
    const NUM_CONSUMERS: usize,
>(
    contract_scheduler: &IbkrContractScheduler,
    producers: &[(
        Subscription<Bar>,
        IbkrBarProducer<BUFFER_CAPACITY, NUM_CONSUMERS>,
    )],
) -> SystemTime {
    let n = producers.len();
    let mut observed = vec![None; n];
    let mut settled = vec![false; n];
    // Generous ceiling — covers a consumer with a couple of cycles' worth
    // of backlog to drain. If nothing settles by then something's wrong
    // upstream and we fall back rather than blocking startup forever.
    let deadline = Instant::now() + BAR_INTERVAL * 4;

    loop {
        let mut progressed = false;

        for (i, (ib_producer, spmc_producer)) in producers.iter().enumerate() {
            if settled[i] {
                continue;
            }
            if !contract_scheduler
                .is_trading(&spmc_producer.contract)
                .expect("Expected schedule to be populated")
            {
                settled[i] = true;
                continue;
            }
            match ib_producer.try_next() {
                Some(mut bar) => {
                    observed[i] = Some(bar.date);
                    let mut is_pushed = false;
                    for _ in 0..10 {
                        match spmc_producer.producer.try_push(bar) {
                            Ok(_) => {
                                is_pushed = true;
                                break;
                            }
                            Err(new_bar) => {
                                bar = new_bar;
                            }
                        };
                    }
                    if !is_pushed {
                        tracing::error!(
                            "Failed to push initial bars into SPMC Buffer for {}",
                            spmc_producer.contract.symbol
                        );
                    }
                    progressed = true;
                }
                None => {
                    if observed[i].is_some() {
                        settled[i] = true;
                    }
                }
            }
        }

        if settled.iter().all(|s| *s) || Instant::now() >= deadline {
            break;
        }
        if !progressed {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    let now = Utc::now();
    // Get latest observed bar time
    // - then for all consumers slower than that -> wait for pop bar
    //   (for up to 250ms)
    match observed.iter().flatten().max().copied() {
        Some(latest) => {
            for (i, (ib_producer, spmc_producer)) in producers.iter().enumerate() {
                if !contract_scheduler
                    .is_trading(&spmc_producer.contract)
                    .expect("Expected schedule to be populated")
                {
                    continue;
                }

                let mut max_wait = 50;
                while observed[i] != Some(latest) {
                    match ib_producer.try_next() {
                        Some(mut bar) => {
                            observed[i] = Some(bar.date);
                            let mut is_pushed = false;
                            for _ in 0..10 {
                                match spmc_producer.producer.try_push(bar) {
                                    Ok(_) => {
                                        is_pushed = true;
                                        break;
                                    }
                                    Err(new_bar) => {
                                        bar = new_bar;
                                    }
                                };
                            }
                            if !is_pushed {
                                tracing::error!(
                                    "Failed to push initial bars into SPMC Buffer for {}",
                                    spmc_producer.contract.symbol
                                );
                            }
                        }
                        None => {
                            std::thread::sleep(Duration::from_millis(5));
                            max_wait -= 1;
                        }
                    }
                    if max_wait == 0 {
                        break;
                    };
                }

                if observed[i] != Some(latest) {
                    tracing::error!(
                        "Bar for {} is not aligned and is very delayed: skipping for now - but will cause misalignment issues",
                        spmc_producer.contract.symbol
                    );
                }
            }
            SystemTime::from(latest) + BAR_INTERVAL
        }
        // Nothing was trading yet at startup — nothing to anchor to
        // fall back to a wall-clock boundary
        None => next_boundary(SystemTime::now(), BAR_INTERVAL),
    }
}

/// Sleeps until `target`, or returns immediately (logging lateness) if
/// `target` has already passed — e.g. because a previous cycle's hot
/// window overran.
fn sleep_until_system_time(target: SystemTime) {
    match target.duration_since(SystemTime::now()) {
        Ok(duration) => std::thread::sleep(duration),
        Err(_) => {
            tracing::warn!(
                "Missed target - producers are running behind time - too slow (not catching up to 5s intervals)"
            );
        }
    }
}

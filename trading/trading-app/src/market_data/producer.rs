//! Live bar producers: one OS thread drives a batch of IBKR realtime-bar
//! subscriptions into their SPMC rings.
//!
//! # Scheduling model
//!
//! The producer group is deliberately bar-clock driven rather than
//! event driven:
//!
//! 1. at startup it drains any backlog and anchors its deadline to the newest
//!    observed bar time ([`align_and_prime_schedule_producers`]);
//! 2. every `BAR_INTERVAL` (5 s) it sleeps until the deadline, then spins for
//!    up to `HOT_WINDOW` (500 ms) calling `try_next` on each trading
//!    subscription and pushing each bar into its ring;
//! 3. a subscription that produced no bar in a cycle counts as a miss; after
//!    12 consecutive misses it is cancelled and re-subscribed;
//! 4. when the contract scheduler says a contract is between sessions, the
//!    subscription is re-created at the next available boundary.
//!
//! # Backpressure and failure signals
//!
//! A full ring is *not* silently dropped: the producer retries the push up to
//! 10 times, incrementing `bar_push_retries`; if all retries fail the bar is
//! logged as lost. Misses and subscription errors are exposed as per-symbol
//! `bar_missed_<symbol>` / `bar_sub_errors_<symbol>` gauges, and arrival lag as
//! `bar_arrival_lag_seconds`.
//!
//! # Panics
//!
//! The producer thread treats an unusable client or scheduler as fatal: a dead
//! `Weak<Client>`, a failed `realtime_bars` subscription, or a scheduler lookup
//! error panics the thread. There is no in-thread recovery; the owning
//! [`crate::market_data::handler::MarketDataHandler`] must be rebuilt.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant, SystemTime},
};

use chrono::{DateTime, TimeDelta, Utc};
use ibapi::{
    client::Subscription,
    contracts::{Contract, SecurityType},
    market_data::realtime::{Bar, WhatToShow},
    Client,
};
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferProducer;

use crate::{
    market_data::consumer::helper::next_boundary,
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
};

/// Cadence of the producer loop; IBKR realtime bars are 5-second bars.
const BAR_INTERVAL: Duration = Duration::from_secs(5);
/// How long each cycle spins waiting for that cycle's bar before declaring a miss.
const HOT_WINDOW: Duration = Duration::from_millis(500);
/// Spin hint between empty polls inside the hot window (zero = pure spin).
const SPIN_BACKOFF: Duration = Duration::ZERO;

/// One IBKR subscription paired with the ring producer it feeds.
pub struct IbkrBarProducer<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize> {
    /// Contract being streamed.
    pub contract: Contract,
    /// Tick type requested.
    pub what_to_show: WhatToShow,
    /// Handle used to push bars into the shared ring.
    producer: SpmcRingBufferProducer<Bar, BUFFER_CAPACITY, NUM_CONSUMERS>,
}

impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize>
    IbkrBarProducer<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    /// Wraps an existing ring producer for `contract`.
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

/// Lifetime handle for one producer thread group.
pub struct MarketDataProducer {
    /// Cleared on drop/shutdown; the thread checks it each cycle.
    is_alive: Arc<AtomicBool>,
    /// Taken by [`MarketDataProducer::async_drop`]; `None` afterwards.
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl MarketDataProducer {
    /// Signals the group to stop and joins its thread.
    ///
    /// The join runs on a blocking-pool thread so the async runtime is not
    /// stalled; a panicked producer thread is logged, not propagated.
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
    /// Signals shutdown only; the thread is detached. Use
    /// [`MarketDataProducer::async_drop`] to join it.
    fn drop(&mut self) {
        self.is_alive.store(false, Ordering::Release);
    }
}

/// Spawns the producer thread for a batch of subscriptions.
///
/// Every producer in `producers` is subscribed with 5-second realtime bars and
/// `Extended` trading hours, except `ForexPair` which uses `Regular` (FX has no
/// extended session). The returned handle owns the thread; dropping it signals
/// shutdown but does not join.
///
/// # Panics
///
/// Panics while spawning the thread, when the client is already dead, or when a
/// `realtime_bars` subscription fails. Panics inside the spawned closure abort
/// the producer group (logged by [`MarketDataProducer::async_drop`]).
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
                            hotpath::measure_block!("ibkr_realtime_bars_subscribe", {
                                client
                                    .realtime_bars(
                                        &producer.contract,
                                        ibapi::market_data::realtime::BarSize::Sec5,
                                        producer.what_to_show,
                                        if producer.contract.security_type == SecurityType::ForexPair {
                                            ibapi::market_data::TradingHours::Regular
                                        } else {
                                            ibapi::market_data::TradingHours::Extended
                                        },
                                    )
                                    .expect(
                                        "Expected to be able to make subscription for realtime_bars",
                                    )
                            }),
                            producer,
                        )
                    })
                    .collect()
            };

            // Per-contract gauges — pass format!() directly to hotpath::gauge!; it
            // takes impl Into<Arc<str>> and aggregates by content, so no Box::leak
            // / &'static str cache is needed.
            #[allow(unused_variables)]
            for (_, p) in subscriptions.iter() {
                hotpath::gauge!(format!("bar_missed_{}", p.contract.symbol)).set(0.0);
                hotpath::gauge!(format!("bar_sub_errors_{}", p.contract.symbol)).set(0.0);
            }
            // Aggregate gauge: ring-full events = consumer can't keep up = the
            // leading overload indicator. (Replaces hotpath::channel! queue-size,
            // which can't wrap the custom spmc-ring — only 6 specific crates.)
            hotpath::gauge!("bar_push_retries").set(0.0);

            let mut next_available_times: Vec<DateTime<Utc>> = match subscriptions
                .iter()
                .map(
                    |(_ib_producer, spmc_producer)| -> Result<DateTime<Utc>, String> {
                        let next_time = contract_scheduler
                            .get_next_earliest_available_data(std::iter::once(&spmc_producer.contract))?;
                        Ok(next_time - TimeDelta::seconds(30))
                    },
                )
                .collect()
            {
                Ok(v) => v,
                Err(e) => {
                    panic!("Failed to get next earliest available data for some contract: {e:?}");
                }
            };
            let mut consecutive_misses: Vec<u32> = subscriptions.iter().map(|_| 0).collect();
            let mut next_deadline = hotpath::measure_block!("align_and_prime_schedule", {
                align_and_prime_schedule_producers(&contract_scheduler, &subscriptions)
            });

            while cloned_is_alive.load(Ordering::Acquire) {
                let now = Utc::now();
                for idx in 0..next_available_times.len() {
                    if now >= next_available_times[idx] {
                        // Assumes no contract schedule has a downtime shorter than 30s
                        next_available_times[idx] = match contract_scheduler
                            .get_next_earliest_available_data(std::iter::once(
                                &subscriptions[idx].1.contract,
                            )) {
                            Ok(v) => {
                                let next_time = v - TimeDelta::seconds(30);
                                if now >= next_time {
                                    resubscribe_bar(&mut subscriptions, &weak_client, idx);
                        
                                    match contract_scheduler.get_next_latest_unavailable_data(
                                        std::iter::once(&subscriptions[idx].1.contract)
                                    ) {
                                        Ok(v) => v + TimeDelta::seconds(5),
                                        Err(e) => panic!("Couldn't fetch contract data for get_next_latest_unavailable_data: {e:?}")
                                    }
                                } else {
                                    next_time
                                }
                            },
                            Err(e) => panic!(
                                "Couldn't fetch contract data for get_next_earliest_available_data: {e:?}"
                            ),
                        };
                    }
                    
                }
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

                sleep_until_system_time(next_deadline);
                let spin_deadline = Instant::now() + HOT_WINDOW; // one window either side of the boundary
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
                                match hotpath::measure_block!("ibkr_bar_next_wait", {
                                    ibapi_producer.try_next()
                                }) {
                                    Some(mut bar) => {
                                        did_pop = true;
                                        // Staleness signal — how late is this bar relative to the
                                        // expected boundary? Positive = late = about to trade on
                                        // stale data. hotpath_gauge_max{key=...} gives the peak lag
                                        // per scrape window (the alertable staleness signal).
                                        let lag = SystemTime::now()
                                            .duration_since(next_deadline)
                                            .map(|d| d.as_secs_f64())
                                            .unwrap_or_else(|e| -(e.duration().as_secs_f64()));
                                        hotpath::gauge!("bar_arrival_lag_seconds").set(lag);
                                        let mut is_pushed = false;
                                        for _ in 0..10 {
                                            match spmc_producer.producer.try_push(bar) {
                                                Ok(_) => {
                                                    is_pushed = true;
                                                    break;
                                                }
                                                Err(bar_returned) => {
                                                    bar = bar_returned;
                                                    // Ring full — consumer too slow. Each retry
                                                    // is a backlog event (the overload signal
                                                    // hotpath_channel_queue_size would have given).
                                                    hotpath::gauge!("bar_push_retries").inc(1.0);
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
                    let sub_idx = active_producers[idx];
                    if *received_bool {
                        consecutive_misses[sub_idx] = 0;
                    } else {
                        let num_misses = consecutive_misses[sub_idx];
                        consecutive_misses[sub_idx] += 1;
                        hotpath::gauge!(format!(
                            "bar_missed_{}",
                            subscriptions[sub_idx].1.contract.symbol
                        ))
                        .inc(1.0);
                        tracing::warn!(
                            "Failed to receive bar for {} from IBKR for {}-th time",
                            subscriptions[sub_idx].1.contract.symbol,
                            num_misses + 1
                        );
                        if let Some(e) = subscriptions[sub_idx].0.error() {
                            hotpath::gauge!(format!(
                                "bar_sub_errors_{}",
                                subscriptions[sub_idx].1.contract.symbol
                            ))
                            .inc(1.0);
                            tracing::error!(
                                "Failed to receive bar for {} from IBKR because of error: {e:?}",
                                subscriptions[sub_idx].1.contract.symbol,
                            );
                        }
                        // if miss a minute worth of bars, re-subscribe
                        if num_misses + 1 > 12 {
                            tracing::warn!(
                                "Re-subscribing to bar for {} from IBKR!",
                                subscriptions[sub_idx].1.contract.symbol
                            );
                            resubscribe_bar(&mut subscriptions, &weak_client, sub_idx);
                            
                            consecutive_misses[sub_idx] = 0;
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

/// Cancels and re-creates the subscription at `idx`, preserving bar size,
/// tick type and trading-hours choice.
///
/// # Panics
///
/// Panics when the client is dead or the replacement subscription fails; the
/// caller treats a broken subscription as fatal for the group.
fn resubscribe_bar<
    const BUFFER_CAPACITY: usize,
    const NUM_CONSUMERS: usize,
>(
    subscriptions: &mut [(
        Subscription<Bar>,
        IbkrBarProducer<BUFFER_CAPACITY, NUM_CONSUMERS>,
    )],
    weak_client: &Weak<Client>,
    idx: usize
) { 
    subscriptions[idx].0.cancel();
    subscriptions[idx].0 = {
        let client = weak_client
            .upgrade()
            .expect("Expected client to still be alive when subscribing to data");
        hotpath::measure_block!("ibkr_realtime_bars_subscribe", {
            client
                .realtime_bars(
                    &subscriptions[idx].1.contract,
                    ibapi::market_data::realtime::BarSize::Sec5,
                    subscriptions[idx].1.what_to_show,
                    if subscriptions[idx].1.contract.security_type
                        == SecurityType::ForexPair
                    {
                        ibapi::market_data::TradingHours::Regular
                    } else {
                        ibapi::market_data::TradingHours::Extended
                    },
                )
                .expect(
                    "Expected to be able to make subscription for realtime_bars",
                )
        })
    };
}

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
///
/// Returns the first steady-state deadline: one `BAR_INTERVAL` after the newest
/// observed bar, or the next wall-clock 5-second boundary when nothing was
/// trading at startup. The function gives up after 20 s and returns a deadline
/// based on whatever was observed, so startup cannot hang forever.
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

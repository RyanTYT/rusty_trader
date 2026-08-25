use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
use std::{collections::VecDeque, sync::Arc};

use chrono::Utc;
use ibapi::Client;
use ibapi::market_data::realtime::Bar;
use ibapi::{contracts::Contract, market_data::realtime::WhatToShow};
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferConsumer;

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::execution::fx_backed_up_order::OrderStore;
use crate::execution::order_engine::OrderEngine;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::consumer::helper::{aggregate_bars, align_and_prime_schedule};
use crate::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};
use crate::strategy::strategy::{BarUpdateOutcome, StrategyExecutor};
use crate::{database::models::AssetType, strategy::strategy::StrategyEnum};

const BAR_INTERVAL: Duration = Duration::from_secs(5);
/// How long to spin-poll around each expected bar arrival before giving up
/// on stragglers for this cycle. IBKR jitter is usually well under 100ms,
/// but leave headroom.
const HOT_WINDOW: Duration = Duration::from_millis(200);
/// Yield the CPU briefly between spin iterations instead of a bare spin_loop
/// hint, if you want to trade a little latency for a lot less CPU burn.
/// Set to Duration::ZERO for a true hot spin.
const SPIN_BACKOFF: Duration = Duration::ZERO;

pub struct IbkrBarConsumer<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize> {
    pub contract: Contract,
    pub what_to_show: WhatToShow,
    consumer: SpmcRingBufferConsumer<Bar, BUFFER_CAPACITY, NUM_CONSUMERS>,
}

impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize>
    IbkrBarConsumer<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    pub fn new(
        contract: Contract,
        what_to_show: WhatToShow,
        consumer: SpmcRingBufferConsumer<Bar, BUFFER_CAPACITY, NUM_CONSUMERS>,
    ) -> Self {
        Self {
            contract,
            what_to_show,
            consumer,
        }
    }
}

#[derive(Debug, Clone)]
pub enum IbkrBarType {
    Normal,
    ForexBid,
    ForexAsk,
}

impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize>
    IbkrBarConsumer<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    // pub fn is_trading(&self) -> bool {
    //     // self.contract
    //     true
    // }
    pub fn try_pop(&self) -> Option<Bar> {
        self.consumer.try_pop()
    }
    pub fn get_bar_type(&self) -> IbkrBarType {
        match self.contract.security_type {
            ibapi::contracts::SecurityType::ForexPair => match self.what_to_show {
                WhatToShow::Bid => IbkrBarType::ForexBid,
                WhatToShow::Ask => IbkrBarType::ForexAsk,
                _ => panic!("ForexPair Security subscribing to data that is not bid or ask"),
            },
            _ => IbkrBarType::Normal,
        }
    }
}

pub struct StrategyDataBundler<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize> {
    contract_scheduler: Arc<IbkrContractScheduler>,
    is_alive: Arc<AtomicBool>,
    thread_handles: Vec<std::thread::JoinHandle<()>>,
}

impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize>
    StrategyDataBundler<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    pub async fn async_drop(&mut self) {
        self.is_alive.store(false, Ordering::Release);

        let thread_handles = std::mem::take(&mut self.thread_handles);
        let drop_threads_handle = tokio::task::spawn_blocking(move || {
            for thread_handle in thread_handles.into_iter() {
                if let Err(e) = thread_handle.join() {
                    tracing::error!("Failed to end strategy thread handle: {e:?}");
                }
            }
        })
        .await;
        if let Err(e) = drop_threads_handle {
            tracing::error!("Failed to drop strategy threads properly: {e:?}");
        }
    }
}

impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize> Drop
    for StrategyDataBundler<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    fn drop(&mut self) {
        // best effort drop but doesn't guarantee threads are dropped
        self.is_alive.store(false, Ordering::Release);
    }
}

#[hotpath::measure_all]
impl<const BUFFER_CAPACITY: usize, const NUM_CONSUMERS: usize>
    StrategyDataBundler<BUFFER_CAPACITY, NUM_CONSUMERS>
{
    pub fn new(contract_scheduler: Arc<IbkrContractScheduler>) -> Self {
        Self {
            contract_scheduler,
            is_alive: Arc::new(AtomicBool::new(false)),
            thread_handles: vec![],
        }
    }

    /// pub visibility ONLY for testing purposes
    pub fn sort_consumers(consumers: &mut Vec<IbkrBarConsumer<BUFFER_CAPACITY, NUM_CONSUMERS>>) {
        consumers.sort_by(|a, b| {
            let is_fx_a = AssetType::from_str(&a.contract.security_type) == AssetType::ForexPair;
            let is_fx_b = AssetType::from_str(&b.contract.security_type) == AssetType::ForexPair;

            // Tuple ordering handles the multi-tier sort:
            // 1. Forex pairs come FIRST (is_fx == true > is_fx == false)
            // 2. Symbol alphabetical order
            // 3. Custom 'what_to_show' tie-breaking for Forex
            (
                is_fx_b,
                &a.contract.symbol.to_string(),
                match &a.what_to_show {
                    WhatToShow::Bid => 0,
                    WhatToShow::Ask => 1,
                    _ => 2,
                },
            )
                .cmp(&(
                    is_fx_a,
                    &b.contract.symbol.to_string(),
                    match &b.what_to_show {
                        WhatToShow::Bid => 0,
                        WhatToShow::Ask => 1,
                        _ => 2,
                    },
                ))
        });
    }

    // Will only ever spawn thread once
    pub fn hook_strategy(
        &mut self,
        mut consumers: Vec<IbkrBarConsumer<BUFFER_CAPACITY, NUM_CONSUMERS>>,
        mut strategy: StrategyEnum,

        order_engine: OrderEngine,
        consolidator: Weak<Consolidator>,
        client: Weak<Client>,
        order_store: Weak<OrderStore>,
    ) {
        if self.is_alive.swap(true, Ordering::AcqRel) {
            // Already started.
            return;
        }
        let is_alive = Arc::clone(&self.is_alive);

        Self::sort_consumers(&mut consumers);
        let contract_scheduler = self.contract_scheduler.clone();
        #[cfg(not(feature = "backtest"))]
        let thread_handle = std::thread::Builder::new()
            .name(format!("{}_strat", strategy.get_name()))
            .spawn(move || {
                let strategy_detail = strategy.get_strategy_details();
                let strategy_name = strategy.get_name();
                let mut strategy_on_bar_update = |contract: &Contract, bar: HistoricalDataFullKeys| {
                    strategy.on_bar_update(
                        contract,
                        &bar,
                        &consolidator
                            .upgrade()
                            .expect("Expected Consolidator to still be alive on_bar_update"),
                    )
                };
                let handle_bar_update_outcome = |bar_update_outcome: BarUpdateOutcome| {
                    let order_store_arc = 
                        match order_store
                            .upgrade() {
                                Some(order_store_some) => order_store_some,
                                None => {
                                    tracing::warn!("Tried to access OrderStore when it is dropped - app may be tearing down");
                                    return;
                                }
                            };
                    order_engine.handle_bar_update_outcome(
                        &client,
                        &consolidator,
                        bar_update_outcome,
                        &strategy_detail,
                        &order_store_arc
                    );
                };
                let mut next_deadline = hotpath::measure_block!("align_and_prime_schedule", {
                    align_and_prime_schedule(&contract_scheduler, &consumers)
                });
                let mut small_bars: Vec<VecDeque<Bar>> = vec![VecDeque::new(); consumers.len()];
                let mut agg_bars: Vec<Option<HistoricalDataFullKeys>> = vec![None; consumers.len()];
                // let gauge_name = format!("{}_strat_bar_rcv_spin_loop", strategy.get_name());
                let gauge_name: &'static str = Box::leak(
                    format!("{}_strat_bar_rcv_spin_loop", strategy_name).into_boxed_str()
                );

                while is_alive.load(Ordering::Acquire) {
                    let now = Utc::now();
                    // Do all pre-work for next loop b4 slping
                    let active: Vec<usize> = hotpath::measure_block!("compute_active_contracts", {
                        (0..consumers.len())
                            .filter(|&i| {
                                contract_scheduler
                                    .is_trading(&consumers[i].contract, &now)
                                    .expect("Expected schedule to be populated")
                            })
                            .collect()
                    });
                    let mut received = vec![false; active.len()];

                    hotpath::measure_block!("sleep_until_deadline", {
                        sleep_until_system_time(next_deadline - HOT_WINDOW, &strategy_name);
                    });

                    let spin_deadline = Instant::now() + HOT_WINDOW * 2; // one window either side of the boundary
                    hotpath::measure_block!(gauge_name, {
                        loop {
                            let mut all_done = true;

                            for (slot, &idx) in active.iter().enumerate() {
                                if received[slot] {
                                    continue;
                                }
                                match consumers[idx].try_pop() {
                                    Some(bar) => {
                                        received[slot] = true;
                                        small_bars[idx].push_back(bar);
                                        match consumers[idx].get_bar_type() {
                                            IbkrBarType::Normal => {
                                                if let Err(e) = Self::dispatch_bar(
                                                    &strategy_name,
                                                    &consumers[idx].contract,
                                                    &consumers[idx].what_to_show,
                                                    &mut small_bars[idx],
                                                    None,
                                                    &mut strategy_on_bar_update,
                                                    handle_bar_update_outcome,
                                                ) {
                                                    tracing::error!(
                                                        "Failed to dispatch_bar: {e:?}"
                                                    );
                                                }
                                            }
                                            IbkrBarType::ForexBid => {
                                                if received[slot + 1] {
                                                    // build bar
                                                    if let Err(e) = Self::dispatch_bar(
                                                        &strategy_name,
                                                        &consumers[idx].contract,
                                                        &consumers[idx].what_to_show,
                                                        &mut small_bars[idx],
                                                        agg_bars.get_mut(idx + 1).unwrap().take(),
                                                        &mut strategy_on_bar_update,
                                                        handle_bar_update_outcome,
                                                    ) {
                                                        tracing::error!(
                                                            "Failed to dispatch_bar: {e:?}"
                                                        );
                                                    }
                                                } else {
                                                    let mut big_bars = aggregate_bars(
                                                        &consumers[idx].contract,
                                                        &consumers[idx].what_to_show,
                                                        &mut small_bars[idx],
                                                        60,
                                                    );
                                                    if big_bars.is_empty() {
                                                        continue;
                                                    }
                                                    if big_bars.len() > 1 {
                                                        tracing::error!(
                                                            "aggregate_bars output more than 1 bar"
                                                        );
                                                    }
                                                    agg_bars[slot] = Some(big_bars.pop().unwrap());
                                                }
                                            }
                                            IbkrBarType::ForexAsk => {
                                                if received[slot - 1] {
                                                    // build bar
                                                    if let Err(e) = Self::dispatch_bar(
                                                        &strategy_name,
                                                        &consumers[idx].contract,
                                                        &consumers[idx].what_to_show,
                                                        &mut small_bars[idx],
                                                        agg_bars.get_mut(idx - 1).unwrap().take(),
                                                        &mut strategy_on_bar_update,
                                                        handle_bar_update_outcome,
                                                    ) {
                                                        tracing::error!(
                                                            "Failed to dispatch_bar: {e:?}"
                                                        );
                                                    }
                                                } else {
                                                    let mut big_bars = aggregate_bars(
                                                        &consumers[idx].contract,
                                                        &consumers[idx].what_to_show,
                                                        &mut small_bars[idx],
                                                        60,
                                                    );
                                                    if big_bars.is_empty() {
                                                        continue;
                                                    }
                                                    if big_bars.len() > 1 {
                                                        tracing::error!(
                                                            "aggregate_bars output more than 1 bar"
                                                        );
                                                    }
                                                    agg_bars[slot] = Some(big_bars.pop().unwrap());
                                                }
                                            }
                                        }
                                    }
                                    None => all_done = false,
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
                    let errs = active
                        .iter()
                        .enumerate()
                        .filter_map(|(slot, &idx)| {
                            if !received[slot] {
                                Some(format!(
                                    "Failed to receive bar for {}",
                                    consumers[idx].contract.symbol
                                ))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<String>>()
                        .join("\n");
                    if errs.len() > 0 {
                        tracing::warn!("{}", errs);
                    }
                    // for (slot, &idx) in active.iter().enumerate() {
                    //     if !received[slot] {
                    //         tracing::warn!(
                    //             "Failed to receive bar for {}",
                    //             consumers[idx].contract.symbol
                    //         );
                    //     }
                    // }

                    next_deadline += BAR_INTERVAL;
                }
            })
            .expect("Expected Strategy Thread to be able to be spawned");

        #[cfg(not(feature = "backtest"))]
        self.thread_handles.push(thread_handle);
    }

    fn dispatch_bar<OnBarUpdate, HandleBarUpdate>(
        strategy: &str,
        contract: &Contract,
        what_to_show: &WhatToShow,
        small_bars: &mut VecDeque<Bar>,
        other_fx_bar: Option<HistoricalDataFullKeys>,
        mut strategy_on_bar_update: OnBarUpdate,
        handle_bar_update_outcome: HandleBarUpdate,
    ) -> Result<(), String>
    where
        OnBarUpdate: FnMut(&Contract, HistoricalDataFullKeys) -> Result<BarUpdateOutcome, String>,
        HandleBarUpdate: Fn(BarUpdateOutcome),
    {
        match AssetType::from_str(&contract.security_type) {
            AssetType::ForexPair => {
                let mut big_bars = aggregate_bars(contract, what_to_show, small_bars, 60);
                if big_bars.is_empty() {
                    return Ok(());
                }
                if big_bars.len() > 1 {
                    tracing::error!("Aggregating Forex bars output more than 1 bar!");
                }

                let (bid_bar, ask_bar) = match what_to_show {
                    WhatToShow::Bid => {
                        let bid_bar = big_bars.pop().unwrap();
                        let ask_bar = other_fx_bar.expect("Expected Valid data for other fx bar");
                        (bid_bar, ask_bar)
                    }
                    WhatToShow::Ask => {
                        let ask_bar = big_bars.pop().unwrap();
                        let bid_bar = other_fx_bar.expect("Expected Valid data for other fx bar");
                        (bid_bar, ask_bar)
                    }
                    _ => panic!("Tried getting non-bid/ask data for ForexPair"),
                };

                let full_bar =
                    HistoricalDataFullKeys::from_inter_repr(&contract, &bid_bar, &ask_bar);

                let bar_update_name: &'static str = Box::leak(
                    format!("{}_strat_bar_update", strategy).into_boxed_str()
                );
                hotpath::measure_block!(bar_update_name, {
                    let bar_update_outcome = strategy_on_bar_update(&contract, full_bar)?;
                    handle_bar_update_outcome(bar_update_outcome);
                });
            }

            _ => {
                let big_bars = aggregate_bars(contract, what_to_show, small_bars, 300);
                for bar in big_bars {
                    let bar_update_outcome = strategy_on_bar_update(&contract, bar)?;
                    handle_bar_update_outcome(bar_update_outcome);
                }
            }
        }
        Ok(())
    }
}

/// Sleeps until `target`, or returns immediately (logging lateness) if
/// `target` has already passed — e.g. because a previous cycle's hot
/// window overran.
fn sleep_until_system_time(target: SystemTime, strategy: &str) {
    match target.duration_since(SystemTime::now()) {
        Ok(duration) => std::thread::sleep(duration),
        Err(_) => {
            tracing::warn!(
                "Missed target - strategy {strategy} is running behind time - too slow (not catching up to 5s intervals)"
            );
        }
    }
}

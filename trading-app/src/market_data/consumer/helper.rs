use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::{TimeZone, Utc};
use ibapi::{
    contracts::Contract,
    market_data::realtime::{Bar, WhatToShow},
};

use crate::{
    database::models_crud::historical_data::historical_data::HistoricalDataFullKeys,
    market_data::consumer::strategy_consumer::IbkrBarConsumer,
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
};

const BAR_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a new OS thread to process the 5 second bars from the subscription
/// - is called by the channel instead of directly since calling directly would be on the
/// separate OS kernel thread which doesn't have a tokio runtime
/// - Note: multithreading should be fine because each bar for each contract is separated by 5
/// sec times which should be sufficient time for this whole check to complete
/// - highest_granularity_timestep_in_seconds: granularity of bars being stored in the table
///     - i.e. for stocks, 5 min / 300 sec
///     - i.e. for forex, 1 min / 60 sec
/// - MINOR_BUG: if data starts being loaded within a bar, start of bar data is ignored/lost
pub fn aggregate_bars(
    contract: &Contract,
    what_to_show: &WhatToShow,
    collected_bars: &mut VecDeque<Bar>,
    bar_time_width_u32: u32,
) -> Vec<HistoricalDataFullKeys> {
    if collected_bars.is_empty() {
        return vec![];
    }

    let bar_time_width = bar_time_width_u32 as i64;

    let latest_bar_timestamp = collected_bars.back().unwrap().date.unix_timestamp();
    let next_bar_timestamp = latest_bar_timestamp + 5;

    let latest_bar_no = latest_bar_timestamp - (latest_bar_timestamp % bar_time_width);
    let next_bar_no = next_bar_timestamp - (next_bar_timestamp % bar_time_width);
    if latest_bar_no == next_bar_no {
        return vec![];
    }

    let mut agg_bars = Vec::new();
    while !collected_bars.is_empty() {
        // Process first bar first
        let first_bar = collected_bars.pop_front().unwrap();
        let bar_time = first_bar.date.unix_timestamp();
        let prev_bar_time = bar_time - 5;
        let bar_no = bar_time - (bar_time % bar_time_width);
        tracing::warn!("Bar Time: {bar_time:?}, bar_no: {bar_no:?}");
        let prev_bar_no = prev_bar_time - (prev_bar_time % bar_time_width);
        let has_first_bar = prev_bar_no != bar_no;

        let (open, mut high, mut low, mut close, mut volume) = (
            first_bar.open,
            first_bar.high,
            first_bar.low,
            first_bar.close,
            first_bar.volume,
        );

        loop {
            if collected_bars.is_empty() {
                break;
            }
            let this_bar_date = &collected_bars.front().unwrap().date.unix_timestamp();
            let this_bar_no = this_bar_date - (this_bar_date % bar_time_width);
            if bar_no != this_bar_no {
                break;
            }

            let first_bar = &collected_bars.pop_front().unwrap();
            high = f64::max(high, first_bar.high);
            low = f64::min(low, first_bar.low);
            close = first_bar.close;
            volume += first_bar.volume;
        }
        tracing::info!("Has first bar: {has_first_bar:?}");

        agg_bars.push(HistoricalDataFullKeys::from_data(
            &contract,
            &what_to_show,
            Utc.timestamp_opt(bar_no, 0).unwrap(),
            open,
            high,
            low,
            close,
            volume,
        ))
    }

    agg_bars
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
pub fn align_and_prime_schedule<const BUFFER_CAPACITY: usize>(
    contract_scheduler: &IbkrContractScheduler,
    consumers: &[IbkrBarConsumer<BUFFER_CAPACITY>],
) -> SystemTime {
    let n = consumers.len();
    let mut observed = vec![None; n];
    let mut settled = vec![false; n];
    // Generous ceiling — covers a consumer with a couple of cycles' worth
    // of backlog to drain. If nothing settles by then something's wrong
    // upstream and we fall back rather than blocking startup forever.
    let deadline = Instant::now() + BAR_INTERVAL * 4;

    loop {
        let mut progressed = false;

        let now = Utc::now();
        for (i, consumer) in consumers.iter().enumerate() {
            if settled[i] {
                continue;
            }
            if !contract_scheduler
                .is_trading(&consumer.contract, &now)
                .expect("Expected schedule to be populated")
            {
                settled[i] = true;
                continue;
            }
            match consumer.try_pop() {
                Some(bar) => {
                    observed[i] = Some(bar.date);
                    // dispatch_bar(
                    //     idx,
                    //     &agg_bars,
                    //     &consumers[idx].contract,
                    //     &consumers[idx].what_to_show,
                    //     &mut small_bars,
                    // );
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
            for (i, consumer) in consumers.iter().enumerate() {
                if !contract_scheduler
                    .is_trading(&consumer.contract, &now)
                    .expect("Expected schedule to be populated")
                {
                    continue;
                }
                let mut max_wait = 50;
                while observed[i] != Some(latest) {
                    match consumer.try_pop() {
                        Some(bar) => {
                            observed[i] = Some(bar.date);
                            // dispatch_bar(
                            //     idx,
                            //     &agg_bars,
                            //     &consumers[idx].contract,
                            //     &consumers[idx].what_to_show,
                            //     &mut small_bars,
                            // );
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
                        consumer.contract.symbol
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

/// Rounds `from` up to the next multiple of `interval` since the Unix
/// epoch. Used as a fallback when no consumer was trading yet at startup
/// to anchor `align_and_prime_schedule` against.
fn next_boundary(from: SystemTime, interval: Duration) -> SystemTime {
    let since_epoch = from.duration_since(UNIX_EPOCH).unwrap();
    let interval_nanos = interval.as_nanos();
    let elapsed_nanos = since_epoch.as_nanos();
    let remainder = elapsed_nanos % interval_nanos;
    let to_add = if remainder == 0 {
        0
    } else {
        interval_nanos - remainder
    };
    from + Duration::from_nanos(to_add as u64)
}

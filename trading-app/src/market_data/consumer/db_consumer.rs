use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
use std::{collections::VecDeque, sync::Arc};

use chrono::{DateTime, Utc};
use ibapi::market_data::realtime::Bar;
use moka::sync::Cache;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys,
};
use crate::market_data::consumer::helper::{aggregate_bars, align_and_prime_schedule};
use crate::market_data::consumer::strategy_consumer::{IbkrBarConsumer, IbkrBarType};
use crate::schedule::contract_scheduler::IbkrContractScheduler;

const BAR_INTERVAL: Duration = Duration::from_secs(5);
/// How long to spin-poll around each expected bar arrival before giving up
/// on stragglers for this cycle. IBKR jitter is usually well under 100ms,
/// but leave headroom.
const HOT_WINDOW: Duration = Duration::from_millis(200);
/// Yield the CPU briefly between spin iterations instead of a bare spin_loop
/// hint, if you want to trade a little latency for a lot less CPU burn.
/// Set to Duration::ZERO for a true hot spin.
const SPIN_BACKOFF: Duration = Duration::ZERO;

// struct MarketDataDbConsumer<const BUFFER_CAPACITY: usize> {
//     consumer: SpmcRingBufferConsumer<Bar, BUFFER_CAPACITY>,
// }
pub struct MarketDataDbConsumer {
    is_alive: Arc<AtomicBool>,
}

impl Drop for MarketDataDbConsumer {
    fn drop(&mut self) {
        self.is_alive.store(false, Ordering::Release);
    }
}

#[hotpath::measure]
pub fn begin_db_consumer_thread_singular<const BUFFER_CAPACITY: usize>(
    pool: PgPool,
    contract_scheduler: Arc<IbkrContractScheduler>,
    consumer: IbkrBarConsumer<BUFFER_CAPACITY>,
    cache: Cache<i32, (DateTime<Utc>, f64)>,
    rt_handle: tokio::runtime::Handle,
) -> MarketDataDbConsumer {
    let is_alive = Arc::new(AtomicBool::new(false));
    let is_alive_cloned = is_alive.clone();
    let contract_scheduler = contract_scheduler.clone();
    std::thread::Builder::new()
        .name(format!(
            "({}, {}) Consumer Thread",
            consumer.contract.symbol, consumer.contract.security_type
        ))
        .spawn(move || {
            let mut small_bars: VecDeque<Bar> = VecDeque::new();
            // can be optimised but is initialisation stage: i.e. not really hot path to optimise
            let mut dummy_consumers = vec![consumer];
            let mut next_deadline = hotpath::measure_block!("align_and_prime_schedule_singular", {
                align_and_prime_schedule(&contract_scheduler, &dummy_consumers)
            });
            let consumer = dummy_consumers.pop().unwrap();
            let contract_id = consumer.contract.contract_id;

            while is_alive.load(Ordering::Acquire) {
                sleep_until_system_time(next_deadline - HOT_WINDOW);
                let spin_deadline = Instant::now() + HOT_WINDOW * 2; // one window either side of the boundary
                hotpath::measure_block!("db_consumer_singular_spin_loop", {
                    loop {
                        match consumer.try_pop() {
                            Some(bar) => {
                                small_bars.push_back(bar);
                                let big_bars = aggregate_bars(
                                    &consumer.contract,
                                    &consumer.what_to_show,
                                    &mut small_bars,
                                    match consumer.get_bar_type() {
                                        IbkrBarType::Normal => 60,
                                        _ => 300,
                                    },
                                );

                                for bar in big_bars {
                                    let asset_type =
                                        AssetType::from_str(&consumer.contract.security_type);
                                    let historical_data_pk =
                                        HistoricalDataPrimaryKeys::from_contract(
                                            &consumer.contract,
                                            bar.get_time(),
                                        );
                                    let historical_data_uk = HistoricalDataUpdateKeys::from_bar(
                                        &consumer.contract,
                                        &consumer.what_to_show,
                                        &bar,
                                    );
                                    let historical_data_crud =
                                        HistoricalDataCRUD::from(&asset_type, pool.clone());
                                    cache.insert(contract_id, (bar.get_time(), bar.get_price()));
                                    rt_handle.spawn(hotpath::future!(
                                        async move {
                                            if let Err(e) = historical_data_crud
                                                .create_or_update(
                                                    &historical_data_pk,
                                                    &historical_data_uk,
                                                )
                                                .await
                                            {
                                                tracing::error!(
                                                    "Failed to update Historical Data: {e:?}"
                                                );
                                            }
                                        },
                                        label = "historical_data_create_or_update_singular"
                                    ));
                                }
                                break;
                            }
                            None => {
                                if Instant::now() >= spin_deadline {
                                    tracing::warn!(
                                        "Failed to receive bar for {} in db consumer",
                                        consumer.contract.symbol
                                    );
                                    break;
                                }
                                if SPIN_BACKOFF.is_zero() {
                                    std::hint::spin_loop();
                                } else {
                                    std::thread::sleep(SPIN_BACKOFF);
                                }
                            }
                        }
                    }
                });

                next_deadline += BAR_INTERVAL;
            }
        })
        .expect("Expected DB Consumer thread to be able to be spawned");
    MarketDataDbConsumer {
        is_alive: is_alive_cloned,
    }
}

#[hotpath::measure]
pub fn begin_db_consumer_thread_grouped<const BUFFER_CAPACITY: usize>(
    pool: PgPool,
    contract_scheduler: Arc<IbkrContractScheduler>,
    consumers: Vec<IbkrBarConsumer<BUFFER_CAPACITY>>,
    cache: Cache<i32, (DateTime<Utc>, f64)>,
    rt_handle: tokio::runtime::Handle,
) -> MarketDataDbConsumer {
    let is_alive = Arc::new(AtomicBool::new(false));
    let is_alive_cloned = is_alive.clone();
    let contract_scheduler = contract_scheduler.clone();
    std::thread::Builder::new()
        .name(format!(
            "({}, {}) Consumer Thread",
            consumers
                .first()
                .expect("Expected consumers argument to not be empty")
                .contract
                .symbol,
            consumers
                .first()
                .expect("Expected consumers argument to not be empty")
                .contract
                .security_type,
        ))
        .spawn(move || {
            let mut next_deadline = hotpath::measure_block!("align_and_prime_schedule_grouped", {
                align_and_prime_schedule(&contract_scheduler, &consumers)
            });
            let mut small_bars: Vec<VecDeque<Bar>> = vec![VecDeque::new(); consumers.len()];
            let contract_ids: Vec<i32> = consumers.iter().map(|c| c.contract.contract_id).collect();

            while is_alive.load(Ordering::Acquire) {
                // Do all pre-work for next loop b4 slping
                let mut received = vec![false; consumers.len()];

                sleep_until_system_time(next_deadline - HOT_WINDOW);

                let spin_deadline = Instant::now() + HOT_WINDOW * 2; // one window either side of the boundary
                hotpath::measure_block!("db_consumer_grouped_spin_loop", {
                    loop {
                        let mut all_done = true;

                        for (idx, consumer) in consumers.iter().enumerate() {
                            if received[idx] {
                                continue;
                            }
                            match consumer.try_pop() {
                                Some(bar) => {
                                    received[idx] = true;
                                    small_bars[idx].push_back(bar);
                                    let big_bars = aggregate_bars(
                                        &consumers[idx].contract,
                                        &consumers[idx].what_to_show,
                                        &mut small_bars[idx],
                                        match consumer.get_bar_type() {
                                            IbkrBarType::Normal => 60,
                                            _ => 300,
                                        },
                                    );

                                    for bar in big_bars {
                                        let asset_type =
                                            AssetType::from_str(&consumer.contract.security_type);
                                        let historical_data_pk =
                                            HistoricalDataPrimaryKeys::from_contract(
                                                &consumer.contract,
                                                bar.get_time(),
                                            );
                                        let historical_data_uk = HistoricalDataUpdateKeys::from_bar(
                                            &consumer.contract,
                                            &consumer.what_to_show,
                                            &bar,
                                        );
                                        let historical_data_crud =
                                            HistoricalDataCRUD::from(&asset_type, pool.clone());
                                        let contract_id = contract_ids[idx];
                                        cache
                                            .insert(contract_id, (bar.get_time(), bar.get_price()));
                                        rt_handle.spawn(hotpath::future!(
                                            async move {
                                                if let Err(e) = historical_data_crud
                                                    .create_or_update(
                                                        &historical_data_pk,
                                                        &historical_data_uk,
                                                    )
                                                    .await
                                                {
                                                    tracing::error!(
                                                        "Failed to update Historical Data: {e:?}"
                                                    );
                                                }
                                            },
                                            label = "historical_data_create_or_update_grouped"
                                        ));
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
                for (idx, received_idx) in received.iter().enumerate() {
                    if !received_idx {
                        tracing::warn!(
                            "Failed to receive bar for {} in db consumer",
                            consumers[idx].contract.symbol
                        );
                    }
                }

                next_deadline += BAR_INTERVAL;
            }
        })
        .expect("Expected to be able to spawn DB consumer thread");
    MarketDataDbConsumer {
        is_alive: is_alive_cloned,
    }
}

#[hotpath::measure]
fn sleep_until_system_time(target: SystemTime) {
    match target.duration_since(SystemTime::now()) {
        Ok(duration) => std::thread::sleep(duration),
        Err(_) => {
            tracing::warn!(
                "Missed target - DB ops are running behind time - too slow (not catching up to 5s intervals)"
            );
        }
    }
}

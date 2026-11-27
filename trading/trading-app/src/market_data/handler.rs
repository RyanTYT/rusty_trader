//! Subscription registry and thread-lifetime owner for live market data.
//!
//! [`MarketDataHandler`] is the object that turns a list of
//! [`DataSubscription`]s into running IBKR streams:
//!
//! 1. each new subscription gets an `SpmcRingBuffer<Bar, 128, 10>` plus one
//!    producer and one strategy consumer;
//! 2. all new producers are driven by one producer thread group;
//! 3. bars are persisted by DB consumers, either one thread per subscription or
//!    one grouped thread per [`MarketDataHandler::load_all_subscription_producers`](crate::market_data::handler::MarketDataHandler::load_all_subscription_producers) call;
//! 4. the latest price per IBKR request id is cached in `live_prices` for 60 s.
//!
//! # Invariants and caveats
//!
//! - Existing [`DataSubscription`]s are never re-subscribed; the ring and its
//!   threads are reused.
//! - A ring's producer/consumer slots are bounded; exhausting them panics while
//!   loading (see [`MarketDataHandler::load_all_subscription_producers`](crate::market_data::handler::MarketDataHandler::load_all_subscription_producers)).
//! - Dropping the handler does **not** stop its threads or cancel IBKR
//!   subscriptions; call [`MarketDataHandler::async_drop`](crate::market_data::handler::MarketDataHandler::async_drop).
//! - Prices in `live_prices` are keyed by IBKR request id and expire after
//!   60 s, so a stale id reads as `None` rather than an old price.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Weak},
    time::Duration,
};

use chrono::{DateTime, Utc};
use ibapi::{
    Client,
    contracts::{Contract, SecurityType},
    market_data::realtime::{Bar, WhatToShow},
};
use moka::sync::Cache;
use ordered_float::OrderedFloat;
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;
use sqlx::PgPool;

use crate::{
    market_data::{
        consumer::{
            db_consumer::{
                MarketDataDbConsumer, begin_db_consumer_thread_grouped,
                begin_db_consumer_thread_singular,
            },
            strategy_consumer::IbkrBarConsumer,
        },
        producer::{IbkrBarProducer, MarketDataProducer, begin_producer_thread_grouped},
    },
    schedule::contract_scheduler::IbkrContractScheduler,
};

const BUFFER_SIZE: usize = 128;
const MAX_NO_OF_CONSUMERS: usize = 10;

/// Identity of one live market-data stream: a contract plus a `what_to_show`.
///
/// Equality is intentionally narrower than [`Contract`]'s: two subscriptions
/// are the same stream when they agree on
///
/// - `primary_exchange` (trimmed), `symbol`, `currency`, `security_type`;
/// - for options additionally `right`, `last_trade_date_or_contract_month`,
///   `strike` (compared as [`OrderedFloat`]) and `multiplier`;
/// - the normalized `what_to_show` (bid/ask/trades/midpoint).
///
/// Fields such as `contract_id`, `local_symbol` and `exchange` are ignored, so
/// the same instrument discovered through different routes shares one ring.
/// Hashing uses exactly the same fields as [`PartialEq`].
#[derive(Debug, Clone)]
pub struct DataSubscription {
    /// Instrument to stream.
    pub contract: Contract,
    /// Tick type requested from IBKR.
    pub what_to_show: WhatToShow,
}

impl DataSubscription {
    /// Pairs a contract with the tick type to stream.
    pub fn new(contract: Contract, what_to_show: WhatToShow) -> Self {
        Self {
            contract,
            what_to_show,
        }
    }
}

impl Hash for DataSubscription {
    /// Hashes exactly the fields used by [`DataSubscription`]'s `PartialEq`.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.contract.primary_exchange.as_str().trim().hash(state);
        self.contract.symbol.as_str().hash(state);
        self.contract.currency.as_str().hash(state);
        self.contract.security_type.to_string().hash(state);

        if self.contract.security_type == SecurityType::Option {
            self.contract.right.hash(state);
            self.contract.last_trade_date_or_contract_month.hash(state);
            ordered_float::OrderedFloat(self.contract.strike).hash(state);
            self.contract.multiplier.hash(state);
        }

        let name = match self.what_to_show {
            WhatToShow::Bid => "bid",
            WhatToShow::Ask => "ask",
            WhatToShow::Trades => "trades",
            WhatToShow::MidPoint => "midpoint",
        };
        name.hash(state);
    }
}

impl PartialEq for DataSubscription {
    /// Compares the stream-identity fields documented on the type.
    fn eq(&self, other: &Self) -> bool {
        match (self.what_to_show, other.what_to_show) {
            (WhatToShow::Bid, WhatToShow::Bid)
            | (WhatToShow::Ask, WhatToShow::Ask)
            | (WhatToShow::Trades, WhatToShow::Trades)
            | (WhatToShow::MidPoint, WhatToShow::MidPoint) => {}
            _ => {
                return false;
            }
        }

        if self.contract.primary_exchange.as_str().trim()
            != other.contract.primary_exchange.as_str().trim()
            || self.contract.symbol.as_str() != other.contract.symbol.as_str()
            || self.contract.currency.as_str() != other.contract.currency.as_str()
            || self.contract.security_type != other.contract.security_type
        {
            return false;
        }

        if self.contract.security_type == SecurityType::Option {
            if self.contract.right != other.contract.right
                || self.contract.last_trade_date_or_contract_month
                    != other.contract.last_trade_date_or_contract_month
                || OrderedFloat(self.contract.strike) != OrderedFloat(other.contract.strike)
                || self.contract.multiplier != other.contract.multiplier
            {
                return false;
            }
        }

        true
    }
}

impl Eq for DataSubscription {}

/// Owns every producer and consumer created for a set of subscriptions.
///
/// Each field is a lifetime handle: dropping the handler without
/// [`MarketDataHandler::async_drop`](crate::market_data::handler::MarketDataHandler::async_drop) leaks running threads.
pub struct MarketDataHandler {
    /// DB writer threads created so far.
    db_consumers: Vec<MarketDataDbConsumer>,
    /// IBKR producer thread groups created so far.
    client_producers: Vec<MarketDataProducer>,
    /// Latest price per IBKR request id, 60 s TTL.
    live_prices: Cache<i32, (DateTime<Utc>, f64)>,
    /// One ring per distinct subscription; the ring outlives both its producer
    /// and its consumers.
    subscriptions:
        HashMap<DataSubscription, Arc<SpmcRingBuffer<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>>>,
    /// Pool used by DB consumers for bar persistence.
    pool: PgPool,
}

impl MarketDataHandler {
    /// Shuts down all producers, then all DB consumers.
    ///
    /// Producers go first so no new bars are pushed while consumers drain.
    /// Calling it twice is harmless: both vectors are emptied.
    pub async fn async_drop(&mut self) {
        let mut client_producers = std::mem::take(&mut self.client_producers);
        futures::future::join_all(
            client_producers
                .iter_mut()
                .map(|producer| producer.async_drop()),
        )
        .await;
        let mut db_consumers = std::mem::take(&mut self.db_consumers);
        futures::future::join_all(
            db_consumers
                .iter_mut()
                .map(|consumer| consumer.async_drop()),
        )
        .await;
    }
}

/// How DB consumers are threaded when subscriptions are loaded.
pub enum DbSubscriptionMethod {
    /// One DB consumer thread per new subscription. Easiest to debug in logs,
    /// at the cost of one thread per stream.
    OnePerThread,
    /// One DB consumer thread for all subscriptions added by the current
    /// `load_all_subscription_producers` call. Fewer threads; a slow writer
    /// stalls every stream in that group.
    GroupedPerThread,
}

impl MarketDataHandler {
    /// Creates an empty handler with an empty 60 s price cache.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            subscriptions: HashMap::new(),
            live_prices: Cache::builder()
                .time_to_live(Duration::from_secs(60))
                .build(),
            db_consumers: vec![],
            client_producers: vec![],
        }
    }

    /// This updates the subscriptions handled
    /// - if subscription already exists, nothing is done,
    /// - if subscription doesn't exst,
    ///     - initiates a subscription via client.realtime_bars()
    ///     - updates the associated RingBuffer into the subscriptions HashMap
    /// - if DbSubscriptionMethod::OnePerThread
    ///     - assigns a single thread per subscription for DB updating
    ///       (much easier for debugging; for early testing)
    /// - if DbSubscriptionMethod::GroupedPerThread
    ///     - assigns a single thread every time this method is called,
    ///       whereby all "new" subscriptions are handled in that one thread
    ///
    /// The `client` is held weakly: if the IBKR client is already gone, the
    /// producer group logs and exits instead of keeping a dead stream alive.
    ///
    /// # Panics
    ///
    /// Panics when a ring has no producer or consumer slot left, i.e. more than
    /// one producer or more than `MAX_NO_OF_CONSUMERS` consumers for one
    /// subscription. That indicates subscriptions are being loaded twice for
    /// the same key, which the `contains_key` guard is meant to prevent.
    pub fn load_all_subscription_producers(
        &mut self,
        client: &Weak<Client>,
        contract_scheduler: Arc<IbkrContractScheduler>,
        subscriptions: Vec<DataSubscription>,
        subscription_method: DbSubscriptionMethod,
        rt_handle: tokio::runtime::Handle,
    ) {
        // let ring_buffers = HashMap::new();
        // for subscription in subscriptions {
        //     ring_buffers
        // }
        let mut new_consumers = vec![];
        let mut new_producers = vec![];
        for subscription in subscriptions.into_iter() {
            if !self.subscriptions.contains_key(&subscription) {
                let ring_buffer =
                    Arc::new(SpmcRingBuffer::<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>::new());
                new_producers.push(IbkrBarProducer::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>::new(
                    subscription.contract.clone(),
                    subscription.what_to_show.clone(),
                    ring_buffer.get_new_producer().expect("Expected to be able to get_new_consumer() -> i.e. no. of producers exceeded!"),
                ));
                new_consumers.push(IbkrBarConsumer::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>::new(
                    subscription.contract.clone(),
                    subscription.what_to_show.clone(),
                    ring_buffer.get_new_consumer().expect("Expected to be able to get_new_consumer() -> i.e. no. of consumers exceeded!"),
                ));
                self.subscriptions.insert(subscription, ring_buffer);
            }
        }

        let producer_heartbeat =
            begin_producer_thread_grouped(client, contract_scheduler.clone(), new_producers);
        self.client_producers.push(producer_heartbeat);

        match subscription_method {
            DbSubscriptionMethod::OnePerThread => {
                for consumer in new_consumers {
                    self.db_consumers.push(begin_db_consumer_thread_singular(
                        self.pool.clone(),
                        contract_scheduler.clone(),
                        consumer,
                        self.live_prices.clone(),
                        rt_handle.clone(),
                    ));
                }
            }
            DbSubscriptionMethod::GroupedPerThread => {
                self.db_consumers.push(begin_db_consumer_thread_grouped(
                    self.pool.clone(),
                    contract_scheduler.clone(),
                    new_consumers,
                    self.live_prices.clone(),
                    rt_handle.clone(),
                ));
            }
        }
    }

    /// Latest cached price for an IBKR request id, or `None` if the id is
    /// unknown or the entry is older than 60 s.
    pub fn try_get_price(&self, key: i32) -> Option<f64> {
        self.live_prices.get(&key).map(|v| v.1)
    }

    /// Returns the ring for a subscription, or `None` if it was never loaded.
    ///
    /// The name keeps its historical typo (`subsription`) to avoid an API
    /// break; it is not a behavioural detail.
    pub fn get_subsription(
        &self,
        sub: &DataSubscription,
    ) -> Option<&Arc<SpmcRingBuffer<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>>> {
        self.subscriptions.get(&sub)
    }
}

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Weak},
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
            db_consumer::{begin_db_consumer_thread_grouped, begin_db_consumer_thread_singular},
            strategy_consumer::IbkrBarConsumer,
        },
        producer::subscribe_to_data,
    },
    schedule::contract_scheduler::IbkrContractScheduler,
};

const BUFFER_SIZE: usize = 128;
const MAX_NO_OF_CONSUMERS: usize = 10;

#[derive(Debug, Clone)]
struct DataSubscription {
    contract: Contract,
    what_to_show: WhatToShow,
}

impl Hash for DataSubscription {
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

pub struct MarketDataHandler {
    pool: PgPool,
    subscriptions: HashMap<DataSubscription, SpmcRingBuffer<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>>,
    live_prices: Cache<i32, (DateTime<Utc>, f64)>,
}

pub enum DbSubscriptionMethod {
    OnePerThread,
    GroupedPerThread,
}

impl MarketDataHandler {
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
    pub fn load_all_subscription_producers(
        &mut self,
        client: &Weak<Client>,
        contract_scheduler: Arc<IbkrContractScheduler>,
        subscriptions: Vec<DataSubscription>,
        subscription_method: DbSubscriptionMethod,
    ) {
        let mut new_consumers = vec![];
        for subscription in subscriptions.into_iter() {
            if !self.subscriptions.contains_key(&subscription) {
                let ring_buffer = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
                    client.clone(),
                    subscription.contract.clone(),
                    subscription.what_to_show.clone(),
                );
                new_consumers.push(IbkrBarConsumer::<BUFFER_SIZE>::new(
                    subscription.contract.clone(),
                    subscription.what_to_show.clone(),
                    ring_buffer.get_new_consumer().expect("Expected to be able to get_new_consumer() -> i.e. no. of consumers exceeded!"),
                ));
                self.subscriptions.insert(subscription, ring_buffer);
            }
        }

        match subscription_method {
            DbSubscriptionMethod::OnePerThread => {
                for consumer in new_consumers {
                    begin_db_consumer_thread_singular(
                        self.pool.clone(),
                        contract_scheduler.clone(),
                        consumer,
                        self.live_prices.clone(),
                    );
                }
            }
            DbSubscriptionMethod::GroupedPerThread => {
                begin_db_consumer_thread_grouped(
                    self.pool.clone(),
                    contract_scheduler.clone(),
                    new_consumers,
                    self.live_prices.clone(),
                );
            }
        }
    }

    pub fn try_get_price(&self, key: i32) -> Option<f64> {
        self.live_prices.get(&key).map(|v| v.1)
    }
}

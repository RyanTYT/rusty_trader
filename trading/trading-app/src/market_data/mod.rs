//! Live market-data pipeline: IBKR subscriptions in, cached prices and
//! strategy callbacks out.
//!
//! # Layout
//!
//! ```text
//! ibapi Client ──► producer (one thread group) ──► SpmcRingBuffer<Bar>
//!                                                     │        │
//!                                        db consumer ─┘        └─ strategy consumer
//!                                                     │
//!                                        MarketDataHandler.live_prices (60 s TTL)
//! ```
//!
//! - [`producer`](crate::market_data::producer) owns the IBKR realtime-bar subscriptions and pushes bars into
//!   the ring. Producers are *not* one-per-thread by contract: a producer group
//!   handles a batch of subscriptions.
//! - [`consumer`](crate::market_data::consumer) drains rings. [`consumer::db_consumer`](crate::market_data::consumer::db_consumer) batches bars into
//!   TimescaleDB and updates the in-memory price cache;
//!   [`consumer::strategy_consumer`](crate::market_data::consumer::strategy_consumer) hands bars to a running strategy.
//! - [`handler`](crate::market_data::handler) owns the lifetimes of every producer and consumer created for a
//!   set of [`handler::DataSubscription`](crate::market_data::handler::DataSubscription)s, and holds the live price cache.
//! - [`consolidator`](crate::market_data::consolidator) is the read-side facade used by strategies and the server:
//!   prices, validated contracts, FX conversion and strategy values.
//! - [`memoise`](crate::market_data::memoise) is the TTL cache used to avoid duplicate IBKR round trips.
//! - [`traits`](crate::market_data::traits) holds the ports (`PriceSupplier`, `ContractBroker`,
//!   `StrategyValue`) that let tests and the backtester replace IBKR.
//!
//! # Invariants
//!
//! - Exactly one [`spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer`] exists per
//!   distinct [`handler::DataSubscription`](crate::market_data::handler::DataSubscription); repeated `load_all_subscription_producers`
//!   calls with an existing subscription are no-ops.
//! - A subscription's ring has a fixed capacity ([`handler`](crate::market_data::handler) uses 128 bars) and at
//!   most 10 consumers. A full ring applies backpressure to the producer rather
//!   than dropping bars.
//! - [`handler::DataSubscription`](crate::market_data::handler::DataSubscription) equality is deliberately narrower than
//!   [`ibapi::contracts::Contract`] equality: it ignores fields that do not
//!   change the market-data stream (see its `Hash`/`PartialEq` docs).
//! - Dropping a [`handler::MarketDataHandler`](crate::market_data::handler::MarketDataHandler) does not stop its threads; call
//!   [`handler::MarketDataHandler::async_drop`](crate::market_data::handler::MarketDataHandler::async_drop) for a clean shutdown.
//!
//! # Live vs backtest
//!
//! This module is the live path. The backtester replaces it with a replay
//! oracle that supplies bars and prices through the same [`traits`](crate::market_data::traits) ports, so
//! strategy code does not branch on the source of data.

pub mod consolidator;
pub mod consumer;
pub mod handler;
pub mod memoise;
pub mod producer;
pub mod traits;

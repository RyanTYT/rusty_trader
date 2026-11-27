//! Consumers that drain each subscription's SPMC ring.
//!
//! Two kinds exist, one per sink:
//!
//! - [`db_consumer`] persists bars into TimescaleDB and refreshes the live
//!   price cache; it can run one thread per subscription or one grouped thread.
//! - [`strategy_consumer`] (`IbkrBarConsumer`) feeds bars to a running
//!   strategy's callback.
//!
//! Both are lazy per bar: a consumer that stops polling lets its ring fill, and
//! the producer then reports backpressure rather than dropping data. See
//! [`helper`] for the shared boundary/timing arithmetic.

pub mod db_consumer;
pub mod helper;
pub mod strategy_consumer;

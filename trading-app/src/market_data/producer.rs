use std::sync::Weak;

use ibapi::{
    Client,
    contracts::Contract,
    market_data::realtime::{Bar, WhatToShow},
};
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;

pub fn subscribe_to_data<const BUFFER_SIZE: usize, const MAX_NO_OF_CONSUMERS: usize>(
    weak_client: Weak<Client>,
    contract: Contract,
    what_to_show: WhatToShow,
) -> SpmcRingBuffer<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS> {
    let ring_buffer = SpmcRingBuffer::<Bar, BUFFER_SIZE, MAX_NO_OF_CONSUMERS>::new();
    let producer = ring_buffer
        .get_new_producer()
        .expect("Expected to be able to get producer for SPMC ring buffer");
    std::thread::spawn(move || {
        let client = weak_client.upgrade().expect("Expected client to be alive");
        let subscription = client
            .realtime_bars(
                &contract,
                ibapi::market_data::realtime::BarSize::Sec5,
                what_to_show,
                ibapi::market_data::TradingHours::Regular,
            )
            .expect("Expected realtime_bars request to succeed!");
        for bar in subscription.iter() {
            producer.push(bar);
        }
    });
    return ring_buffer;
}

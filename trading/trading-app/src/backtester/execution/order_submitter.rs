use ibapi::{contracts::Contract, orders::Order};

pub trait OrderSubmitter: Send + Sync {
    fn next_order_id(&self) -> i32;
    fn submit_order(&self, order_id: i32, contract: &Contract, order: &Order);
    fn cancel_order(&self, order_id: i32) -> Result<(), String>;
}

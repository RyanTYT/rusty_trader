//! Fakes for the execution tests: a market broker backed by a fixed price, and
//! a positions/orders broker that replays scripted `ibapi` events without a
//! Gateway.

use std::sync::Mutex;

use ibapi::Error;
use ibapi::accounts::{
    AccountUpdate, AccountValue, PositionMulti, PositionUpdateMulti,
    types::{AccountId, ModelCode},
};
use ibapi::orders::{
    Action, ExecutionData, ExecutionFilter, Executions, OrderData, OrderStatus, Orders,
    order_builder::market_order,
};
use ibapi::prelude::Contract;
use serde_json::Value;
use trading_app::execution::order_engine::OrderIBKR;
use trading_app::execution::traits::order_router::OrderRouter;
use trading_app::execution::traits::positions_tracker::PositionsTracker;
use trading_app::market_data::traits::contracts::ContractBroker;
use trading_app::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};

use crate::common::scenario::{FromJson, JsonError, arr, f, i, opt_s, s};

pub struct FakeMarket {
    pub price: f64,
    pub validates: bool,
}

impl FakeMarket {
    pub fn new(price: f64, validates: bool) -> Self {
        Self { price, validates }
    }
}

#[async_trait::async_trait]
impl PriceSupplier for FakeMarket {
    fn get_current_price(
        &self,
        _contract: Contract,
        _vwap: bool,
        _generic_ticks: &[&str],
    ) -> Result<f64, String> {
        Ok(self.price)
    }

    async fn populate_historical_data(
        &self,
        _contract: &Contract,
        _config: &HistoricalDataConfig,
    ) -> Result<(), String> {
        Ok(())
    }
}

impl ContractBroker for FakeMarket {
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract> {
        self.validates.then_some(contract)
    }
}

#[derive(Default)]
pub struct FakeBroker {
    pub executions: Mutex<Vec<Result<Executions, Error>>>,
    pub open_orders: Mutex<Vec<Result<Orders, Error>>>,
    pub account_updates: Mutex<Vec<Result<AccountUpdate, Error>>>,
    pub positions: Mutex<Vec<Result<PositionUpdateMulti, Error>>>,
}

impl FakeBroker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_executions(mut self, executions: Vec<Result<Executions, Error>>) -> Self {
        self.executions = Mutex::new(executions);
        self
    }

    pub fn with_open_orders(mut self, open_orders: Vec<Result<Orders, Error>>) -> Self {
        self.open_orders = Mutex::new(open_orders);
        self
    }

    pub fn with_account_updates(
        mut self,
        account_updates: Vec<Result<AccountUpdate, Error>>,
    ) -> Self {
        self.account_updates = Mutex::new(account_updates);
        self
    }

    pub fn with_positions(mut self, positions: Vec<Result<PositionUpdateMulti, Error>>) -> Self {
        self.positions = Mutex::new(positions);
        self
    }
}

impl PositionsTracker for FakeBroker {
    fn get_executions(
        &self,
        _filter: ExecutionFilter,
    ) -> impl Iterator<Item = Result<Executions, Error>> {
        std::mem::take(&mut *self.executions.lock().unwrap()).into_iter()
    }

    fn get_all_open_orders(&self) -> impl Iterator<Item = Result<Orders, Error>> {
        std::mem::take(&mut *self.open_orders.lock().unwrap()).into_iter()
    }

    fn get_account_updates(
        &self,
        _account_id: &AccountId,
    ) -> impl Iterator<Item = Result<AccountUpdate, Error>> {
        std::mem::take(&mut *self.account_updates.lock().unwrap()).into_iter()
    }

    fn get_positions_multi(
        &self,
        _account_id: Option<&AccountId>,
        _model_code: Option<&ModelCode>,
    ) -> impl Iterator<Item = Result<PositionUpdateMulti, Error>> {
        std::mem::take(&mut *self.positions.lock().unwrap()).into_iter()
    }
}

impl OrderRouter for FakeBroker {
    fn place_local_order(&self, _order_ibkr: &OrderIBKR) -> i32 {
        0
    }

    fn cancel_local_order(&self, _order_id: i32) {}
}

fn stream_error(value: &Value) -> Option<Error> {
    value
        .get("error")
        .and_then(Value::as_str)
        .map(|message| Error::Simple(message.to_string()))
}

impl FromJson for FakeBroker {
    fn from_json(streams: &Value) -> Result<Self, JsonError> {
        let mut broker = Self::new();

        if streams.get("account").is_some() {
            let mut events = Vec::new();
            for cash in arr(streams, "account") {
                match stream_error(cash) {
                    Some(error) => events.push(Err(error)),
                    None => events.push(Ok(AccountUpdate::AccountValue(AccountValue {
                        key: "CashBalance".to_string(),
                        value: f(cash, "cash").to_string(),
                        currency: s(cash, "currency"),
                        account: None,
                    }))),
                }
            }
            events.push(Ok(AccountUpdate::End));
            broker = broker.with_account_updates(events);
        }

        if streams.get("positions").is_some() {
            let mut events = Vec::new();
            for (index, p) in arr(streams, "positions").iter().enumerate() {
                if let Some(error) = stream_error(p) {
                    events.push(Err(error));
                    continue;
                }
                let contract = Contract::from_json(p)
                    .map_err(|e| e.prefix(format!("positions[{index}]")))?;
                events.push(Ok(PositionUpdateMulti::Position(PositionMulti {
                    account: "DU123".to_string(),
                    contract,
                    position: f(p, "qty"),
                    average_cost: f(p, "price"),
                    model_code: String::new(),
                })));
            }
            events.push(Ok(PositionUpdateMulti::PositionEnd));
            broker = broker.with_positions(events);
        }

        if streams.get("open_orders").is_some() {
            let mut events = Vec::new();
            for (index, o) in arr(streams, "open_orders").iter().enumerate() {
                if let Some(error) = stream_error(o) {
                    events.push(Err(error));
                    continue;
                }
                let contract = Contract::from_json(o)
                    .map_err(|e| e.prefix(format!("open_orders[{index}]")))?;
                let mut order = market_order(Action::Buy, f(o, "qty"));
                order.perm_id = i(o, "perm_id") as i32;
                order.order_id = i(o, "order_id") as i32;
                order.order_ref = opt_s(o, "reference").unwrap_or_default();

                // `emit` sends only one half of the OrderData / OrderStatus pair:
                // "order", "status", or the default "both".
                let emit = opt_s(o, "emit").unwrap_or_else(|| "both".to_string());
                if emit != "status" {
                    events.push(Ok(Orders::OrderData(OrderData {
                        order_id: order.order_id,
                        contract: contract.clone(),
                        order: order.clone(),
                        order_state: Default::default(),
                    })));
                }
                if emit != "order" {
                    let filled = o.get("filled").and_then(Value::as_f64).unwrap_or(0.0);
                    events.push(Ok(Orders::OrderStatus(OrderStatus {
                        order_id: order.order_id,
                        status: opt_s(o, "status").unwrap_or_else(|| "Submitted".to_string()),
                        filled,
                        remaining: f(o, "qty") - filled,
                        perm_id: order.perm_id,
                        ..Default::default()
                    })));
                }
            }
            broker = broker.with_open_orders(events);
        }

        if streams.get("executions").is_some() {
            let mut events = Vec::new();
            for (index, e) in arr(streams, "executions").iter().enumerate() {
                if let Some(error) = stream_error(e) {
                    events.push(Err(error));
                    continue;
                }
                events.push(Ok(Executions::ExecutionData(
                    ExecutionData::from_json(e)
                        .map_err(|err| err.prefix(format!("executions[{index}]")))?,
                )));
            }
            broker = broker.with_executions(events);
        }

        Ok(broker)
    }
}

/// Records every order it is asked to place and returns sequential ids starting at
/// 1000. Indices listed in `failures` return `-1` instead (IBKR's submit failure).
pub struct FakeOrderRouter {
    calls: Mutex<Vec<OrderIBKR>>,
    cancellations: Mutex<Vec<i32>>,
    failures: std::collections::HashSet<usize>,
}

impl FakeOrderRouter {
    pub fn new(failures: &[usize]) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            cancellations: Mutex::new(Vec::new()),
            failures: failures.iter().copied().collect(),
        }
    }

    pub fn placed(&self) -> Vec<OrderIBKR> {
        self.calls.lock().unwrap().clone()
    }

    pub fn cancelled(&self) -> Vec<i32> {
        self.cancellations.lock().unwrap().clone()
    }
}

impl OrderRouter for FakeOrderRouter {
    fn place_local_order(&self, order_ibkr: &OrderIBKR) -> i32 {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push(order_ibkr.clone());
        if self.failures.contains(&index) {
            return -1;
        }
        1000 + index as i32
    }

    fn cancel_local_order(&self, order_id: i32) {
        self.cancellations.lock().unwrap().push(order_id);
    }
}

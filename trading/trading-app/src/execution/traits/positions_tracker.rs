use std::sync::Arc;

use ibapi::{
    Client,
    accounts::{
        AccountUpdate, PositionUpdateMulti,
        types::{AccountId, ModelCode},
    },
    client::Subscription,
    orders::{ExecutionFilter, Executions, Orders},
};

pub trait PositionsTracker {
    fn get_executions(&self, filter: ExecutionFilter) -> Subscription<Executions>;
    fn get_all_open_orders(&self) -> Subscription<Orders>;
    fn get_account_updates(&self, account_id: &AccountId) -> Subscription<AccountUpdate>;
    fn get_positions_multi(
        &self,
        account_id: Option<&AccountId>,
        model_code: Option<&ModelCode>,
    ) -> Subscription<PositionUpdateMulti>;
}

impl<T> PositionsTracker for Arc<T>
where
    T: PositionsTracker,
{
    fn get_executions(&self, filter: ExecutionFilter) -> Subscription<Executions> {
        (**self).get_executions(filter)
    }
    fn get_account_updates(&self, account_id: &AccountId) -> Subscription<AccountUpdate> {
        (**self).get_account_updates(account_id)
    }
    fn get_all_open_orders(&self) -> Subscription<Orders> {
        (**self).get_all_open_orders()
    }
    fn get_positions_multi(
        &self,
        account_id: Option<&AccountId>,
        model_code: Option<&ModelCode>,
    ) -> Subscription<PositionUpdateMulti> {
        (**self).get_positions_multi(account_id, model_code)
    }
}

impl PositionsTracker for Client {
    fn get_executions(&self, filter: ExecutionFilter) -> Subscription<Executions> {
        self.executions(filter)
            .expect("Error requesting executions from IBKR")
    }

    fn get_all_open_orders(&self) -> Subscription<Orders> {
        self.all_open_orders()
            .expect("Error requesting open orders from IBKR")
    }

    fn get_account_updates(&self, account_id: &AccountId) -> Subscription<AccountUpdate> {
        self.account_updates(account_id)
            .expect("Error requesting account updates from IBKR")
    }

    fn get_positions_multi(
        &self,
        account_id: Option<&AccountId>,
        model_code: Option<&ModelCode>,
    ) -> Subscription<PositionUpdateMulti> {
        self.positions_multi(account_id, model_code)
            .expect("Error requesting positions multi from IBKR")
    }
}

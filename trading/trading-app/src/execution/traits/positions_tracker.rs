//! Lazy, owning views over the ibapi subscription streams used by the syncer.
//!
//! Each method opens the subscription and returns an iterator that owns it.
//! Nothing is collected into a `Vec`: items are yielded as they arrive, and the
//! iterator ends after `SYNC_IDLE_TIMEOUT` without an item. The final error (if
//! any) is yielded as the last item, then the iterator stops; dropping the
//! iterator cancels the subscription (`Subscription::drop` calls `cancel`).
//!
//! The subscription type is generic over ibapi's private `StreamDecoder`, so
//! the adapter is built inside each method with `std::iter::from_fn` rather
//! than a named struct. A macro keeps the four implementations identical.

use std::{sync::Arc, time::Duration};

use ibapi::{
    Client, Error,
    accounts::{
        AccountUpdate, PositionUpdateMulti,
        types::{AccountId, ModelCode},
    },
    orders::{ExecutionFilter, Executions, Orders},
};

/// How long an idle subscription waits before it is treated as finished.
const SYNC_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Turns a subscription into an owning, lazy iterator.
///
/// Yields `Ok` items until the subscription idles for
/// [`SYNC_IDLE_TIMEOUT`] or ends; a terminal error is yielded once as `Err`.
macro_rules! timeout_iter {
    ($subscription:expr) => {{
        let subscription = $subscription;
        let mut finished = false;
        std::iter::from_fn(move || {
            if finished {
                return None;
            }
            match subscription.next_timeout(SYNC_IDLE_TIMEOUT) {
                Some(item) => Some(Ok(item)),
                None => {
                    finished = true;
                    subscription.error().map(Err)
                }
            }
        })
    }};
}

/// Streams the broker state the syncer reconciles against.
///
/// Every method opens a subscription and returns an iterator that owns it.
/// Items arrive lazily; the iterator ends after ten seconds without an item,
/// and a stream error is yielded as the final `Err`. Dropping the iterator
/// cancels the subscription, so callers should not collect unless they need
/// the whole stream.
///
/// # Panics
///
/// The `Client` implementation panics when the subscription cannot be created
/// (for example, an unknown execution filter); stream errors afterwards are
/// reported as `Err` values.
pub trait PositionsTracker {
    /// Streams executions matching `filter`.
    ///
    /// Yields [`Executions`] values: fills, commission reports and notices,
    /// interleaved in broker order.
    fn get_executions(
        &self,
        filter: ExecutionFilter,
    ) -> impl Iterator<Item = Result<Executions, Error>>;

    /// Streams the resting open-order book.
    ///
    /// `OrderData` and `OrderStatus` arrive as separate items keyed by
    /// `perm_id`; the caller is responsible for pairing them.
    fn get_all_open_orders(&self) -> impl Iterator<Item = Result<Orders, Error>>;

    /// Streams account values for `account_id`.
    ///
    /// Ends with an `End` marker; cash balances arrive as `AccountValue` rows
    /// whose `key` is `"CashBalance"`.
    fn get_account_updates(
        &self,
        account_id: &AccountId,
    ) -> impl Iterator<Item = Result<AccountUpdate, Error>>;

    /// Streams positions, optionally filtered by account and model.
    ///
    /// `None` means "no filter". Each position is followed by an eventual
    /// `PositionEnd` marker for its batch.
    fn get_positions_multi(
        &self,
        account_id: Option<&AccountId>,
        model_code: Option<&ModelCode>,
    ) -> impl Iterator<Item = Result<PositionUpdateMulti, Error>>;
}

/// Forwards through an `Arc`, so shared fakes and clients behave the same.
impl<T> PositionsTracker for Arc<T>
where
    T: PositionsTracker,
{
    fn get_executions(
        &self,
        filter: ExecutionFilter,
    ) -> impl Iterator<Item = Result<Executions, Error>> {
        (**self).get_executions(filter)
    }

    fn get_all_open_orders(&self) -> impl Iterator<Item = Result<Orders, Error>> {
        (**self).get_all_open_orders()
    }

    fn get_account_updates(
        &self,
        account_id: &AccountId,
    ) -> impl Iterator<Item = Result<AccountUpdate, Error>> {
        (**self).get_account_updates(account_id)
    }

    fn get_positions_multi(
        &self,
        account_id: Option<&AccountId>,
        model_code: Option<&ModelCode>,
    ) -> impl Iterator<Item = Result<PositionUpdateMulti, Error>> {
        (**self).get_positions_multi(account_id, model_code)
    }
}

/// Live implementation over the IB subscription API.
impl PositionsTracker for Client {
    fn get_executions(
        &self,
        filter: ExecutionFilter,
    ) -> impl Iterator<Item = Result<Executions, Error>> {
        timeout_iter!(
            self.executions(filter)
                .expect("Error requesting executions from IBKR")
        )
    }

    fn get_all_open_orders(&self) -> impl Iterator<Item = Result<Orders, Error>> {
        timeout_iter!(
            self.all_open_orders()
                .expect("Error requesting open orders from IBKR")
        )
    }

    fn get_account_updates(
        &self,
        account_id: &AccountId,
    ) -> impl Iterator<Item = Result<AccountUpdate, Error>> {
        timeout_iter!(
            self.account_updates(account_id)
                .expect("Error requesting account updates from IBKR")
        )
    }

    fn get_positions_multi(
        &self,
        account_id: Option<&AccountId>,
        model_code: Option<&ModelCode>,
    ) -> impl Iterator<Item = Result<PositionUpdateMulti, Error>> {
        timeout_iter!(
            self.positions_multi(account_id, model_code)
                .expect("Error requesting positions multi from IBKR")
        )
    }
}

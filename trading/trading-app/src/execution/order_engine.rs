//! Order placement and bar-outcome reconciliation.
//!
//! [`OrderEngine`](crate::execution::order_engine::OrderEngine) is the write
//! side of the execution layer: it turns a
//! [`BarUpdateOutcome`](crate::strategy::strategy::BarUpdateOutcome) into broker
//! submissions and keeps the database's working-order book in step with them.
//!
//! # Outcome paths
//!
//! - [`BarUpdateOutcome::EmitOrders`](crate::strategy::strategy::BarUpdateOutcome::EmitOrders)
//!   cancels the listed rows **before** submitting the batch, so a cancel from
//!   one bar can never hit an order the same bar just placed.
//! - [`BarUpdateOutcome::PendingDbQuery`](crate::strategy::strategy::BarUpdateOutcome::PendingDbQuery)
//!   reads target-position diffs for every requested asset class, shares one
//!   cash budget across them (`CASH:` rows live in the stock table), builds an
//!   [`FxFundingPlan`](crate::execution::fx_organiser::FxFundingPlan), and
//!   reconciles each batch against the working orders for that `order_ref`.
//! - [`BarUpdateOutcome::NoAction`](crate::strategy::strategy::BarUpdateOutcome::NoAction)
//!   does nothing.
//!
//! # Invariants
//!
//! - [`OrderEngine::place_order`](crate::execution::order_engine::OrderEngine::place_order)
//!   writes an *optimistic* row keyed by `(perm_id = -1, order_id)`; the broker
//!   acknowledgement replaces it with the real `perm_id` (see
//!   `order_update_stream::event_handlers::open_order`).
//! - [`OrderIBKR`](crate::execution::order_engine::OrderIBKR) equality ignores
//!   the `:price` suffix of `order_ref`; the suffix only distinguishes parked
//!   funding keys (`strategy:price`).
//! - Bracket legs reference their head by index into the submitted batch
//!   ([`OrderIBKR::references_parent_order`](crate::execution::order_engine::OrderIBKR::references_parent_order)),
//!   and
//!   [`OrderEngine::place_orders`](crate::execution::order_engine::OrderEngine::place_orders)
//!   rewrites `parent_id` from the ids it actually received.
//! - `CASH:<ccy>` target rows are funding capacity, never orders
//!   ([`is_cash_diff`](crate::execution::order_engine::is_cash_diff)).
//!
//! # Example
//!
//! Two orders that differ only in the price suffix of `order_ref` are the same
//! order for reconciliation purposes:
//!
//! ```
//! use ibapi::orders::{Action, order_builder::market_order};
//! use ibapi::prelude::Contract;
//! use trading_app::execution::order_engine::OrderIBKR;
//!
//! let contract = Contract::stock("AAPL").build();
//! let mut priced = market_order(Action::Buy, 10.0);
//! priced.order_ref = "momentum:150.25".to_string();
//! let mut bare = priced.clone();
//! bare.order_ref = "momentum".to_string();
//!
//! assert_eq!(
//!     OrderIBKR::new(contract.clone(), priced, -1),
//!     OrderIBKR::new(contract, bare, -1),
//! );
//! ```

// Issues: Line 619
// - NEED to update place_order to function properly and efficiently
// await for order updates
// - once async version is released and stable - can use tokio threads instead of new os kernel
// thread
// - More issues:
// -- Need to deconflict potential revisions to executions - .02 is a revision to an execution with
// .01 and so on
// -- Finish writing the place_orders_for_strategy for options
// -- Update the place_orders_for_strategy functionality to update according to current open
// orders, ...
// -- place_orders_for_strategy fn will update open orders but if it updates it too late, an
// execution may come in with no open order and execution (NOTE: from my understanding this should
// be fine as the new execution will be filed under "unknown" for which target positions will
// always be 0 && when we delete the open order it is only because the open order is in the
// opposite direction we want to go in - i.e. under "unknown", position to go to 0 is also fine -
// just maybe different order types but that is fine - should be minimal impact)
#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use crate::execution::traits::order_router::OrderRouter;
use crate::{database::tables::DbTables, market_data::traits::current_price::PriceSupplier};
use ibapi::Client;
use ibapi::{
    orders::{Action, Order, order_builder::market_order},
    prelude::Contract,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
#[cfg(not(feature = "backtest"))]
use std::sync::Weak;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{AssetType, OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys},
        models_crud::{
            open_orders::open_orders::{
                OpenOrdersFullKeys, OpenOrdersOps, OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys,
            },
            target_positions::target_positions::{TargetPositionsOps, TargetPositionsQtyDiff},
        },
    },
    execution::{fx_backed_up_order::OrderStore, fx_organiser::FxFundingPlan},
    helpers::contract::{HashContract, LocalContractTypes, get_contract_from},
    strategy::strategy::{BarUpdateOutcome, StrategyDetails, StrategyEnum},
};

// pub struct StaleOrderEngine {
//     pub pool: PgPool,
//     // pub(super) strategy_map: Arc<HashMap<String, StrategyEnum>>,
//     // pub(super) account: String,
// }

/// Execution-layer façade for order placement and reconciliation.
///
/// Holds only long-lived context — a pool and a runtime handle — so it is cheap
/// to clone and safe to share. Every operation opens its own short-lived
/// database session ([`crate::database::db::DbDirect`]) rather than caching a
/// connection.
#[derive(Debug, Clone)]
pub struct OrderEngine {
    pool: PgPool,
    tokio_handle: tokio::runtime::Handle,
    // pub(super) strategy_map: Arc<HashMap<String, StrategyEnum>>,
    // pub(super) account: String,
    // order_update_stream: Arc<OrderUpdateStreamController>,
}

/// A broker order plus the contract it trades.
///
/// `references_parent_order` is an index into the batch being submitted: `-1`
/// means "standalone", and `>= 0` points at the head order whose broker id
/// becomes this order's `parent_id`.
///
/// # Equality
///
/// Equality compares the contract and the order after truncating `order_ref` at
/// the first `:` on both sides. The suffix exists only to make parked funding
/// orders unique in [`OrderStore`]; the same working order may be recorded as
/// `strategy` or `strategy:price` depending on who wrote it.
///
/// ```
/// use ibapi::orders::{Action, order_builder::market_order};
/// use ibapi::prelude::Contract;
/// use trading_app::execution::order_engine::OrderIBKR;
///
/// let contract = Contract::stock("AAPL").build();
/// let mut priced = market_order(Action::Sell, 25.0);
/// priced.order_ref = "orb:101.5".to_string();
/// let mut bare = priced.clone();
/// bare.order_ref = "orb".to_string();
///
/// assert_eq!(
///     OrderIBKR::new(contract.clone(), priced, -1),
///     OrderIBKR::new(contract, bare, -1),
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIBKR {
    pub contract: Contract,
    pub order: Order,
    // For efficiency purposes
    // -> we directly link via the index of the parent in the Vec this is passed in
    pub references_parent_order: i32,
}

impl PartialEq for OrderIBKR {
    fn eq(&self, other: &OrderIBKR) -> bool {
        let mut self_order = self.order.clone();
        self_order.order_ref = self_order.order_ref.split(":").next().unwrap().to_string();
        let mut order = other.order.clone();
        order.order_ref = order.order_ref.split(":").next().unwrap().to_string();
        return self.contract.eq(&other.contract) && self_order.eq(&order);
    }
}
impl Eq for OrderIBKR {}

impl OrderIBKR {
    /// Builds the pair, recording where the parent sits in the submitted batch.
    pub fn new(contract: Contract, order: Order, references_parent_order: i32) -> Self {
        Self {
            contract,
            order,
            references_parent_order,
        }
    }
}

/// A working order as reconciliation sees it.
///
/// `is_from_db` distinguishes the authoritative row (`true`) from a parked
/// order replayed out of [`OrderStore`] (`false`). Cancellation uses that flag
/// to decide whether to delete a database row or edit the store.
#[derive(Debug, Clone)]
pub struct LocalOpenOrder {
    pub order: OpenOrdersFullKeys,
    pub is_from_db: bool,
}

#[hotpath::measure_all]
impl OrderEngine {
    /// Creates the façade. Both handles are cheap to clone.
    pub fn new(
        pool: PgPool,
        tokio_handle: tokio::runtime::Handle,
        // active_strategies: &Vec<StrategyParameters>,
    ) -> Self {
        // create strategy map
        // let mut strategy_map = HashMap::new();
        // for strategy in active_strategies {
        //     strategy_map.insert(strategy.strategy.get_name(), strategy.strategy.clone());
        // }
        Self { pool, tokio_handle }
    }

    /// Submits one order and records an optimistic open-order row.
    ///
    /// The caller owns `transmit`; bracket legs must arrive with the parent's
    /// transmit semantics already set.
    ///
    /// Writes the row under the optimistic primary key `(perm_id = -1,
    /// order_id)` from a spawned task, so the ack can replace it with the
    /// broker's perm id. A database failure is logged, not returned.
    ///
    /// # Returns
    ///
    /// The broker order id, or `-1` when the broker rejected the order (in
    /// which case no row is written).
    ///
    /// # Panics
    ///
    /// Panics if the client weak reference is already dead.
    #[cfg(not(feature = "backtest"))]
    pub fn place_order<T>(
        handle: tokio::runtime::Handle,
        pool: PgPool,
        weak_client: &Weak<T>,
        order_ibkr: OrderIBKR,
    ) -> i32
    where
        T: OrderRouter,
    {
        let client = {
            let client_opt = weak_client.upgrade();
            if client_opt.is_none() {
                tracing::error!("Client is dead when trying to place order");
                panic!("Client died when trying to place order");
            }
            client_opt.unwrap()
        };
        let strategy_name = order_ibkr.order.order_ref.clone();
        #[allow(unused_variables)]
        let total_label = StrategyEnum::metric_label(&strategy_name, "order_submit_total");
        #[allow(unused_variables)]
        let wire_label = StrategyEnum::metric_label(&strategy_name, "order_submit_ibkr_wire");
        hotpath::measure_block!(total_label, {
            let order_id =
                hotpath::measure_block!(wire_label, client.place_local_order(&order_ibkr));
            tracing::info!(
                "Order submitted to IBKR: {:?} for {:?}",
                order_ibkr.order.action,
                order_ibkr.contract.symbol
            );
            if order_id == -1 {
                return -1;
            }

            let asset_type = AssetType::from_str(&order_ibkr.contract.security_type);
            let pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, order_id);
            let uk =
                OpenOrdersUpdateKeys::new(&asset_type, &order_ibkr.contract, &order_ibkr.order);
            handle.spawn(
                hotpath::future!(
                    async move {
                        let mut db = crate::database::db::DbDirect::connect(&pool);
                        let mut open_orders_crud = db.open_orders(&asset_type);
                        if let Err(e) = open_orders_crud.create_or_update(&pk, &uk).await {
                            tracing::error!("Failed to create or update open order for place_order (Optimistic Open Order): {e:?}");
                        };
                    },
                    label = "order_submit_db_write"
                )
            );

            order_id
        })
    }

    /// Backtest variant: submits through the replay [`OrderSubmitter`] and
    /// returns the id it allocated. The replay engine owns the order book, so
    /// no optimistic database row is written here.
    ///
    /// [`OrderSubmitter`]: crate::backtester::execution::OrderSubmitter
    #[cfg(feature = "backtest")]
    pub fn place_order(
        handle: tokio::runtime::Handle,
        pool: PgPool,
        submitter: &dyn crate::backtester::execution::OrderSubmitter,
        order_ibkr: OrderIBKR,
    ) -> i32 {
        let order_id = submitter.next_order_id();
        submitter.submit_order(order_id, &order_ibkr.contract, &order_ibkr.order);
        order_id
    }

    /// `place_order` that surfaces broker failure instead of logging it.
    ///
    /// Same wire call and the same optimistic row as [`OrderEngine::place_order`],
    /// but a rejected submission returns `Err` and writes **no** open-order row,
    /// so the books never show an order the broker refused.
    ///
    /// # Errors
    ///
    /// - the client weak reference is dead;
    /// - the broker rejects the order (returns `-1`).
    ///
    /// Database failures after a successful submission are still only logged,
    /// because the order is live at the broker and the ack path will heal the
    /// row.
    #[cfg(not(feature = "backtest"))]
    pub fn place_order_checked<T>(
        handle: tokio::runtime::Handle,
        pool: PgPool,
        weak_client: &Weak<T>,
        order_ibkr: OrderIBKR,
    ) -> Result<i32, String>
    where
        T: OrderRouter,
    {
        let client = {
            let client_opt = weak_client.upgrade();
            if client_opt.is_none() {
                tracing::error!("Client is dead when trying to place order");
                return Err("Client is dead when trying to place order".to_string());
            }
            client_opt.unwrap()
        };

        let strategy_name = order_ibkr.order.order_ref.clone();
        #[allow(unused_variables)]
        let wire_label = StrategyEnum::metric_label(&strategy_name, "order_submit_ibkr_wire");
        let order_id = hotpath::measure_block!(wire_label, client.place_local_order(&order_ibkr));
        if order_id == -1 {
            return Err("Could not place order in IBKR".to_string());
        }
        tracing::info!(
            "Order submitted to IBKR: {:?} for {:?}",
            order_ibkr.order.action,
            order_ibkr.contract.symbol
        );

        let asset_type = AssetType::from_str(&order_ibkr.contract.security_type);
        let pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, order_id);
        let uk = OpenOrdersUpdateKeys::new(&asset_type, &order_ibkr.contract, &order_ibkr.order);
        handle.spawn(hotpath::future!(
            async move {
                let mut db = crate::database::db::DbDirect::connect(&pool);
                let mut open_orders_crud = db.open_orders(&asset_type);
                if let Err(e) = open_orders_crud.create_or_update(&pk, &uk).await {
                    tracing::error!(
                        "Failed to create or update open order for place_order_checked: {e:?}"
                    );
                }
            },
            label = "order_submit_db_write"
        ));

        Ok(order_id)
    }

    /// Submits a batch, linking bracket legs to their head.
    ///
    /// `references_parent_order` is rewritten to the real broker id of the
    /// referenced entry, so callers use positions in this iterator rather than
    /// ids they cannot know yet.
    ///
    /// A failure of the head (`idx == 0`, id `-1`) aborts the rest of the batch;
    /// failures of later legs are logged and the batch continues. The caller
    /// owns `transmit` on every order.
    pub fn place_orders<#[cfg(not(feature = "backtest"))] T: OrderRouter>(
        handle: tokio::runtime::Handle,
        pool: PgPool,

        #[cfg(not(feature = "backtest"))] weak_client: &Weak<T>,
        #[cfg(feature = "backtest")] submitter: &dyn crate::backtester::execution::OrderSubmitter,

        orders: impl IntoIterator<Item = OrderIBKR>,
    ) {
        let orders_iter = orders.into_iter();
        let (lower_bound, _) = orders_iter.size_hint();
        let mut order_ids = vec![-1; lower_bound];
        for (idx, mut order_ibkr) in orders_iter.enumerate() {
            // let order = order_ibkr.order;
            if order_ibkr.references_parent_order >= 0 {
                order_ibkr.order.parent_id = order_ids[order_ibkr.references_parent_order as usize];
            }

            #[cfg(not(feature = "backtest"))]
            let order_id =
                Self::place_order(handle.clone(), pool.clone(), &weak_client, order_ibkr);
            #[cfg(feature = "backtest")]
            let order_id = Self::place_order(handle.clone(), pool.clone(), submitter, order_ibkr);
            if order_id == -1 {
                if idx == 0 {
                    break;
                }
                tracing::error!("Bracket order for for some order failed!");
            }
            order_ids[idx] = order_id;
        }
    }

    /// Cancels a list of working orders on the broker and in local storage.
    ///
    /// - Orders with a real `perm_id` are cancelled at the broker (live) or
    ///   through the submitter (backtest); the optimistic `perm_id == -1` rows
    ///   never were accepted, so they are only removed locally.
    /// - `is_from_db` rows are deleted from the database; parked rows are
    ///   removed from [`OrderStore`] under their bare strategy name.
    ///
    /// Broker-side cancellation runs on a fresh OS thread in live mode, so this
    /// method does not await it.
    fn cancel_orders<
        #[cfg(not(feature = "backtest"))] OrderBroker: OrderRouter + Send + Sync + 'static,
    >(
        &self,
        asset_type: &AssetType,

        #[cfg(not(feature = "backtest"))] weak_client: &Weak<OrderBroker>,
        #[cfg(feature = "backtest")] submitter: &dyn crate::backtester::execution::OrderSubmitter,

        local_open_orders: Vec<LocalOpenOrder>,
        order_store: &OrderStore,
    ) {
        if local_open_orders.is_empty() {
            return;
        }

        let mut to_delete_from_order_store = HashMap::new();
        local_open_orders.into_iter().for_each(|open_order| {
            let (order_perm_id, order_id, strategy) = match &open_order.order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    order_perm_id,
                    order_id,
                    strategy,
                    ..
                }) => (order_perm_id, order_id, strategy.clone()),
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                    order_perm_id,
                    order_id,
                    strategy,
                    ..
                }) => (order_perm_id, order_id, strategy.clone()),
            };
            // in the case that we are cancelling OrderStore items
            let strategy = strategy.split(":").next().unwrap().to_string();

            #[cfg(not(feature = "backtest"))]
            if *order_perm_id != -1 {
                // Cancel on IBKR side
                let cloned_weak_client = weak_client.clone();
                let owned_order_id = *order_id;
                std::thread::spawn(move || {
                    hotpath::measure_block!("cancel_order_on_ibkr", {
                        let client_opt = cloned_weak_client.upgrade();
                        if client_opt.is_none() {
                            tracing::warn!("client died while cancelling order!");
                            return;
                        }
                        let client = client_opt.unwrap();
                        client.cancel_local_order(owned_order_id);
                    });
                });
            }
            #[cfg(feature = "backtest")]
            // Parked/deferred orders in backtest mode cancel regardless (perm_id means nth thr)
            if let Err(e) = submitter.cancel_order(*order_id) {
                tracing::error!("Failed to cancel order ({:?}): {e:?}", *order_id)
            }

            if open_order.is_from_db {
                // Cancel on Local Side (i.e. DB)
                let open_order_pk =
                    OpenOrdersPrimaryKeys::new(&asset_type, *order_perm_id, *order_id);
                let spawn_pool = self.pool.clone();
                let spawn_asset_type = asset_type.clone();
                self.tokio_handle.spawn(hotpath::future!(
                    async move {
                        let mut db = crate::database::db::DbDirect::connect(&spawn_pool);
                        let mut open_orders_crud = db.open_orders(&spawn_asset_type);
                        if let Err(e) = open_orders_crud.delete(&open_order_pk).await {
                            tracing::error!("Error trying to delete OpenOptionOrder entry: {e:?}")
                        };
                    },
                    label = "delete_open_order"
                ));
            } else {
                to_delete_from_order_store
                    .entry(strategy)
                    .and_modify(|v: &mut Vec<LocalOpenOrder>| v.push(open_order.clone()))
                    .or_insert(vec![open_order]);
            }
        });

        for (strategy_name, local_open_orders) in to_delete_from_order_store.iter() {
            let orders_to_remove: Vec<OrderIBKR> = local_open_orders
                .into_iter()
                .map(|open_order| OrderIBKR {
                    contract: get_contract_from(&LocalContractTypes::OpenOrders(
                        open_order.order.clone(),
                    )),
                    order: {
                        let qty = open_order.order.get_qty();
                        let mut order = market_order(
                            if qty < 0.0 { Action::Sell } else { Action::Buy },
                            qty.abs(),
                        );
                        order.transmit = true;
                        order.order_ref = strategy_name.to_string();
                        order
                    },
                    references_parent_order: -1,
                })
                .collect();
            if let Err(e) = order_store.remove_orders(&strategy_name, &orders_to_remove) {
                tracing::error!(
                    "Failed to remove cancelled orders from redb backed_up_orders for Strategy ({strategy_name}): {e:?}"
                );
            }
        }
    }

    /// Executes whatever a strategy asked for after a bar.
    ///
    /// This is the main reconciliation entry point, called once per strategy
    /// per bar from the strategy driver.
    ///
    /// - **`EmitOrders`** cancels the listed stock and option rows first, then
    ///   submits the batch. The head order failing aborts the batch; later legs
    ///   may fail individually.
    /// - **`PendingDbQuery`** reads target diffs and working orders for each
    ///   requested asset class, adds the stock `CASH:` rows as shared funding
    ///   capacity, and skips any asset class already covered by its working
    ///   orders ([`requires_reconciliation`]). The surviving diffs go through
    ///   one [`OrderEngine::get_fx_attachments`] pass, whose batches are
    ///   reconciled by [`OrderEngine::on_new_qty_diff_for_strat`]; standalone FX
    ///   roots are submitted directly and unfunded orders are parked.
    /// - **`NoAction`** does nothing.
    ///
    /// Errors are logged, never returned: this runs inside the strategy loop,
    /// and a failed bar must not stall the strategy. A missing target table or
    /// unreadable working book just means the class is skipped this bar.
    ///
    /// # Blocking
    ///
    /// Uses [`tokio::runtime::Handle::block_on`] for its database reads, so it
    /// must not be called from inside an async task; the driver calls it on a
    /// blocking thread.
    pub fn handle_bar_update_outcome<
        #[cfg(not(feature = "backtest"))] OrderBroker: OrderRouter + Send + Sync + 'static,
        #[cfg(not(feature = "backtest"))] MarketBroker: PriceSupplier,
    >(
        &self,

        #[cfg(not(feature = "backtest"))] weak_client: &Weak<OrderBroker>,
        #[cfg(feature = "backtest")] submitter: &dyn crate::backtester::execution::OrderSubmitter,
        #[cfg(not(feature = "backtest"))] consolidator: &Weak<MarketBroker>,
        #[cfg(feature = "backtest")]
        price_supplier: &dyn crate::market_data::traits::current_price::PriceSupplier,

        bar_update_outcome: BarUpdateOutcome,
        strategy_detail: &StrategyDetails,
        order_store: &OrderStore,
    ) {
        let strategy_name_orig = strategy_detail.name.clone();
        match bar_update_outcome {
            BarUpdateOutcome::EmitOrders {
                place_orders: orders_ibkr,
                cancel_orders: orders_to_cancel,
            } => {
                // Cancel BEFORE placing - preventing cancel from polluting just placed orders
                let stock_orders_to_cancel = orders_to_cancel
                    .iter()
                    .filter_map(|order| match order {
                        OpenOrdersFullKeys::Stock(_) => Some(LocalOpenOrder {
                            order: order.clone(),
                            is_from_db: true,
                        }),
                        _ => None,
                    })
                    .collect();
                let option_orders_to_cancel = orders_to_cancel
                    .iter()
                    .filter_map(|order| match order {
                        OpenOrdersFullKeys::Options(_) => Some(LocalOpenOrder {
                            order: order.clone(),
                            is_from_db: true,
                        }),
                        _ => None,
                    })
                    .collect();
                self.cancel_orders(
                    &AssetType::Stock,
                    #[cfg(not(feature = "backtest"))]
                    weak_client,
                    #[cfg(feature = "backtest")]
                    submitter,
                    stock_orders_to_cancel,
                    &order_store,
                );
                self.cancel_orders(
                    &AssetType::Option,
                    #[cfg(not(feature = "backtest"))]
                    weak_client,
                    #[cfg(feature = "backtest")]
                    submitter,
                    option_orders_to_cancel,
                    &order_store,
                );
                #[cfg(not(feature = "backtest"))]
                Self::place_orders(
                    self.tokio_handle.clone(),
                    self.pool.clone(),
                    weak_client,
                    orders_ibkr,
                );
                #[cfg(feature = "backtest")]
                Self::place_orders(
                    self.tokio_handle.clone(),
                    self.pool.clone(),
                    submitter,
                    orders_ibkr,
                );
            }

            BarUpdateOutcome::PendingDbQuery(asset_types) => {
                // One funding pass across asset classes: the CASH: rows live in
                // the stock table, so read both sets here and share the budget.
                // Stock is read even when only options are requested.
                let read_diffs = |asset_type: AssetType| -> Option<Vec<TargetPositionsQtyDiff>> {
                    let mut db = crate::database::db::DbDirect::connect(&self.pool);
                    let mut target_positions_crud = db.target_positions(&asset_type);
                    let strategy_name = strategy_name_orig.clone();
                    match hotpath::measure_block!(
                        StrategyEnum::metric_label(
                            &strategy_name_orig,
                            "get_target_pos_diff_by_strat"
                        ),
                        {
                            self.tokio_handle.block_on(async move {
                                target_positions_crud
                                    .get_target_pos_diff_by_strat(&strategy_name)
                                    .await
                            })
                        }
                    ) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            tracing::error!("target_qty_diff error ({asset_type:?}): {e}");
                            None
                        }
                    }
                };

                let open_orders_for =
                    |asset_type: AssetType| -> Result<Vec<LocalOpenOrder>, String> {
                        let mut db = crate::database::db::DbDirect::connect(&self.pool);
                        let mut open_orders_crud = db.open_orders(&asset_type);
                        let strategy_name = strategy_name_orig.clone();
                        let orders = self
                            .tokio_handle
                            .block_on(async move {
                                let res = open_orders_crud
                                    .get_orders_for_strat(
                                        &strategy_name,
                                        #[cfg(feature = "backtest")]
                                        BacktestMode::Db,
                                    )
                                    .await;
                                res
                            })
                            .map_err(|e| format!("{e:?}"))?;
                        let mut open_orders: Vec<LocalOpenOrder> = orders
                            .into_iter()
                            .map(|open_order| LocalOpenOrder {
                                order: open_order,
                                is_from_db: true,
                            })
                            .collect();
                        let parked = order_store
                            .load_orders(&strategy_name_orig)
                            .map_err(|e| format!("{e:?}"))?
                            .unwrap_or_default();
                        for order in parked {
                            open_orders.push(LocalOpenOrder {
                                order: OpenOrdersFullKeys::from_contract_and_order(
                                    &order.contract,
                                    &order.order,
                                    0.0,
                                ),
                                is_from_db: false,
                            });
                        }
                        Ok(open_orders)
                    };

                let wants_stock = asset_types.iter().any(|a| matches!(a, &AssetType::Stock));
                let wants_option = asset_types.iter().any(|a| matches!(a, &AssetType::Option));
                let stock_diffs = read_diffs(AssetType::Stock).unwrap_or_default();
                let option_diffs = if wants_option {
                    read_diffs(AssetType::Option).unwrap_or_default()
                } else {
                    Vec::new()
                };

                // CASH rows are funding capacity, never orders: always include
                // them so option openings can draw the same cash as stocks.
                let mut combined: Vec<TargetPositionsQtyDiff> = stock_diffs
                    .iter()
                    .filter(|d| is_cash_diff(d))
                    .cloned()
                    .collect();
                if wants_stock {
                    match open_orders_for(AssetType::Stock) {
                        Ok(open_orders) => {
                            if requires_reconciliation(&stock_diffs, open_orders, strategy_detail) {
                                combined.extend(
                                    stock_diffs.iter().filter(|d| !is_cash_diff(d)).cloned(),
                                );
                            }
                        }
                        Err(e) => tracing::error!("open orders crud error (Stock): {e}"),
                    }
                }
                if wants_option {
                    match open_orders_for(AssetType::Option) {
                        Ok(open_orders) => {
                            if requires_reconciliation(&option_diffs, open_orders, strategy_detail)
                            {
                                combined.extend(option_diffs.iter().cloned());
                            }
                        }
                        Err(e) => tracing::error!("open orders crud error (Option): {e}"),
                    }
                }

                // Cash only means every requested type is covered: nothing to plan.
                if combined.iter().any(|d| !is_cash_diff(d)) {
                    // Plan funding once per bar; every batch below is [head] or
                    // [closing head, fx legs..] and is reconciled against working orders.
                    let fx_plan = hotpath::measure_block!(
                        StrategyEnum::metric_label(&strategy_name_orig, "compute_fx_attachments"),
                        {
                            if strategy_detail.is_fx_strategy {
                                FxFundingPlan::default()
                            } else {
                                match OrderEngine::get_fx_attachments(
                                    &combined,
                                    &strategy_name_orig,
                                    order_store,
                                    #[cfg(not(feature = "backtest"))]
                                    consolidator,
                                    #[cfg(feature = "backtest")]
                                    price_supplier,
                                ) {
                                    Ok(plan) => plan,
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to build FX funding plan for Strategy ({}): {e}",
                                            strategy_name_orig
                                        );
                                        FxFundingPlan::default()
                                    }
                                }
                            }
                        }
                    );

                    for batch in fx_plan.order_batches {
                        let pool = self.pool.clone();
                        #[cfg(not(feature = "backtest"))]
                        let weak_client_cloned = weak_client.clone();
                        let orders: VecDeque<OrderIBKR> = batch.into();

                        let res = self.on_new_qty_diff_for_strat(
                            pool,
                            #[cfg(not(feature = "backtest"))]
                            weak_client_cloned,
                            #[cfg(feature = "backtest")]
                            submitter,
                            orders,
                            order_store,
                        );

                        if let Err(e) = res {
                            tracing::error!(
                                "Failed to run on_new_qty_diff_for_strat in handle_bar_update_outcome: {e:?}"
                            );
                        };
                    }

                    // Standalone FX conversions: submitted directly, never as a
                    // strategy head, so they cannot trigger the book-wide
                    // cancel/resize logic.
                    for order in fx_plan.root_orders {
                        Self::place_orders(
                            self.tokio_handle.clone(),
                            self.pool.clone(),
                            #[cfg(not(feature = "backtest"))]
                            &weak_client,
                            #[cfg(feature = "backtest")]
                            submitter,
                            std::iter::once(order),
                        );
                    }

                    if !fx_plan.parked_orders.is_empty() {
                        if let Err(e) = order_store
                            .insert_or_update_orders(&strategy_name_orig, &fx_plan.parked_orders)
                        {
                            tracing::error!(
                                "Failed to park orders in order_store in handle_bar_update_outcome: {e:?}"
                            );
                        };
                    }
                }
            }
            BarUpdateOutcome::NoAction => {}
        }
    }

    /// Reconciles one funding batch against the working orders for its head's
    /// `order_ref`.
    ///
    /// `orders` is the head followed by any legs; the head's `order_ref` is the
    /// lookup key, which is the bare strategy for closings and FX roots but
    /// `strategy:price` for opening heads.
    ///
    /// The target is signed (`+` buy, `-` sell). The current position implied by
    /// working orders is `(Σ|quantity| − Σfilled) × sign(Σquantity)`, so fills
    /// already booked count toward the target and are not re-submitted.
    ///
    /// # Branches
    ///
    /// - target `0` — cancel everything (the position is already correct);
    /// - opposite sign, or `|current| > |target|` — cancel everything and submit
    ///   the full target again;
    /// - `|current| < |target|` — keep the working orders and submit only the
    ///   residual;
    /// - equal — do nothing.
    ///
    /// A mix of buy and sell working orders under one key is logged as an error
    /// but not rejected; the signed sum is still used.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the batch is empty or the working orders cannot be
    /// read.
    ///
    /// # Blocking
    ///
    /// Reads through [`tokio::runtime::Handle::block_on`]; call it off the async
    /// runtime.
    pub fn on_new_qty_diff_for_strat<
        #[cfg(not(feature = "backtest"))] OrderBroker: OrderRouter + Send + Sync + 'static,
    >(
        &self,
        pool: PgPool,

        #[cfg(not(feature = "backtest"))] weak_client: Weak<OrderBroker>,
        #[cfg(feature = "backtest")] submitter: &dyn crate::backtester::execution::OrderSubmitter,

        mut orders: VecDeque<OrderIBKR>,
        order_store: &OrderStore,
    ) -> Result<(), String> {
        let order_ibkr_opt = orders.pop_front();
        if order_ibkr_opt.is_none() {
            return Err("Expected orders not to be empty!".to_string());
        }
        let order_ibkr = order_ibkr_opt.unwrap();
        let contract = order_ibkr.contract;
        let qty_diff = order_ibkr.order.total_quantity * {
            if order_ibkr.order.action == Action::Sell {
                -1.0
            } else {
                1.0
            }
        };
        let asset_type = AssetType::from_str(&contract.security_type);
        let mut order = order_ibkr.order;

        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut open_orders_crud = db.open_orders(&asset_type);
        let open_orders = {
            let mut open_orders_db = match hotpath::measure_block!("get_orders_for_strat", {
                self.tokio_handle.block_on(async {
                    #[cfg(feature = "backtest")]
                    let res = open_orders_crud
                        .get_orders_for_strat(&order.order_ref, BacktestMode::Db)
                        .await;
                    #[cfg(not(feature = "backtest"))]
                    let res = open_orders_crud
                        .get_orders_for_strat(&order.order_ref)
                        .await;
                    res
                })
            }) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("Failed to get orders for strategy: {e:?}");
                    return Err("Failed to get orders for strategy".to_string());
                }
            }
            .into_iter()
            .map(|open_order| LocalOpenOrder {
                order: open_order,
                is_from_db: true,
            })
            .collect::<Vec<LocalOpenOrder>>();

            if let Some(backed_up_orders) = order_store
                .load_orders(&order.order_ref)
                .expect("Expected load_orders function to work")
            {
                backed_up_orders.iter().for_each(|order| {
                    open_orders_db.push(LocalOpenOrder {
                        order: OpenOrdersFullKeys::from_contract_and_order(
                            &order.contract,
                            &order.order,
                            0.0,
                        ),
                        is_from_db: false,
                    });
                });
            }

            open_orders_db
        };

        let tot_qty_dir = open_orders
            .iter()
            .map(|open_order| match open_order.order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => {
                    quantity.signum()
                }
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => {
                    quantity.signum()
                }
            })
            .sum::<f64>()
            .abs() as u64;
        if tot_qty_dir != open_orders.len() as u64 {
            tracing::error!(
                message=%format!(
                    "Error: Open orders placed for {} for stock {} are not all in the same direction!",
                    &order.order_ref, &contract.symbol
                )
            );
        };

        let (curr_open_orders_filled, curr_open_orders_quantity): (f64, f64) = (
            open_orders
                .iter()
                .map(|open_order| match open_order.order {
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { filled, .. }) => filled,
                    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { filled, .. }) => filled,
                })
                .sum(),
            open_orders
                .iter()
                .map(|open_order| match open_order.order {
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => quantity,
                    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => {
                        quantity
                    }
                })
                .sum(),
        );

        // return 1 entry
        let current_qty_diff = (curr_open_orders_quantity.abs() - curr_open_orders_filled)
            * (curr_open_orders_quantity.signum());

        // Alr correct
        // i.e. cancel all open orders (since positions are alr correct)
        if qty_diff == 0.0 {
            // Cancel Open Orders
            #[cfg(not(feature = "backtest"))]
            self.cancel_orders(&asset_type, &weak_client, open_orders, order_store);
            #[cfg(feature = "backtest")]
            self.cancel_orders(&asset_type, submitter, open_orders, order_store);
            return Ok(());
        }

        // Cancel the order if qty_diff is in wrong direction / open order qty too high
        if current_qty_diff.signum() != qty_diff.signum()
            || (current_qty_diff.signum() == qty_diff.signum()
                && current_qty_diff.abs() > qty_diff.abs())
        {
            // Cancel all open orders first
            #[cfg(not(feature = "backtest"))]
            self.cancel_orders(&asset_type, &weak_client, open_orders, order_store);
            #[cfg(feature = "backtest")]
            self.cancel_orders(&asset_type, submitter, open_orders, order_store);

            orders.push_front(OrderIBKR::new(contract, order, -1));
            let handle = self.tokio_handle.clone();
            let pool = self.pool.clone();
            #[cfg(not(feature = "backtest"))]
            {
                let weak_client_cloned = weak_client;
                std::thread::spawn(move || {
                    Self::place_orders(handle, pool, &weak_client_cloned, orders.into_iter())
                });
            }
            #[cfg(feature = "backtest")]
            Self::place_orders(handle, pool, submitter, orders.into_iter());

            return Ok(());
        }

        // If it's here: Order is in same dirction of qty_diff
        // i.e. no need to cancel open orders
        if current_qty_diff.abs() < qty_diff.abs() {
            let handle = self.tokio_handle.clone();
            let pool = self.pool.clone();
            order.total_quantity = (qty_diff - current_qty_diff).abs();
            orders.push_front(OrderIBKR::new(contract, order, -1));
            #[cfg(not(feature = "backtest"))]
            {
                let weak_client_cloned = weak_client;
                std::thread::spawn(move || {
                    Self::place_orders(handle, pool, &weak_client_cloned, orders.into_iter())
                });
            }
            #[cfg(feature = "backtest")]
            Self::place_orders(handle, pool, submitter, orders.into_iter());
        }

        Ok(())
    }
}

/// A `CASH:<ccy>` row carries funding capacity, not an order.
///
/// Funding rows are always included in the plan's inputs so an option opening
/// can draw the same cash as a stock opening; they never become orders.
pub fn is_cash_diff(diff: &TargetPositionsQtyDiff) -> bool {
    matches!(diff, TargetPositionsQtyDiff::Stock(v) if v.stock.starts_with("CASH:"))
}

/// Whether the working orders leave any target uncovered.
///
/// Sums the signed quantity of every working order per contract, then compares
/// each target diff with that sum: any non-zero difference means the book must
/// be reconciled. `CASH:` rows are ignored for non-FX strategies (they are
/// budget, not inventory).
///
/// Used by [`OrderEngine::handle_bar_update_outcome`] to skip asset classes
/// whose targets are already fully represented by resting orders.
pub fn requires_reconciliation(
    target_pos_diffs: &Vec<TargetPositionsQtyDiff>,
    open_orders: Vec<LocalOpenOrder>,
    strategy_detail: &StrategyDetails,
) -> bool {
    let mut orders_map: HashMap<HashContract<'static>, f64> = HashMap::new();
    for order in open_orders.into_iter() {
        let qty = order.order.get_qty();
        let contract = get_contract_from(&LocalContractTypes::OpenOrders(order.order));
        let hash_contract = HashContract::new(contract);
        orders_map
            .entry(hash_contract)
            .and_modify(|v| {
                *v += qty;
            })
            .or_insert(qty);
    }

    target_pos_diffs.into_iter().any(|pos_diff| {
        // Skip if "CASH:..." position diff for non-FX strategies
        if !strategy_detail.is_fx_strategy && pos_diff.get_stock().strip_prefix("CASH:").is_some() {
            return false;
        }

        let contract = get_contract_from(&LocalContractTypes::TargetPosQtyDiff(pos_diff.clone()));
        let hash_contract = HashContract::borrowed(&contract);
        let open_orders_qty = orders_map.get(&hash_contract).unwrap_or(&0.0);
        let diff = pos_diff.get_qty_diff() - *open_orders_qty;

        if diff == 0.0 { false } else { true }
    })
}

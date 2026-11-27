//! Application of a fill.
//!
//! One `ExecutionData` event becomes one database transaction that
//! 1. rejects duplicates,
//! 2. locks and updates (or deletes) the matching open order,
//! 3. inserts the transaction row,
//! 4. applies the signed quantity to the instrument position,
//! 5. applies the signed cash to the `CASH:<currency>` position.
//!
//! # Semantics
//!
//! - `filled` records IBKR's `cumulative_quantity` (not the local sum of
//!   shares), so a reconnect that skipped intermediate fills still converges.
//!   The position moves by the execution's `shares` only.
//! - Quantity signs come from the execution side (`"BOT"` = positive,
//!   everything else negative) and match
//!   [`TransactionsFullKeys`].
//! - Cash uses the opposite sign: a buy consumes cash.
//! - A full fill (`cumulative_quantity == |quantity|`) deletes the open order;
//!   a partial fill updates it and appends the execution id.
//! - For non-FX strategies, a position whose stock is `FX:<from>:<to>` is
//!   rewritten to `CASH:<from>` so FX conversions land in the cash book.
//! - Strategy hooks are forwarded **after** the commit, so a hook can never
//!   observe uncommitted rows.
//! - Parked orders are released **after** the commit, in a separate task; a
//!   failure there re-parks the order and cannot roll back the fill.
//!
//! # Duplicate handling
//!
//! The duplicate check reads by transaction primary key before the insert. That
//! is safe because the open-order row is locked with `read_for_update`, which
//! serialises two handlers racing on the same order; a duplicate that slips
//! past the read fails the primary key on insert and rolls the transaction back.

use crate::{
    database::tables::DbTables,
    execution::traits::order_router::OrderRouter,
    market_data::traits::{contracts::ContractBroker, current_price::PriceSupplier},
};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use ibapi::orders::ExecutionData;
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferProducer;
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersUpdateKeys,
            OpenStockOrdersFullKeys, OpenStockOrdersUpdateKeys,
        },
        models_crud::{
            current_positions::current_positions::{
                CurrentPositionsOps, CurrentPositionsPrimaryKeys, CurrentPositionsUpdateKeys,
            },
            open_orders::open_orders::{
                OpenOrdersFullKeys, OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys,
            },
            transactions::transactions::{TransactionsFullKeys, TransactionsPrimaryKeys},
        },
    },
    execution::{
        fx_backed_up_order::OrderStore,
        order_engine::{OrderEngine, OrderIBKR},
    },
    strategy::strategy::StrategyDetails,
};

/// Applies one fill to the database.
///
/// This is the sink for every execution event produced by the order-update
/// stream and by the startup syncer. Validation runs synchronously; all
/// database work runs in a task spawned on `handle`, so the function returns
/// before any row is written.
///
/// # Arguments
///
/// - `consolidator` — market-data broker used only to canonicalise the
///   contract via [`ContractBroker::get_validated_contract`]. The validated
///   contract replaces `execution_data.contract`, so every key built later uses
///   the canonical security type. A dropped weak reference is an error, never a
///   silent skip.
/// - `pool` — database pool moved into the spawned task. The task begins one
///   `DbTxn` per fill, so the duplicate check, open-order write, transaction
///   insert and both position upserts commit or roll back together.
/// - `execution_data` — the fill as IBKR reported it. `execution.order_reference`
///   names the strategy; the other execution fields drive the transaction row,
///   the position deltas and the open order's fill counter.
/// - `execution_events_queue` — per-strategy ring buffers. When the resolved
///   strategy has a producer and `hooks_execution` is set, the fill is pushed
///   there *after* the commit, so hooks only ever observe applied fills. A
///   missing producer is not an error.
/// - `strategy_details` — the configured strategies. The resolved strategy's
///   entry decides whether the execution hook fires; an unknown strategy falls
///   back to the `default_strategy` entry.
/// - `default_strategy` — used as the strategy name when the execution carries
///   no `order_reference` (manual orders), and as the configuration fallback.
/// - `handle` — runtime used to spawn the database task. The caller does not
///   await it.
/// - `weak_client` — order router (the IB client) used after the commit to
///   release parked orders. It is weak because the task may outlive the client;
///   a dead client only disables that release.
/// - `backed_up_orders` — the redb park store. After the commit the positions
///   touched by the fill are re-checked and any parked order the new balance
///   affords is placed and removed.
///
/// # Transaction
///
/// The task first exits silently when a transaction with the same
/// `execution_id` already exists. It then locks the matching open order with
/// `read_for_update` so concurrent fills for the same order serialise. With no
/// open order it records the fill and the additive position deltas; with one it
/// updates or deletes the order (a full fill deletes it) and records the same
/// rows. Any write failure is logged and rolls the transaction back.
///
/// # Errors
///
/// - The consolidator has been dropped.
/// - The contract is not recognised.
/// - The contract maps to `AssetType::CASH` or `AssetType::Unknown` (neither
///   can be held).
///
/// # Panics
///
/// Panics if `default_strategy` is absent from `strategy_details` and the
/// resolved strategy name is unknown, because there is then no fallback
/// configuration to apply the fill with.
pub fn on_execution_update<MarketBroker, OrderBroker>(
    consolidator: &Weak<MarketBroker>,
    pool: PgPool,
    mut execution_data: ExecutionData,
    execution_events_queue: Arc<HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>>,
    strategy_details: &Arc<HashMap<String, StrategyDetails>>,
    default_strategy: &str,
    handle: tokio::runtime::Handle,
    weak_client: &Weak<OrderBroker>,
    backed_up_orders: Arc<OrderStore>,
) -> Result<(), String>
where
    MarketBroker: PriceSupplier + ContractBroker,
    OrderBroker: OrderRouter + Send + Sync + 'static,
{
    let contract = {
        consolidator
            .upgrade()
            .ok_or("Consolidator died when received submitted order update")?
            .get_validated_contract(execution_data.contract.clone())
            .ok_or("No valid contract for contract for order update")?
    };
    execution_data.contract = contract;
    let asset_type = AssetType::from_str(&execution_data.contract.security_type);
    match asset_type {
        AssetType::CASH => {
            tracing::error!(
                "New Order received for CASH asset - need to identify why ({}, {})",
                execution_data.contract.symbol,
                execution_data.contract.security_type
            );
            return Err("ERORORKOKEOKRK".to_string());
        }
        AssetType::Unknown => {
            tracing::error!(
                message=%format!(
                    "New Execution: Unknown security type encountered in system for symbol {}: {}",
                    execution_data.contract.symbol,
                    execution_data.contract.security_type
                )
            );
            return Err("ERORORKOKEOKRK".to_string());
        }
        _ => {}
    }

    let strategy_name = if execution_data.execution.order_reference != "" {
        execution_data.execution.order_reference.clone()
    } else {
        default_strategy.to_string()
    };

    let strategy_detail = strategy_details
        .get(&strategy_name)
        .unwrap_or(
            strategy_details
                .get(default_strategy)
                .expect("Expected default strategy to be defined in strategy map"),
        )
        .clone();

    let weak_client = weak_client.clone();
    let cloned_handle = handle.clone();
    handle.spawn(async move {
        let mut db = match crate::database::db::DbTxn::begin(&pool).await {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to begin execution transaction: {e:?}");
                return;
            }
        };

        // Check if transaction is valid - not a duplicate.
        let transaction_pk = TransactionsPrimaryKeys::from(&execution_data);
        match db.transactions(&asset_type).read(&transaction_pk).await {
            Ok(Some(_)) => {
                tracing::debug!("Already processed duplicate transaction");
                return;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("{e:?}");
                return;
            }
        }

        // Read Associated Open Order
        tracing::info!(
            message=%format!("Execution: Looking for order with order_id {}",
            &execution_data.execution.order_id)
        );
        let open_order_pk = OpenOrdersPrimaryKeys::from_execution(&execution_data);
        let open_order_opt = match db.open_orders(&asset_type).read_for_update(&open_order_pk).await {
            Ok(val) => val,
            Err(e) => {
                tracing::error!("Failed to read open orders: {e:?}");
                return;
            }
        };

        // If No previous Open Order
        if open_order_opt.is_none() {
            tracing::warn!("OpenStockOrders does not contain required row");
            let transaction_fk =
                TransactionsFullKeys::from_strat_and_exec(&strategy_name, &execution_data);
            let currency_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract_currency(
                &strategy_name,
                &execution_data.contract,
            );
            let currency_uk = CurrentPositionsUpdateKeys::from_execution_currency(&execution_data);
            let current_positions_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract(
                &strategy_name,
                &execution_data.contract,
            );
            let current_positions_uk = CurrentPositionsUpdateKeys::from_execution(&execution_data);
            let placement_pk = currency_pk.clone();

            let write_res: Result<(), String> = async {
                db.transactions(&asset_type).create(&transaction_fk).await?;
                update_position(
                    &mut db,
                    &asset_type,
                    current_positions_pk,
                    current_positions_uk,
                )
                .await?;
                update_position(&mut db, &asset_type, currency_pk, currency_uk).await?;
                Ok(())
            }
            .await;

            if let Err(e) = write_res {
                tracing::error!("Failed to apply execution transaction: {e:?}");
                return;
            }
            if let Err(e) = db.commit().await {
                tracing::error!("Failed to commit execution transaction: {e:?}");
                return;
            }

            if strategy_detail.hooks_execution {
                forward_execution_hook(&execution_events_queue, &strategy_name, &execution_data);
            }

            tokio::spawn(async move {
                place_backed_up_orders(
                    cloned_handle,
                    pool,
                    &weak_client,
                    asset_type,
                    placement_pk,
                    backed_up_orders,
                    strategy_name.as_str(),
                ).await;
            });
            return;
        }

        // By here, there is an incoming execution + a matching open order.
        let open_order = open_order_opt.unwrap();
        let (mut executions, filled, quantity, strategy_name, stock) = match &open_order {
            &OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                ref executions,
                filled,
                quantity,
                ref strategy,
                ref stock,
                ..
            }) => (
                executions.clone(),
                filled,
                quantity,
                strategy.clone(),
                stock.clone(),
            ),
            &OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                ref executions,
                filled,
                quantity,
                ref strategy,
                ref stock,
                ..
            }) => (
                executions.clone(),
                filled,
                quantity,
                strategy.clone(),
                stock.clone(),
            ),
        };

        let open_order_pk = OpenOrdersPrimaryKeys::from_open_order(&open_order);
        let open_order_uk = if execution_data.execution.cumulative_quantity == quantity.abs() {
            None
        } else {
            if executions.contains(&execution_data.execution.execution_id) {
                tracing::warn!("Open Order found but execution already recorded");
                return;
            }
            executions.push(execution_data.execution.execution_id.to_string());

            if filled
                != execution_data.execution.cumulative_quantity - execution_data.execution.shares
            {
                tracing::error!(
                    message=%format!("New Execution: Cumulative Quantity does not coincide with locally tracked filled quantity (Cumulative: {}, Locally Tracked: {})",
                    execution_data.execution.cumulative_quantity
                        - execution_data.execution.shares,
                    filled
                    )
                );
            }

            Some(match &open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    time,
                    quantity,
                    filled,
                    ..
                }) => OpenOrdersUpdateKeys::Stock(OpenStockOrdersUpdateKeys {
                    strategy: None,
                    stock: None,
                    primary_exchange: None,
                    currency: None,
                    time: Some(*time),
                    quantity: Some(*quantity),
                    parent_order_id: None,
                    executions: Some(executions),
                    filled: Some(filled.max(execution_data.execution.cumulative_quantity)),
                }),
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                    time,
                    quantity,
                    filled,
                    ..
                }) => OpenOrdersUpdateKeys::Options(OpenOptionOrdersUpdateKeys {
                    strategy: None,
                    stock: None,
                    primary_exchange: None,
                    currency: None,
                    expiry: None,
                    strike: None,
                    multiplier: None,
                    option_type: None,
                    time: Some(*time),
                    quantity: Some(*quantity),
                    parent_order_id: None,
                    executions: Some(executions),
                    filled: Some(filled.max(execution_data.execution.cumulative_quantity)),
                }),
            })
        };

        let transaction_fk =
            TransactionsFullKeys::from_strat_and_exec(&strategy_name, &execution_data);
        let mut current_positions_pk = CurrentPositionsPrimaryKeys::from_open_order(&open_order);
        let stock = if strategy_detail.is_fx_strategy {
            stock
        } else {
            match stock.strip_prefix("FX:") {
                Some(currencies) => format!("CASH:{}", currencies.split(":").next().unwrap()),
                None => stock,
            }
        };
        current_positions_pk.with_stock(&stock);
        let current_positions_uk = CurrentPositionsUpdateKeys::from_execution(&execution_data);
        let currency_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract_currency(
            &strategy_name,
            &execution_data.contract,
        );
        let currency_uk = CurrentPositionsUpdateKeys::from_execution_currency(&execution_data);
        let placement_pk = currency_pk.clone();

        let write_res: Result<(), String> = async {
            match &open_order_uk {
                Some(uk) => {
                    db.open_orders(&asset_type)
                        .update(&open_order_pk, uk)
                        .await?;
                }
                None => {
                    db.open_orders(&asset_type).delete(&open_order_pk).await?;
                }
            }
            db.transactions(&asset_type).create(&transaction_fk).await?;
            update_position(
                &mut db,
                &asset_type,
                current_positions_pk,
                current_positions_uk,
            )
            .await?;
            update_position(&mut db, &asset_type, currency_pk, currency_uk).await?;
            Ok(())
        }
        .await;

        if let Err(e) = write_res {
            tracing::error!("Failed to apply execution transaction: {e:?}");
            return;
        }
        if let Err(e) = db.commit().await {
            tracing::error!("Failed to commit execution transaction: {e:?}");
            return;
        }

        if strategy_detail.hooks_execution {
            forward_execution_hook(&execution_events_queue, &strategy_name, &execution_data);
        }

        tokio::spawn(async move {
            place_backed_up_orders(
                cloned_handle,
                pool,
                &weak_client,
                asset_type,
                placement_pk,
                backed_up_orders,
                strategy_name.as_str(),
            ).await;
        });
    });
    Ok(())
}

/// Applies an additive position change inside an open transaction.
///
/// Delegates to
/// [`CurrentPositionsOps::update_positions_additive`](crate::database::models_crud::current_positions::current_positions::CurrentPositionsOps),
/// passing [`BacktestMode::Db`](crate::backtester::setup::config::BacktestMode)
/// when the `backtest` feature is on so the SQL path (rather than the in-memory
/// book) is used.
async fn update_position(
    db: &mut crate::database::db::DbTxn,
    asset_type: &AssetType,
    pk: CurrentPositionsPrimaryKeys,
    uk: CurrentPositionsUpdateKeys,
) -> Result<(), String> {
    let mut current_positions_crud = db.current_positions(asset_type);
    #[cfg(feature = "backtest")]
    {
        current_positions_crud
            .update_positions_additive(pk, uk, BacktestMode::Db)
            .await
    }
    #[cfg(not(feature = "backtest"))]
    {
        current_positions_crud
            .update_positions_additive(pk, uk)
            .await
    }
}

/// Releases parked orders that the currency balance now covers.
///
/// Runs after the fill commits. Reads the `CASH:<currency>` position, does
/// nothing when it is non-positive, asks the [`OrderStore`] for as many parked
/// orders as the balance affords, and submits each with the bare strategy as
/// `order_ref`. Parked copies keep their `{strategy}:{price}` key, so a failed
/// placement is re-parked unchanged and retried on a later fill.
///
/// # Panics
///
/// Panics if the currency row disappears between the fill commit and this read,
/// or if the primary key's stock lacks the `CASH:` prefix — both indicate the
/// caller passed a position key that is not a currency row.
#[allow(clippy::too_many_arguments)]
async fn place_backed_up_orders<OrderBroker>(
    handle: tokio::runtime::Handle,
    pool: PgPool,
    weak_client: &Weak<OrderBroker>,
    asset_type: AssetType,
    pk: CurrentPositionsPrimaryKeys,
    backed_up_orders: Arc<OrderStore>,
    strategy_name: &str,
) where
    OrderBroker: OrderRouter,
{
    let mut db = crate::database::db::DbDirect::connect(&pool);
    let mut current_positions_crud = db.current_positions(&asset_type);
    let current_positions_currency: Option<
        crate::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys,
    > = match current_positions_crud.read(&pk).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                "Failed to fetch current currency position, ignoring backed_up_orders! Err: {e:?}"
            );
            return;
        }
    };

    let currency_value = current_positions_currency
        .expect("Expected current positions of currency not to be None")
        .get_qty();
    if currency_value <= 0.0 {
        return;
    }

    let currency = pk
        .get_stock()
        .strip_prefix("CASH:")
        .expect("Expected currency stock to have CASH: prefix")
        .to_string();

    let ready = match backed_up_orders.remove_all_valid_orders(
        strategy_name,
        &currency,
        currency_value,
    ) {
        Ok(ready) => ready,
        Err(e) => {
            tracing::error!(
                "Failed to take placeable orders from backed_up_orders for {strategy_name}: {e:?}"
            );
            return;
        }
    };

    let mut failed: Vec<OrderIBKR> = Vec::new();
    for strat_order in ready {
        let mut to_place = strat_order.clone();
        // The parked copy keeps its "{strategy}:{price}" suffix so a failed placement can be
        // re-parked and re-priced; only the submitted order carries the plain strategy.
        to_place.order.order_ref = strategy_name.to_string();
        let order_id =
            OrderEngine::place_order(handle.clone(), pool.clone(), weak_client, to_place);
        if order_id == -1 {
            failed.push(strat_order);
        }
    }
    if !failed.is_empty() {
        if let Err(e) = backed_up_orders.insert_or_update_orders(strategy_name, &failed) {
            tracing::error!("Failed to re-park orders after failed placement: {e:?}");
        }
    }
}

/// Pushes the fill into the strategy's execution ring.
///
/// Non-blocking: `try_push` never waits for the consumer, so a full or missing
/// ring only logs. An unknown strategy name (no ring registered) is warned and
/// dropped — the database write is already committed at this point.
fn forward_execution_hook(
    execution_events_queue: &Arc<HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>>,
    strategy_name: &str,
    execution_data: &ExecutionData,
) {
    match execution_events_queue.get(strategy_name) {
        Some(execution_events_producer) => {
            if let Err(e) = execution_events_producer.try_push(execution_data.clone()) {
                tracing::error!("Failed to push execution event for {strategy_name}: {e:?}");
            };
        }
        None => {
            let symbol = &execution_data.contract.symbol;
            tracing::warn!("Unknown strategy's order received: {strategy_name}, {symbol}");
        }
    }
}

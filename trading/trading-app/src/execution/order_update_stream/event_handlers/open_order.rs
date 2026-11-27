//! Acknowledged and cancelled open orders.
//!
//! The order engine inserts an *optimistic* row under the synthetic perm id
//! `-1` when it submits an order. IBKR later echoes the real order back; these
//! handlers replace the optimistic row with the acknowledged one, or remove both
//! when the order is cancelled.
//!
//! | Function | Trigger | Effect (one transaction) |
//! |---|---|---|
//! | [`submitted`] | `OpenOrder` with status `Submitted` / `PreSubmitted` | delete `(-1, order_id)`, `create_or_ignore` the real row |
//! | [`cancelled`] | `OrderStatus` cancelled-like | delete `(perm_id, order_id)` and `(-1, order_id)` |
//!
//! The engine's optimistic insert and these handlers are the two halves of the
//! `(order_perm_id = -1)` convention documented in
//! [`crate::execution::order_engine`].

use crate::{database::tables::DbTables, market_data::traits::contracts::ContractBroker};
use std::sync::Weak;

use ibapi::{
    contracts::{Contract, SecurityType},
    orders::Order,
};
use sqlx::PgPool;

use crate::database::{
    crud::CRUDTrait,
    db::DbTxn,
    models::AssetType,
    models_crud::open_orders::open_orders::{OpenOrdersFullKeys, OpenOrdersPrimaryKeys},
};

/// Replaces the optimistic open-order row with the one IBKR acknowledged.
///
/// Validates the contract through `consolidator` first, then spawns a task that
/// deletes `(order_perm_id = -1, order_id)` and inserts the acknowledged row in
/// a single transaction. The insert is `create_or_ignore`, so a repeated event
/// keeps the first acknowledged row; the delete is idempotent.
///
/// Returns the spawned task so the caller can await the database work (and
/// surface errors through the join result). Asset-class selection follows
/// [`AssetType::from_str`](crate::database::models::AssetType) on the contract's
/// security type.
///
/// # Errors
///
/// - The consolidator has been dropped.
/// - The contract is not recognised by the consolidator.
///
/// Database failures happen inside the spawned task and are logged, not
/// returned; the task returns without committing, so the transaction rolls back.
///
/// # Panics
///
/// Panics when the primary keys cannot address an open-order table (`CASH` or
/// `Unknown` asset types).
pub fn submitted<MarketBroker>(
    consolidator: &Weak<MarketBroker>,
    pool: PgPool,
    contract: &Contract,
    order: &Order,
) -> Result<tokio::task::JoinHandle<()>, String>
where
    MarketBroker: ContractBroker,
{
    let contract = {
        consolidator
            .upgrade()
            .ok_or("Consolidator died when received submitted order update")?
            .get_validated_contract(contract.clone())
            .ok_or("No valid contract for contract for order update")?
    };
    let asset_type = AssetType::from_str(&contract.security_type);
    let open_order_fk = OpenOrdersFullKeys::from_contract_and_order(&contract, order, 0.0);
    let open_orders_pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, order.order_id);
    Ok(tokio::spawn(async move {
        let mut db = match DbTxn::begin(&pool).await {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to begin submitted open-order transaction: {e:?}");
                return;
            }
        };
        if let Err(e) = db.open_orders(&asset_type).delete(&open_orders_pk).await {
            tracing::error!("Failed to delete received open order from DB: {e:?}");
            return;
        }
        if let Err(e) = db
            .open_orders(&asset_type)
            .create_or_ignore(&open_order_fk)
            .await
        {
            tracing::error!("Error occured while inserting into OpenStockOrders: {e:?}");
            return;
        }
        if let Err(e) = db.commit().await {
            tracing::error!("Failed to commit submitted open-order transaction: {e:?}");
        }
    }))
}

/// Removes an order from the open-order table after a cancellation.
///
/// Spawns a task that deletes both the acknowledged row `(perm_id, order_id)`
/// and the optimistic row `(-1, order_id)` in one transaction. Both deletes are
/// idempotent, so repeated or out-of-order cancellations are harmless.
///
/// There is no caller in this repository today; the order-status handler owns
/// the cancellation path that IBKR actually drives.
///
/// # Panics
///
/// Panics when the security type maps to `CASH` or `Unknown`, which cannot
/// address an open-order table.
pub fn cancelled(pool: PgPool, order_id: i32, order_perm_id: i32, security_type: &SecurityType) {
    let asset_type = AssetType::from_str(security_type);
    let open_orders_pk = OpenOrdersPrimaryKeys::new(&asset_type, order_perm_id, order_id);
    let open_orders_pk_fake = OpenOrdersPrimaryKeys::new(&asset_type, -1, order_id);

    tokio::spawn(async move {
        let mut db = match DbTxn::begin(&pool).await {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to begin cancelled open-order transaction: {e:?}");
                return;
            }
        };
        if let Err(e) = db.open_orders(&asset_type).delete(&open_orders_pk).await {
            tracing::error!("Failed to cancel Open Order: {e:?}");
            return;
        }
        if let Err(e) = db
            .open_orders(&asset_type)
            .delete(&open_orders_pk_fake)
            .await
        {
            tracing::error!("Failed to cancel Optimistic Open Order: {e:?}");
            return;
        }
        if let Err(e) = db.commit().await {
            tracing::error!("Failed to commit cancelled open-order transaction: {e:?}");
        }
    });
}

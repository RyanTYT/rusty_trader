//! Order-status bookkeeping.
//!
//! `orderStatus` callbacks are the lightest source of truth: they arrive often
//! and may precede or follow the corresponding `openOrder` event. The handlers
//! therefore never assert the order exists.
//!
//! | Function | Trigger | Effect |
//! |---|---|---|
//! | [`submitted`] | `Submitted` | read-only comparison of the recorded fill and quantity; logs discrepancies |
//! | [`cancelled`] | `PendingCancel`, `Inactive`, `ApiCancelled`, `Cancelled` | deletes the row from the stock and option tables |
//!
//! Neither handler opens a transaction: the reads are independent and each
//! delete is a single atomic statement with no cross-row invariant.

use crate::database::tables::DbTables;
use ibapi::orders::OrderStatus;
use sqlx::PgPool;

use crate::database::{
    crud::CRUDTrait,
    models::{AssetType, OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys},
    models_crud::open_orders::open_orders::{OpenOrdersFullKeys, OpenOrdersPrimaryKeys},
};

/// Compares the broker's fill/quantity with the recorded open order.
///
/// Read-only and fire-and-forget: each asset-class table is read by
/// `(perm_id, order_id)` and any difference between the broker's
/// `filled`/`filled + remaining` and the stored `filled`/`|quantity|` is logged.
/// A missing row is not an error — the status event may arrive before the
/// open-order row.
///
/// # Panics
///
/// Panics if a stock lookup returns an option row or vice versa. That would mean
/// the primary key matched the wrong variant, which the constructors prevent.
pub fn submitted(pool: PgPool, order_status: &OrderStatus) {
    let order_perm_id = order_status.perm_id;
    let order_id = order_status.order_id;
    let (filled, quantity) = (
        order_status.filled,
        order_status.filled + order_status.remaining,
    );

    tokio::spawn(async move {
        let mut db = crate::database::db::DbDirect::connect(&pool);

        {
            let mut open_orders_crud = db.open_orders(&AssetType::Stock);
            let open_order_pk =
                OpenOrdersPrimaryKeys::new(&AssetType::Stock, order_perm_id, order_id);

            match open_orders_crud.read(&open_order_pk).await {
                Ok(open_order_opt) => {
                    if let Some(open_order) = open_order_opt {
                        let (recorded_filled, recorded_quantity) = match open_order {
                            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                                filled,
                                quantity,
                                ..
                            }) => (filled, quantity),
                            OpenOrdersFullKeys::Options(_) => panic!("Shouldn't get Option type"),
                        };
                        if filled != recorded_filled || quantity != recorded_quantity.abs() {
                            tracing::error!(
                                "Dicrepancy in filled/quantity: Recorded ({recorded_filled}/{recorded_quantity}), Broker ({filled}/{quantity})"
                            );
                        }
                    }
                }
                Err(e) => tracing::error!("Failed to read stock open order: {e:?}"),
            };
        }

        {
            let mut open_orders_crud = db.open_orders(&AssetType::Option);
            let open_order_pk =
                OpenOrdersPrimaryKeys::new(&AssetType::Option, order_perm_id, order_id);

            match open_orders_crud.read(&open_order_pk).await {
                Ok(open_order_opt) => {
                    if let Some(open_order) = open_order_opt {
                        let (recorded_filled, recorded_quantity) = match open_order {
                            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                                filled,
                                quantity,
                                ..
                            }) => (filled, quantity),
                            OpenOrdersFullKeys::Stock(_) => panic!("Shouldn't get Option type"),
                        };
                        if filled != recorded_filled || quantity != recorded_quantity.abs() {
                            tracing::error!(
                                "Dicrepancy in filled/quantity: Recorded ({recorded_filled}/{recorded_quantity}), Broker ({filled}/{quantity})"
                            );
                        }
                    }
                }
                Err(e) => tracing::error!("Failed to read option open order: {e:?}"),
            };
        }
    });
}

/// Deletes the order from both open-order tables.
///
/// Spawns a task that attempts the stock delete and then the option delete; an
/// order exists in at most one of them, so one delete normally reports zero
/// rows and the other removes the row. Failures are logged and do not roll back
/// the successful delete.
///
/// # Panics
///
/// Panics when `perm_id`/`order_id` cannot address an open-order table (the
/// constructors reject `CASH`/`Unknown`); the asset types here are fixed.
pub fn cancelled(pool: PgPool, order_status: &OrderStatus) {
    let (order_perm_id, order_id) = (order_status.perm_id, order_status.order_id);

    tokio::spawn(async move {
        let mut db = crate::database::db::DbDirect::connect(&pool);

        {
            let mut open_orders_crud = db.open_orders(&AssetType::Stock);
            let open_orders_pk =
                OpenOrdersPrimaryKeys::new(&AssetType::Stock, order_perm_id, order_id);
            if let Err(e) = open_orders_crud.delete(&open_orders_pk).await {
                tracing::error!("Failed to cancel stock Open Order: {e:?}");
            }
        }

        {
            let mut open_orders_crud = db.open_orders(&AssetType::Option);
            let open_orders_pk =
                OpenOrdersPrimaryKeys::new(&AssetType::Option, order_perm_id, order_id);
            if let Err(e) = open_orders_crud.delete(&open_orders_pk).await {
                tracing::error!("Failed to cancel option Open Order: {e:?}");
            }
        }
    });
}

use ibapi::orders::OrderStatus;
use sqlx::PgPool;

use crate::database::{
    crud::CRUDTrait,
    models::{AssetType, OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys},
    models_crud::open_orders::open_orders::{
        OpenOrdersCRUD, OpenOrdersFullKeys, OpenOrdersPrimaryKeys,
    },
};

/// Should be triggered by Submitted and PreSubmitted Order Events to update the local OpenOrders
/// table
pub fn submitted(pool: PgPool, order_status: &OrderStatus) {
    // order_status.
    let order_perm_id = order_status.perm_id;
    let order_id = order_status.order_id;
    let (filled, quantity) = (
        order_status.filled,
        order_status.filled + order_status.remaining,
    );

    tokio::spawn(async move {
        let open_orders_crud = OpenOrdersCRUD::from(&AssetType::Stock, pool.clone());
        let open_order_pk = OpenOrdersPrimaryKeys::new(&AssetType::Stock, order_perm_id, order_id);

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
                    if filled != recorded_filled || quantity != recorded_quantity {
                        tracing::error!(
                            "Dicrepancy in filled/quantity: Recorded ({recorded_filled}/{recorded_quantity}), Broker ({filled}/{quantity})"
                        );
                    }
                }
            }
            Err(e) => tracing::error!("Error occured while inserting into OpenStockOrders: {e:?}"),
        };

        let open_orders_crud = OpenOrdersCRUD::from(&AssetType::Option, pool.clone());
        let open_order_pk = OpenOrdersPrimaryKeys::new(&AssetType::Option, order_perm_id, order_id);

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
                    if filled != recorded_filled || quantity != recorded_quantity {
                        tracing::error!(
                            "Dicrepancy in filled/quantity: Recorded ({recorded_filled}/{recorded_quantity}), Broker ({filled}/{quantity})"
                        );
                    }
                }
            }
            Err(e) => tracing::error!("Error occured while inserting into OpenStockOrders: {e:?}"),
        };
    });
}

/// Should be triggered on "Cancelled" or "ApiCancelled"
/// - deletes the associated order in the OpenOrders table
pub fn cancelled(pool: PgPool, order_status: &OrderStatus) {
    let (order_perm_id, order_id) = (order_status.perm_id, order_status.order_id);

    tokio::spawn(async move {
        let open_orders_crud = OpenOrdersCRUD::from(&AssetType::Stock, pool.clone());
        let open_orders_pk = OpenOrdersPrimaryKeys::new(&AssetType::Stock, order_perm_id, order_id);
        if let Err(e) = open_orders_crud.delete(&open_orders_pk).await {
            tracing::error!("Failed to cancel Open Order: {e:?}");
        }

        let open_orders_crud = OpenOrdersCRUD::from(&AssetType::Option, pool.clone());
        let open_orders_pk =
            OpenOrdersPrimaryKeys::new(&AssetType::Option, order_perm_id, order_id);
        if let Err(e) = open_orders_crud.delete(&open_orders_pk).await {
            tracing::error!("Failed to cancel Open Order: {e:?}");
        }
    });
}

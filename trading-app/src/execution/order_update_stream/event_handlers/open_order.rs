use ibapi::{
    contracts::{Contract, SecurityType},
    orders::Order,
};
use sqlx::PgPool;

use crate::database::{
    crud::CRUDTrait,
    models::AssetType,
    models_crud::open_orders::open_orders::{
        OpenOrdersCRUD, OpenOrdersFullKeys, OpenOrdersPrimaryKeys,
    },
};

/// Should be triggered by Submitted and PreSubmitted Order Events to update the local OpenOrders
/// table
pub fn submitted(
    pool: PgPool,
    contract: &Contract,
    order: &Order,
) -> Result<tokio::task::JoinHandle<()>, String> {
    let asset_type = AssetType::from_str(&contract.security_type);
    let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool);
    let open_order_fk = OpenOrdersFullKeys::from_contract_and_order(contract, order, 0.0);
    Ok(tokio::spawn(async move {
        if let Err(e) = open_orders_crud.create_or_ignore(&open_order_fk).await {
            tracing::error!("Error occured while inserting into OpenStockOrders: {e:?}")
        };
    }))
}

/// Should be triggered on "Cancelled" or "ApiCancelled"
/// - deletes the associated order in the OpenOrders table
pub fn cancelled(pool: PgPool, order_id: i32, order_perm_id: i32, security_type: &SecurityType) {
    let asset_type = AssetType::from_str(security_type);
    let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool);
    let open_orders_pk = OpenOrdersPrimaryKeys::new(&asset_type, order_perm_id, order_id);

    tokio::spawn(async move {
        if let Err(e) = open_orders_crud.delete(&open_orders_pk).await {
            tracing::error!("Failed to cancel Open Order: {e:?}");
        }
    });
}

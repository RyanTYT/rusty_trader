use crate::database::tables::DbTables;
use std::{sync::Weak, time::Duration};

use ibapi::{
    contracts::{Contract, SecurityType},
    orders::Order,
};
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        db::DbTxn,
        models::AssetType,
        models_crud::open_orders::open_orders::{OpenOrdersFullKeys, OpenOrdersPrimaryKeys},
    },
    market_data::consolidator::Consolidator,
};

/// Should be triggered by Submitted and PreSubmitted Order Events to update the local OpenOrders
/// table
pub fn submitted(
    consolidator: &Weak<Consolidator>,
    pool: PgPool,
    contract: &Contract,
    order: &Order,
) -> Result<tokio::task::JoinHandle<()>, String> {
    let contract = {
        consolidator
            .upgrade()
            .ok_or("Consolidator died when received submitted order update")?
            .validate_contract(contract.clone(), Duration::from_secs(10))
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

/// Should be triggered on "Cancelled" or "ApiCancelled"
/// - deletes the associated order in the OpenOrders table
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
        if let Err(e) = db.open_orders(&asset_type).delete(&open_orders_pk_fake).await {
            tracing::error!("Failed to cancel Optimistic Open Order: {e:?}");
            return;
        }
        if let Err(e) = db.commit().await {
            tracing::error!("Failed to commit cancelled open-order transaction: {e:?}");
        }
    });
}

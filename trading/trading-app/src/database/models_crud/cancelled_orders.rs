//! `logs.cancelled_orders` — audit trail for cancelled orders.
//!
//! Composite primary key `(time, order_perm_id, order_id)`. `strategy` is a
//! foreign key to `trading.strategy`; `executions` holds the execution ids
//! seen before cancellation and `filled` the cumulative filled quantity at
//! that point. Rows are written by the order engine when it cancels.
//!
//! # Example
//!
//! ```rust
//! use chrono::{DateTime, Utc};
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, Status, StrategyFullKeys,
//!     StrategyPrimaryKeys,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let strategy = format!("doc_cancelled_{}", std::process::id());
//! let time = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
//! let mut db = DbDirect::connect(&pool);
//!
//! db.strategy()
//!     .create_or_ignore(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
//!     .await?;
//! db.cancelled_orders()
//!     .create(&CancelledOrdersFullKeys {
//!         time,
//!         order_perm_id: 9001,
//!         order_id: 1,
//!         strategy: strategy.clone(),
//!         stock: "AAPL".into(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         quantity: 10.0,
//!         executions: Vec::new(),
//!         filled: 0.0,
//!         reason: "doc example".into(),
//!     })
//!     .await?;
//!
//! let pk = CancelledOrdersPrimaryKeys { time, order_perm_id: 9001, order_id: 1 };
//! assert!(db.cancelled_orders().read(&pk).await?.is_some());
//!
//! db.cancelled_orders().delete(&pk).await?;
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys},
    },
    implement_all_crud_methods,
};

/// CRUD handle for `logs.cancelled_orders`.
///
/// Full CRUD surface; ordering is by `time`, and the strategy row must exist
/// before an audit row can be inserted.
pub struct CancelledOrdersCRUD<'a> {
    /// Generic CRUD bound to `logs.cancelled_orders`.
    pub(super) crud:
        CRUD<'a, CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    CancelledOrdersFullKeys,
    CancelledOrdersPrimaryKeys,
    CancelledOrdersUpdateKeys,
    CancelledOrdersCRUD
);

#[hotpath::measure_all]
impl<'a> CancelledOrdersCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held
    /// connection) for its lifetime and performs no I/O until a method is
    /// awaited.
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                CancelledOrdersFullKeys,
                CancelledOrdersPrimaryKeys,
                CancelledOrdersUpdateKeys,
            >::new(exec, "logs.cancelled_orders"),
        }
    }
}

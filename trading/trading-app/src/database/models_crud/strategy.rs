//! `trading.strategy` — the strategy registry.
//!
//! Primary key `strategy`. Open orders, positions, transactions, and
//! cancelled orders reference this table by name (`ON DELETE CASCADE` or
//! `RESTRICT`), so a strategy row must exist before its orders are written.
//! `status` drives lifecycle gating (`Active`/`Stopping`/`Inactive`).
//!
//! # Example
//!
//! ```rust
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{Status, StrategyFullKeys, StrategyPrimaryKeys};
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let name = format!("doc_strategy_{}", std::process::id());
//! let mut db = DbDirect::connect(&pool);
//!
//! db.strategy()
//!     .create_or_ignore(&StrategyFullKeys { strategy: name.clone(), status: Status::Active })
//!     .await?;
//! assert!(db
//!     .strategy()
//!     .read(&StrategyPrimaryKeys { strategy: name.clone() })
//!     .await?
//!     .is_some());
//!
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy: name })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys},
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.strategy`.
///
/// Implements the full [`CRUDTrait`](crate::database::crud::CRUDTrait)
/// surface; `strategy` is the primary key and `status` is the only updatable
/// field.
pub struct StrategyCRUD<'a> {
    /// Generic CRUD bound to `trading.strategy`.
    pub(super) crud: CRUD<'a, StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    StrategyFullKeys,
    StrategyPrimaryKeys,
    StrategyUpdateKeys,
    StrategyCRUD
);

#[hotpath::measure_all]
impl<'a> StrategyCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held
    /// connection) for its lifetime and performs no I/O until a method is
    /// awaited.
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>::new(exec, "trading.strategy"),
        }
    }
}

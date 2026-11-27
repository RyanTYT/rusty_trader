//! `trading.staged_commissions` — commissions that arrive before their fill.
//!
//! Primary key `execution_id`. The table bridges an execution and its
//! commission when they arrive out of order: inserting a row fires
//! `trg_try_apply_commission_stocks` / `_options`, which apply the fee when
//! the transaction already exists and leave the row staged otherwise. A later
//! commission for the same execution overwrites the earlier fee (last wins),
//! and the trigger and fill insert coordinate through a row lock.
//!
//! # Example
//!
//! ```rust
//! use rust_decimal::Decimal;
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let id = format!("doc_commission_{}", std::process::id());
//! let mut db = DbDirect::connect(&pool);
//!
//! // No transaction row for this id exists yet, so the fee stays staged.
//! db.staged_commissions()
//!     .create_or_update(
//!         &StagedCommissionsPrimaryKeys { execution_id: id.clone() },
//!         &StagedCommissionsUpdateKeys { fees: Some(Decimal::new(150, 2)) },
//!     )
//!     .await?;
//!
//! let row = db
//!     .staged_commissions()
//!     .read(&StagedCommissionsPrimaryKeys { execution_id: id.clone() })
//!     .await?
//!     .expect("staged fee");
//! assert_eq!(row.fees, Decimal::new(150, 2));
//!
//! db.staged_commissions()
//!     .delete(&StagedCommissionsPrimaryKeys { execution_id: id })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.staged_commissions`.
///
/// Full CRUD surface; `create_or_update` is the normal write. The staged fee
/// is applied by database triggers, not by this handle.
pub struct StagedCommissionsCRUD<'a> {
    /// Generic CRUD bound to `trading.staged_commissions`.
    pub(super) crud:
        CRUD<'a, StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    StagedCommissionsFullKeys,
    StagedCommissionsPrimaryKeys,
    StagedCommissionsUpdateKeys,
    StagedCommissionsCRUD
);

#[hotpath::measure_all]
impl<'a> StagedCommissionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held
    /// connection) for its lifetime and performs no I/O until a method is
    /// awaited.
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                StagedCommissionsFullKeys,
                StagedCommissionsPrimaryKeys,
                StagedCommissionsUpdateKeys,
            >::new(exec, "trading.staged_commissions"),
        }
    }
}

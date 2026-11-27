//! Database layer: schema types, typed CRUD, and the session/executor seams.
//!
//! The layer is deliberately bottom-up:
//!
//! 1. [`exec`](crate::database::exec) — *where* a statement runs ([`exec::Exec`](crate::database::exec::Exec): a pool or a borrowed
//!    connection).
//! 2. [`db`](crate::database::db) — *how* statements are grouped ([`db::DbTxn`](crate::database::db::DbTxn) for atomic work,
//!    [`db::DbDirect`](crate::database::db::DbDirect) for independent work) plus [`db::DbError`](crate::database::db::DbError).
//! 3. [`crud`](crate::database::crud) — the generic statement implementations ([`crud::CRUD`](crate::database::crud::CRUD),
//!    [`crud::CRUDTrait`](crate::database::crud::CRUDTrait)) and the macros that forward them to model wrappers.
//! 4. [`models`](crate::database::models) — row/key/enum types mirrored from the SQL schema.
//! 5. [`models_crud`](crate::database::models_crud) — one wrapper per table plus the asset-class interface
//!    enums (stock/option/forex dispatch).
//! 6. [`tables`](crate::database::tables) — [`tables::DbTables`](crate::database::tables::DbTables): one accessor per table on any session.
//!
//! Typical use:
//!
//! ```no_run
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbTxn;
//! use trading_app::database::models::{AssetType, Status, StrategyFullKeys};
//! use trading_app::database::tables::DbTables;
//!
//! # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let asset = AssetType::Stock;
//! let mut txn = DbTxn::begin(pool).await?;
//! txn.strategy()
//!     .create(&StrategyFullKeys { strategy: "demo".into(), status: Status::Active })
//!     .await?;
//! let open = txn.open_orders(&asset).read_all().await?;
//! txn.commit().await?;
//! # let _ = open;
//! # Ok(())
//! # }
//! ```
//!
//! A session owns the connection/transaction; the model handles returned by
//! [`tables::DbTables`](crate::database::tables::DbTables) only borrow it for one operation, so keep operations
//! sequential rather than holding two models from one session.

/// Generic statement layer: `CRUD`, `CRUDTrait`, and model-forwarding macros.
pub mod crud;
/// Sessions (`DbTxn`, `DbDirect`), the `DbConn` seam, and `DbError`.
pub mod db;
/// Query execution seam (`Exec`).
pub mod exec;
/// Row, key, and classification types mirrored from the SQL schema.
pub mod models;
/// Per-table CRUD wrappers and asset-class interface enums.
pub mod models_crud;
/// Session accessors: one method per table (`DbTables`).
pub mod tables;

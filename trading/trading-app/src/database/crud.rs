//! Generic typed CRUD statements over one model table.
//!
//! [`CRUD`](crate::database::crud::CRUD) owns the table name and an `Exec` borrow; [`CRUDTrait`](crate::database::crud::CRUDTrait) defines
//! the operations. Concrete table wrappers get the trait through
//! [`implement_all_crud_methods!`](macro@crate::implement_all_crud_methods); asset-class enums get it through
//! [`implement_crud_trait_for_interface!`](macro@crate::implement_crud_trait_for_interface).
//!
//! SQL is assembled at runtime from the `Insertable` key structs:
//! - required fields (`pri_*`) are always bound;
//! - optional fields (`opt_*`) are bound only when `Some`, which makes
//!   [`CRUDTrait::update`](crate::database::crud::CRUDTrait::update) partial and lets generated identity columns be
//!   omitted on insert.
//!
//! Every method borrows the session (`&mut self`) and returns [`DbError`](crate::database::db::DbError).
//! Wrapping the calls in a `DbTxn` makes the sequence atomic.

// use anyhow::Result;
use async_trait::async_trait;
use crud_insertable::Insertable;
use sqlx::FromRow;

use crate::database::db::{DbError, Exec};

/// Formats a bind placeholder, adding the enum cast for the three columns
/// whose SQL type is a Postgres enum (`asset_type`, `status`, `option_type`).
///
/// `pub(crate)` only so database tests can exercise the mapping.
pub(crate) fn map_to_placeholder(key: usize, column_name: &str) -> String {
    match column_name {
        "asset_type" => format!("${}::asset_type", key),
        "status" => format!("${}::status", key),
        "option_type" => format!("${}::option_type", key),
        _ => format!("${}", key),
    }
}

/// Core CRUD implementation, bound to an `Exec` target.
///
/// The executor is either a borrowed pool or a borrowed connection (a
/// transaction or a held direct session). This is what lets the same model
/// code run inside or outside a transaction without duplicating SQL.
pub struct CRUD<'a, FK, PK, UK> {
    /// Schema-qualified table name, supplied as a constant by the model
    /// wrapper (for example `trading.open_stock_orders`).
    pub table: &'static str,
    /// Where statements run: a pool (per-statement connection) or a borrowed
    /// connection (transaction or held session).
    pub exec: Exec<'a>,
    /// Ties the key types to this CRUD; no runtime value.
    pub _marker: std::marker::PhantomData<(FK, PK, UK)>, // Just to "use" the generics
}

impl<'a, FK, PK, UK> CRUD<'a, FK, PK, UK> {
    /// Binds a table name and executor. Called by model wrappers and the
    /// session accessors; not usually constructed directly.
    pub fn new(exec: Exec<'a>, table: &'static str) -> Self {
        Self {
            table,
            exec,
            _marker: std::marker::PhantomData,
        }
    }
}

/// The typed operations available on a model table.
///
/// `FullKeys` is the row shape used for insert and read, `PrimaryKeys`
/// addresses one row, and `UpdateKeys` carries optional columns for partial
/// updates and upserts. Methods borrow the session so operations on one
/// session are sequential.
///
/// # Contract
///
/// `create` inserts, `read` returns the row, `update` reports how many rows it
/// changed, and `delete` removes it. Against `TEST_DATABASE_URL` this example
/// runs and asserts each step; without it, it is skipped.
///
/// ```rust
/// use trading_app::database::crud::CRUDTrait;
/// use trading_app::database::db::DbDirect;
/// use trading_app::database::models::{
///     Status, StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys,
/// };
/// use trading_app::database::tables::DbTables;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
/// # let pool = sqlx::PgPool::connect(&url).await?;
/// let strategy = format!("doc_crud_{}", std::process::id());
/// let pk = StrategyPrimaryKeys { strategy: strategy.clone() };
/// let unknown = StrategyPrimaryKeys { strategy: format!("{strategy}_missing") };
/// let mut db = DbDirect::connect(&pool);
///
/// db.strategy()
///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
///     .await?;
/// let row = db.strategy().read(&pk).await?.expect("created row");
/// assert!(matches!(row.status, Status::Active));
///
/// let changed = db
///     .strategy()
///     .update(&pk, &StrategyUpdateKeys { status: Some(Status::Stopping) })
///     .await?;
/// assert_eq!(changed, 1);
/// assert!(matches!(
///     db.strategy().read(&pk).await?.unwrap().status,
///     Status::Stopping
/// ));
///
/// db.strategy().delete(&pk).await?;
/// assert!(db.strategy().read(&pk).await?.is_none());
///
/// // Missing rows are reported, not invented.
/// assert!(db.strategy().read(&unknown).await?.is_none());
/// assert_eq!(
///     db.strategy()
///         .update(&unknown, &StrategyUpdateKeys { status: Some(Status::Inactive) })
///         .await?,
///     0
/// );
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys>
where
    FullKeys: Sized + Send + Sync,
    PrimaryKeys: Sized + Send + Sync,
    UpdateKeys: Sized + Send + Sync,
{
    // fn new(pool: PgPool, table: String) -> Self;
    /// Inserts one row. Generated/identity columns must be absent from
    /// `FullKeys` (or carried as `Option` and left `None`) so the database
    /// can fill them.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbTxn;
    /// use trading_app::database::models::{Status, StrategyFullKeys};
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut txn = DbTxn::begin(pool).await?;
    /// txn.strategy()
    ///     .create(&StrategyFullKeys { strategy: "demo".into(), status: Status::Active })
    ///     .await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create(&mut self, raw_item: &FullKeys) -> Result<(), DbError>;
    /// Inserts one row and silently skips it if a unique constraint conflicts
    /// (`ON CONFLICT DO NOTHING`). With no natural unique key this is a plain
    /// insert.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{Status, StrategyFullKeys};
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DbDirect::connect(pool);
    /// db.strategy()
    ///     .create_or_ignore(&StrategyFullKeys { strategy: "demo".into(), status: Status::Active })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_or_ignore(&mut self, raw_item: &FullKeys) -> Result<(), DbError>;
    /// Upserts: inserts the primary keys plus the provided update columns and
    /// overwrites those columns on conflict. Not usable with a generated
    /// identity PK, and every `NOT NULL` non-PK column must be present.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{Status, StrategyPrimaryKeys, StrategyUpdateKeys};
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DbDirect::connect(pool);
    /// db.strategy()
    ///     .create_or_update(
    ///         &StrategyPrimaryKeys { strategy: "demo".into() },
    ///         &StrategyUpdateKeys { status: Some(Status::Active) },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_or_update(&mut self, pk: &PrimaryKeys, uk: &UpdateKeys) -> Result<(), DbError>;
    /// Reads one row by primary key; `None` when it does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::AssetType;
    /// use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersPrimaryKeys;
    /// use trading_app::database::models::OpenStockOrdersPrimaryKeys;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DbDirect::connect(pool);
    /// let pk = OpenOrdersPrimaryKeys::Stock(OpenStockOrdersPrimaryKeys {
    ///     order_perm_id: 42,
    ///     order_id: 7,
    /// });
    /// let row = db.open_orders(&AssetType::Stock).read(&pk).await?;
    /// # let _ = row;
    /// # Ok(())
    /// # }
    /// ```
    async fn read(&mut self, raw_pk: &PrimaryKeys) -> Result<Option<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    /// Reads the row and locks it until the enclosing transaction commits
    /// (`SELECT ... FOR UPDATE`). Outside a transaction the lock is released
    /// as soon as the statement completes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbTxn;
    /// use trading_app::database::models::AssetType;
    /// use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersPrimaryKeys;
    /// use trading_app::database::models::OpenStockOrdersPrimaryKeys;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut txn = DbTxn::begin(pool).await?;
    /// let pk = OpenOrdersPrimaryKeys::Stock(OpenStockOrdersPrimaryKeys {
    ///     order_perm_id: 42,
    ///     order_id: 7,
    /// });
    /// let row = txn.open_orders(&AssetType::Stock).read_for_update(&pk).await?;
    /// txn.commit().await?;
    /// # let _ = row;
    /// # Ok(())
    /// # }
    /// ```
    async fn read_for_update(&mut self, raw_pk: &PrimaryKeys) -> Result<Option<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    /// Reads every row in the table. Potentially expensive; avoid on large or
    /// hypertable relations outside maintenance code.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DbDirect::connect(pool);
    /// let strategies = db.strategy().read_all().await?;
    /// # let _ = strategies;
    /// # Ok(())
    /// # }
    /// ```
    async fn read_all(&mut self) -> Result<Vec<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    /// Updates the provided (non-`None`) columns of the matching row and
    /// returns the number of rows affected. When every update field is
    /// `None`, returns `Ok(0)` without executing SQL.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{Status, StrategyPrimaryKeys, StrategyUpdateKeys};
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DbDirect::connect(pool);
    /// let changed = db.strategy()
    ///     .update(
    ///         &StrategyPrimaryKeys { strategy: "demo".into() },
    ///         &StrategyUpdateKeys { status: Some(Status::Stopping) },
    ///     )
    ///     .await?;
    /// # let _ = changed;
    /// # Ok(())
    /// # }
    /// ```
    async fn update(
        &mut self,
        raw_pk: &PrimaryKeys,
        raw_update: &UpdateKeys,
    ) -> Result<u64, DbError>;
    /// Deletes the matching row. Deleting an absent row is `Ok(())`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::StrategyPrimaryKeys;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DbDirect::connect(pool);
    /// db.strategy()
    ///     .delete(&StrategyPrimaryKeys { strategy: "demo".into() })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn delete(&mut self, raw_pk: &PrimaryKeys) -> Result<(), DbError>;
}

#[async_trait]
impl<
    'a,
    FullKeys: Sized + Send + Sync + Insertable,
    PrimaryKeys: Sized + Send + Sync + Insertable,
    UpdateKeys: Sized + Send + Sync + Insertable,
> CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys> for CRUD<'a, FullKeys, PrimaryKeys, UpdateKeys>
{
    // fn new(pool: PgPool, table: String) -> Self {
    //     Self {
    //         pool,
    //         table,
    //         _marker: std::marker::PhantomData,
    //     }
    // }

    /// A typical create function - pass in all FullKeys without Option<>
    async fn create(&mut self, full_keys: &FullKeys) -> Result<(), DbError> {
        let mut all_cols = full_keys.pri_column_names();
        all_cols.extend(full_keys.opt_column_names());
        let all_placeholders = all_cols
            .iter()
            .enumerate()
            .map(|(index, col)| map_to_placeholder(index + 1, col))
            .collect::<Vec<_>>();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            &self.table,
            all_cols.join(", "),
            all_placeholders.join(", ")
        );

        let query = full_keys.bind_pri(&sql);
        let query = full_keys.bind_opt_to_query(query);

        self.exec
            .execute(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "create",
                source,
            })?;
        Ok(())
    }

    /// A create_or_ignore function - ignores if conflicts
    /// - NOTE: the query uses inbuilt conflict in the table. i.e. if the conflict doesn't exist on
    /// any unique_index or primary key, it may raise an error with insertion
    async fn create_or_ignore(&mut self, full_keys: &FullKeys) -> Result<(), DbError> {
        let mut all_cols = full_keys.pri_column_names();
        all_cols.extend(full_keys.opt_column_names());
        let all_placeholders = all_cols
            .iter()
            .enumerate()
            .map(|(index, col)| map_to_placeholder(index + 1, col))
            .collect::<Vec<_>>();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING;",
            &self.table,
            all_cols.join(", "),
            all_placeholders.join(", "),
        );

        let query = full_keys.bind_pri(&sql);
        let query = full_keys.bind_opt_to_query(query);

        self.exec
            .execute(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "create_or_ignore",
                source,
            })?;
        Ok(())
    }

    /// A create_or_update function - upsert basically
    /// - function is split into 2 parameters for ease of processing for function
    async fn create_or_update(&mut self, pk: &PrimaryKeys, uk: &UpdateKeys) -> Result<(), DbError> {
        let mut all_cols = pk.pri_column_names();
        all_cols.extend(uk.opt_column_names());
        let all_placeholders = all_cols
            .iter()
            .enumerate()
            .map(|(index, col)| map_to_placeholder(index + 1, col))
            .collect::<Vec<_>>();
        let on_conflict_clause = pk.pri_column_names().join(", ");
        let set_clause: Vec<String> = uk
            .opt_column_names()
            .iter()
            .map(|col| format!("{} = EXCLUDED.{}", &col, &col))
            .collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {};",
            &self.table,
            all_cols.join(", "),
            all_placeholders.join(", "),
            on_conflict_clause,
            set_clause.join(", ")
        );

        let mut query = pk.bind_pri(&sql);
        query = uk.bind_opt_to_query(query);

        self.exec
            .execute(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "create_or_update",
                source,
            })?;
        Ok(())
    }

    /// A typical read function for a table - give primary keys without Option<>
    async fn read(&mut self, pk: &PrimaryKeys) -> Result<Option<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>,
    {
        let conditions = pk
            .pri_column_names()
            .iter()
            .enumerate()
            .map(|(index, column)| format!("{} = ${}", column, index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!("SELECT * FROM {} WHERE {};", &self.table, conditions);
        let mut query = sqlx::query_as::<_, FullKeys>(&sql);
        query = pk.bind_pri_to_query_as(query);

        let result = self
            .exec
            .fetch_optional(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "read",
                source,
            })?;
        Ok(result)
    }

    /// Like `read`, but locks the selected row until the enclosing transaction
    /// commits.
    async fn read_for_update(&mut self, pk: &PrimaryKeys) -> Result<Option<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>,
    {
        let conditions = pk
            .pri_column_names()
            .iter()
            .enumerate()
            .map(|(index, column)| format!("{} = ${}", column, index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!(
            "SELECT * FROM {} WHERE {} FOR UPDATE;",
            &self.table, conditions
        );
        let mut query = sqlx::query_as::<_, FullKeys>(&sql);
        query = pk.bind_pri_to_query_as(query);

        let result = self
            .exec
            .fetch_optional(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "read_for_update",
                source,
            })?;
        Ok(result)
    }

    /// Typical read_all function that returns all rows in DB
    /// - thus, could be a potentially taxing query
    async fn read_all(&mut self) -> Result<Vec<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>,
    {
        let sql = format!("SELECT * FROM {};", &self.table);
        let query = sqlx::query_as::<_, FullKeys>(&sql);
        let result = self
            .exec
            .fetch_all(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "read_all",
                source,
            })?;
        Ok(result)
    }

    /// Typical update function that updates the matching row in table
    /// - Primary keys should be passed without Option
    /// - Update keys should be passed as Option<>: If a key should not be updated, pass None
    async fn update(&mut self, pk: &PrimaryKeys, update: &UpdateKeys) -> Result<u64, DbError> {
        // Make Set clauses
        let set_placeholders: Vec<String> = update
            .opt_column_names()
            .iter()
            .enumerate()
            .map(|(index, col)| format!("{} = {}", col, map_to_placeholder(index + 1, col)))
            .collect();
        if set_placeholders.is_empty() {
            return Ok(0);
        }
        let set_clause = set_placeholders.join(", ");

        // Make Where clauses
        let index_start_at = set_placeholders.len();
        let where_placeholders: Vec<String> = pk
            .pri_column_names()
            .iter()
            .enumerate()
            .map(|(index, col)| {
                format!(
                    "{} = ${}",
                    col,
                    // map_to_placeholder(&index_start_at + index + 1, col)
                    index_start_at + index + 1
                )
            })
            .collect();
        let where_clause = where_placeholders.join(" AND ");

        let sql = format!(
            "UPDATE {} SET {} WHERE {};",
            &self.table, set_clause, where_clause
        );
        let mut query = sqlx::query(&sql);

        query = update.bind_opt_to_query(query);
        query = pk.bind_pri_to_query(query);

        let res = self
            .exec
            .execute(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "update",
                source,
            })?;
        Ok(res.rows_affected())
    }

    /// Typical delete function that deletes the matching row in the table
    async fn delete(&mut self, pk: &PrimaryKeys) -> Result<(), DbError> {
        let conditions = pk
            .pri_column_names()
            .iter()
            .enumerate()
            .map(|(index, key)| format!("{} = ${}", key, index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!("DELETE FROM {} WHERE {};", &self.table, conditions);
        let mut query = sqlx::query(&sql);
        query = pk.bind_pri_to_query(query);
        self.exec
            .execute(query)
            .await
            .map_err(|source| DbError::Query {
                operation: "delete",
                source,
            })?;

        Ok(())
    }
}

/// Implements [`CRUDTrait`](crate::database::crud::CRUDTrait) for a concrete table wrapper by forwarding every
/// method to its inner [`CRUD`](crate::database::crud::CRUD) field.
///
/// Arguments: `(delegator_field, FullKeys, PrimaryKeys, UpdateKeys, Wrapper)`,
/// where `Wrapper` is a generic type (`$crud<'a>`) exposing `delegator_field`.
#[macro_export]
macro_rules! implement_all_crud_methods {
    ($delegator:ident, $FullKeys:ty, $PrimaryKeys:ty, $UpdateKeys:ty, $crud:ident) => {
        #[async_trait::async_trait]
        impl<'a> crate::database::crud::CRUDTrait<$FullKeys, $PrimaryKeys, $UpdateKeys>
            for $crud<'a>
        {
            async fn create(
                &mut self,
                raw_item: &$FullKeys,
            ) -> Result<(), crate::database::db::DbError> {
                self.$delegator.create(raw_item).await
            }
            async fn create_or_ignore(
                &mut self,
                raw_item: &$FullKeys,
            ) -> Result<(), crate::database::db::DbError> {
                self.$delegator.create_or_ignore(raw_item).await
            }
            async fn read(
                &mut self,
                raw_pk: &$PrimaryKeys,
            ) -> Result<Option<$FullKeys>, crate::database::db::DbError>
            where
                $FullKeys: Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
            {
                self.$delegator.read(raw_pk).await
            }
            async fn read_for_update(
                &mut self,
                raw_pk: &$PrimaryKeys,
            ) -> Result<Option<$FullKeys>, crate::database::db::DbError>
            where
                $FullKeys: Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
            {
                self.$delegator.read_for_update(raw_pk).await
            }
            async fn create_or_update(
                &mut self,
                pk: &$PrimaryKeys,
                uk: &$UpdateKeys,
            ) -> Result<(), crate::database::db::DbError> {
                self.$delegator.create_or_update(pk, uk).await
            }
            async fn read_all(&mut self) -> Result<Vec<$FullKeys>, crate::database::db::DbError>
            where
                $FullKeys: Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
            {
                self.$delegator.read_all().await
            }
            async fn update(
                &mut self,
                raw_pk: &$PrimaryKeys,
                raw_update: &$UpdateKeys,
            ) -> Result<u64, crate::database::db::DbError> {
                self.$delegator.update(raw_pk, raw_update).await
            }
            async fn delete(
                &mut self,
                raw_pk: &$PrimaryKeys,
            ) -> Result<(), crate::database::db::DbError> {
                self.$delegator.delete(raw_pk).await
            }
        }
    };
}

/// Implements [`CRUDTrait`](crate::database::crud::CRUDTrait) for an asset-class interface enum.
///
/// Arguments: `(Enum, FullKeys, PrimaryKeys, UpdateKeys, [Variant, ...])`.
/// Each variant must hold a concrete CRUD whose key/row payloads correspond
/// positionally to the enum variants. Mismatched key variants return
/// [`DbError::KeyVariantMismatch`] without executing anything; `read_all`
/// concatenates every variant's rows.
#[macro_export]
macro_rules! implement_crud_trait_for_interface {
    ($crud:ident, $fk:ident, $pk:ident, $uk:ident, [$($variant:ident),* $(,)?]) => {
        #[async_trait::async_trait]
        impl<'a> crate::database::crud::CRUDTrait<$fk, $pk, $uk> for $crud<'a>
        {
            async fn read(
                &mut self,
                raw_pk: &$pk,
            ) -> Result<Option<$fk>, crate::database::db::DbError>
            where
                $fk: Unpin + for<'r> sqlx::prelude::FromRow<'r, sqlx::postgres::PgRow>,
            {
                match (self, raw_pk) {
                    $(
                        (Self::$variant(s), $pk::$variant(actual_pk)) => {
                            s.read(actual_pk).await.map(|ok_res| ok_res.map($fk::$variant))
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "read",
                    }),
                }
            }
            async fn read_for_update(
                &mut self,
                raw_pk: &$pk,
            ) -> Result<Option<$fk>, crate::database::db::DbError>
            where
                $fk: Unpin + for<'r> sqlx::prelude::FromRow<'r, sqlx::postgres::PgRow>,
            {
                match (self, raw_pk) {
                    $(
                        (Self::$variant(s), $pk::$variant(actual_pk)) => {
                            s.read_for_update(actual_pk).await.map(|ok_res| ok_res.map($fk::$variant))
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "read_for_update",
                    }),
                }
            }
            async fn create(
                &mut self,
                raw_item: &$fk,
            ) -> Result<(), crate::database::db::DbError> {
                match (self, raw_item) {
                    $(
                        (Self::$variant(s), $fk::$variant(actual_fk)) => {
                            s.create(actual_fk).await
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "create",
                    }),
                }
            }
            async fn update(
                &mut self,
                raw_pk: &$pk,
                raw_uk: &$uk,
            ) -> Result<u64, crate::database::db::DbError> {
                match (self, raw_pk, raw_uk) {
                    $(
                        (Self::$variant(s), $pk::$variant(actual_pk), $uk::$variant(actual_uk)) => {
                            s.update(actual_pk, actual_uk).await
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "update",
                    }),
                }
            }
            async fn delete(
                &mut self,
                raw_pk: &$pk,
            ) -> Result<(), crate::database::db::DbError> {
                match (self, raw_pk) {
                    $(
                        (Self::$variant(s), $pk::$variant(actual_pk)) => {
                            s.delete(actual_pk).await
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "delete",
                    }),
                }
            }
            async fn read_all(&mut self) -> Result<Vec<$fk>, crate::database::db::DbError>
            where
                $fk:
                    Unpin + for<'r> sqlx::prelude::FromRow<'r, sqlx::postgres::PgRow>,
            {
                match self {
                    $(Self::$variant(s) => s.read_all().await.map(|vec| vec.into_iter().map($fk::$variant).collect())),*
                }
            }
            async fn create_or_ignore(
                &mut self,
                raw_item: &$fk,
            ) -> Result<(), crate::database::db::DbError> {
                match (self, raw_item) {
                    $(
                        (Self::$variant(s), $fk::$variant(actual_fk)) => {
                            s.create_or_ignore(actual_fk).await
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "create_or_ignore",
                    }),
                }
            }
            async fn create_or_update(
                &mut self,
                raw_pk: &$pk,
                raw_uk: &$uk,
            ) -> Result<(), crate::database::db::DbError> {
                match (self, raw_pk, raw_uk) {
                    $(
                        (Self::$variant(s), $pk::$variant(actual_pk), $uk::$variant(actual_uk)) => {
                            s.create_or_update(actual_pk, actual_uk).await
                        }
                    ),*
                    _ => Err(crate::database::db::DbError::KeyVariantMismatch {
                        operation: "create_or_update",
                    }),
                }
            }
        }
    };
}

// use anyhow::Result;
use async_trait::async_trait;
use crud_insertable::Insertable;
use sqlx::FromRow;

use crate::database::db::{DbError, Exec};

/// pub(crate) visibility ONLY for testing purposes
pub(crate) fn map_to_placeholder(key: usize, column_name: &str) -> String {
    match column_name {
        "asset_type" => format!("${}::asset_type", key),
        "status" => format!("${}::trading.status", key),
        "option_type" => format!("${}::trading.option_type", key),
        _ => format!("${}", key),
    }
}

/// Core CRUD implementation, bound to an [`Exec`] target.
///
/// The executor is either a borrowed pool or a borrowed connection (a
/// transaction or a held direct session). This is what lets the same model
/// code run inside or outside a transaction without duplicating SQL.
pub struct CRUD<'a, FK, PK, UK> {
    pub table: &'static str,
    pub exec: Exec<'a>,
    pub _marker: std::marker::PhantomData<(FK, PK, UK)>, // Just to "use" the generics
}

impl<'a, FK, PK, UK> CRUD<'a, FK, PK, UK> {
    pub fn new(exec: Exec<'a>, table: &'static str) -> Self {
        Self {
            table,
            exec,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
pub trait CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys>
where
    FullKeys: Sized + Send + Sync,
    PrimaryKeys: Sized + Send + Sync,
    UpdateKeys: Sized + Send + Sync,
{
    // fn new(pool: PgPool, table: String) -> Self;
    async fn create(&mut self, raw_item: &FullKeys) -> Result<(), DbError>;
    async fn create_or_ignore(&mut self, raw_item: &FullKeys) -> Result<(), DbError>;
    async fn create_or_update(
        &mut self,
        pk: &PrimaryKeys,
        uk: &UpdateKeys,
    ) -> Result<(), DbError>;
    async fn read(&mut self, raw_pk: &PrimaryKeys) -> Result<Option<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    async fn read_all(&mut self) -> Result<Vec<FullKeys>, DbError>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    async fn update(
        &mut self,
        raw_pk: &PrimaryKeys,
        raw_update: &UpdateKeys,
    ) -> Result<u64, DbError>;
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
    async fn create_or_update(
        &mut self,
        pk: &PrimaryKeys,
        uk: &UpdateKeys,
    ) -> Result<(), DbError> {
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
    async fn update(
        &mut self,
        pk: &PrimaryKeys,
        update: &UpdateKeys,
    ) -> Result<u64, DbError> {
        // Make Set clauses
        let set_placeholders: Vec<String> = update
            .opt_column_names()
            .iter()
            .enumerate()
            .map(|(index, col)| format!("{} = {}", col, map_to_placeholder(index + 1, col)))
            .collect();
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

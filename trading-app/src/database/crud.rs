use std::usize;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};

use crate::Insertable;

fn map_to_placeholder(key: usize, column_name: &str) -> String {
    match column_name {
        "asset_type" => format!("${}::asset_type", key),
        "status" => format!("${}::status", key),
        "option_type" => format!("${}::option_type", key),
        _ => format!("${}", key),
    }
}

#[derive(Debug, Clone)]
pub struct CRUD<FK, PK, UK> {
    pub pool: PgPool,
    pub table: String,
    pub _marker: std::marker::PhantomData<(FK, PK, UK)>, // Just to "use" the generics
}

#[async_trait]
pub trait CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys>
where
    FullKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    PrimaryKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    UpdateKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn new(pool: PgPool, table: String) -> Self;
    async fn create(&self, raw_item: &FullKeys) -> Result<()>;
    async fn create_or_ignore(&self, raw_item: &FullKeys) -> Result<()>;
    async fn create_or_update(&self, pk: &PrimaryKeys, uk: &UpdateKeys) -> Result<()>;
    async fn read(&self, raw_pk: &PrimaryKeys) -> Result<Option<FullKeys>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    async fn read_all(&self) -> Result<Option<Vec<FullKeys>>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    async fn update(
        &self,
        raw_pk: &PrimaryKeys,
        raw_update: &UpdateKeys,
    ) -> Result<u64, anyhow::Error>;
    async fn delete(&self, raw_pk: &PrimaryKeys) -> Result<()>;
}

#[async_trait]
impl<
    FullKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de> + Insertable,
    PrimaryKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de> + Insertable,
    UpdateKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de> + Insertable,
> CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys> for CRUD<FullKeys, PrimaryKeys, UpdateKeys>
{
    fn new(pool: PgPool, table: String) -> Self {
        Self {
            pool,
            table,
            _marker: std::marker::PhantomData,
        }
    }

    /// A typical create function - pass in all FullKeys without Option<>
    async fn create(&self, full_keys: &FullKeys) -> Result<()> {
        let all_cols = full_keys.pri_column_names();
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

        query.execute(&self.pool).await?;
        Ok(())
    }

    /// A create_or_ignore function - ignores if conflicts
    /// - NOTE: the query uses inbuilt conflict in the table. i.e. if the conflict doesn't exist on
    /// any unique_index or primary key, it may raise an error with insertion
    async fn create_or_ignore(&self, full_keys: &FullKeys) -> Result<()> {
        let all_cols = full_keys.pri_column_names();
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

        query.execute(&self.pool).await?;
        Ok(())
    }

    /// A create_or_update function - upsert basically
    /// - function is split into 2 parameters for ease of processing for function
    async fn create_or_update(&self, pk: &PrimaryKeys, uk: &UpdateKeys) -> Result<()> {
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

        query.execute(&self.pool).await?;
        Ok(())
    }

    /// A typical read function for a table - give primary keys without Option<>
    async fn read(&self, pk: &PrimaryKeys) -> Result<Option<FullKeys>>
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

        let result = query.fetch_optional(&self.pool).await?;
        Ok(result)
    }

    /// Typical read_all function that returns all rows in DB
    /// - thus, could be a potentially taxing query
    async fn read_all(&self) -> Result<Option<Vec<FullKeys>>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>,
    {
        let sql = format!("SELECT * FROM {};", &self.table);
        let query = sqlx::query_as::<_, FullKeys>(&sql);
        let result = query.fetch_all(&self.pool).await?;
        Ok(Some(result))
    }

    /// Typical update function that updates the matching row in table
    /// - Primary keys should be passed without Option
    /// - Update keys should be passed as Option<>: If a key should not be updated, pass None
    async fn update(&self, pk: &PrimaryKeys, update: &UpdateKeys) -> Result<u64, anyhow::Error> {
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

        let res = query.execute(&self.pool).await?;
        Ok(res.rows_affected())
    }

    /// Typical delete function that deletes the matching row in the table
    async fn delete(&self, pk: &PrimaryKeys) -> Result<()> {
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
        query.execute(&self.pool).await?;

        Ok(())
    }
}

#[macro_export]
macro_rules! delegate_all_crud_methods {
    ($delegator:ident, $FullKeys:ty, $PrimaryKeys:ty, $UpdateKeys:ty) => {
        pub async fn create(&self, raw_item: &$FullKeys) -> anyhow::Result<()> {
            self.$delegator.create(raw_item).await
        }
        pub async fn create_or_ignore(&self, raw_item: &$FullKeys) -> anyhow::Result<()> {
            self.$delegator.create_or_ignore(raw_item).await
        }
        pub async fn read(&self, raw_pk: &$PrimaryKeys) -> anyhow::Result<Option<$FullKeys>>
        where
            $FullKeys: Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
        {
            self.$delegator.read(raw_pk).await
        }
        pub async fn create_or_update(
            &self,
            pk: &$PrimaryKeys,
            uk: &$UpdateKeys,
        ) -> anyhow::Result<()> {
            self.$delegator.create_or_update(pk, uk).await
        }
        pub async fn read_all(&self) -> anyhow::Result<Option<Vec<$FullKeys>>>
        where
            $FullKeys: Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
        {
            self.$delegator.read_all().await
        }
        pub async fn update(
            &self,
            raw_pk: &$PrimaryKeys,
            raw_update: &$UpdateKeys,
        ) -> anyhow::Result<u64, anyhow::Error> {
            self.$delegator.update(raw_pk, raw_update).await
        }
        pub async fn delete(&self, raw_pk: &$PrimaryKeys) -> anyhow::Result<()> {
            self.$delegator.delete(raw_pk).await
        }
    };
}

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};

pub struct CRUD<FK, PK, UK> {
    db: PgPool,
    table: String,
    _marker: std::marker::PhantomData<(FK, PK, UK)>,
}

#[async_trait]
pub trait CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys>
where
    FullKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    PrimaryKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    UpdateKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn new(db: PgPool, table: String) -> Self;
    async fn create(&self, raw_item: &FullKeys) -> Result<()>;
    async fn read(&self, raw_pk: &PrimaryKeys) -> Result<Option<FullKeys>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    async fn read_all(&self) -> Result<Option<Vec<FullKeys>>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>;
    async fn update(&self, raw_pk: &PrimaryKeys, raw_update: &UpdateKeys) -> Result<()>;
    async fn delete(&self, raw_pk: &PrimaryKeys) -> Result<()>;
}

#[macro_export]
macro_rules! bind_json_value {
    ($query:expr, $key:expr, $value:expr) => {{
        match $value {
            serde_json::Value::String(s) => Ok($query.bind(s.as_str())),
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    Ok($query.bind(f))
                } else if let Some(i) = n.as_i64() {
                    Ok($query.bind(i))
                } else {
                    Err(anyhow::anyhow!(
                        "Unsupported number format for column `{}`",
                        $key
                    ))
                }
            }
            serde_json::Value::Bool(b) => Ok($query.bind(*b)),
            _ => Err(anyhow::anyhow!(
                "Unsupported value type for column `{}`",
                $key
            )),
        }
    }};
}

#[async_trait]
impl<
    FullKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    PrimaryKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    UpdateKeys: Sized + Send + Sync + Serialize + for<'de> Deserialize<'de>,
> CRUDTrait<FullKeys, PrimaryKeys, UpdateKeys> for CRUD<FullKeys, PrimaryKeys, UpdateKeys>
{
    fn new(db: PgPool, table: String) -> Self {
        Self {
            db,
            table,
            _marker: std::marker::PhantomData,
        }
    }

    async fn create(&self, raw_item: &FullKeys) -> Result<()> {
        let item_unpacked = serde_json::to_value(raw_item)?;
        let item = item_unpacked
            .as_object()
            .ok_or_else(|| anyhow!("Expected JSON object"))?;

        let columns: Vec<_> = item.keys().map(|value| format!("{}", value)).collect();
        let placeholders: Vec<_> = (1..=columns.len()).map(|i| format!("${}", i)).collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            &self.table,
            columns.join(", "),
            placeholders.join(", ")
        );

        let mut query = sqlx::query(&sql);
        for (key, value) in item.iter() {
            query = bind_json_value!(query, key, value)?;
        }
        query.execute(&self.db).await?;
        Ok(())
    }

    async fn read(&self, raw_pk: &PrimaryKeys) -> Result<Option<FullKeys>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>,
    {
        let pk_unpacked = serde_json::to_value(raw_pk)?;
        let pk = pk_unpacked
            .as_object()
            .ok_or_else(|| anyhow!("Expected JSON object"))?;

        let conditions = pk
            .keys()
            .enumerate()
            .map(|(index, column)| format!("{} = ${}", column, index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!("SELECT * FROM {} WHERE {}", &self.table, conditions);
        // let mut query = sqlx::query_as::<_, FullKeys>(&sql);
        let mut query = sqlx::query_as::<_, FullKeys>(&sql);

        for (key, value) in pk.iter() {
            query = bind_json_value!(query, key, value)?;
        }

        let result = query.fetch_optional(&self.db).await?;
        Ok(result)
    }

    async fn read_all(&self) -> Result<Option<Vec<FullKeys>>>
    where
        FullKeys: Unpin + for<'r> FromRow<'r, sqlx::postgres::PgRow>,
    {
        let sql = format!("SELECT * FROM {}", &self.table);
        let query = sqlx::query_as::<_, FullKeys>(&sql);
        let result = query.fetch_all(&self.db).await?;
        Ok(Some(result))
    }

    async fn update(&self, raw_pk: &PrimaryKeys, raw_update: &UpdateKeys) -> Result<()> {
        let pk_unpacked = serde_json::to_value(raw_pk)?;
        let update_unpacked = serde_json::to_value(raw_update)?;
        let pk = pk_unpacked
            .as_object()
            .ok_or_else(|| anyhow!("Expected JSON object"))?;
        let update = update_unpacked
            .as_object()
            .ok_or_else(|| anyhow!("Expected JSON object"))?;

        let mut index = 0;
        // Make Set clauses
        let mut set_clause_vec = Vec::new();
        for (key, value) in update.iter() {
            if !value.is_null() {
                index += 1;
                set_clause_vec.push(format!("{} = ${}", key, index));
            }
        }
        let set_clause = set_clause_vec.join(", ");

        // Make Where clauses
        let mut where_clause_vec = Vec::new();
        for key in pk.keys() {
            index += 1;
            where_clause_vec.push(format!("{} = ${}", key, index));
        }
        let where_clause = where_clause_vec.join(" AND ");

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            &self.table, set_clause, where_clause
        );
        let mut query = sqlx::query(&sql);

        for (key, value) in update.iter() {
            if !value.is_null() {
                query = bind_json_value!(query, key, value)?;
            }
        }
        for (key, value) in pk.iter() {
            query = bind_json_value!(query, key, value)?;
        }

        query.execute(&self.db).await?;

        Ok(())
    }

    async fn delete(&self, raw_pk: &PrimaryKeys) -> Result<()> {
        let pk_unpacked = serde_json::to_value(raw_pk)?;
        let pk = pk_unpacked
            .as_object()
            .ok_or_else(|| anyhow!("Expected JSON object"))?;

        let conditions = pk
            .keys()
            .enumerate()
            .map(|(index, key)| format!("{} = ${}", key, index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!("DELETE FROM {} WHERE {}", &self.table, conditions);
        let mut query = sqlx::query(&sql);
        for (key, value) in pk.iter() {
            query = bind_json_value!(query, key, value)?;
        }

        query.execute(&self.db).await?;
        Ok(())
    }
}

use async_trait::async_trait;
use sqlx::{Postgres, postgres::PgArguments, query::QueryAs};
pub mod database;
pub mod execution;
pub mod init;
pub mod logger;
pub mod market_data;
pub mod strategy;

#[macro_export]
macro_rules! unlock {
    ($variable:expr, $name:expr, $fn_name:expr) => {{
        $variable.lock().map_err(|e| {
            tracing::error!(
                "Failed to acquire lock from {} in {}: {}",
                $name,
                $fn_name,
                e
            );
            format!(
                "Failed to acquire lock from {} in {}: {}",
                $name, $fn_name, e
            )
        })?
    }};
}

#[async_trait]
pub trait Insertable {
    fn table_name() -> &'static str;
    fn pri_column_names(&self) -> Vec<&'static str>;
    fn opt_column_names(&self) -> Vec<&'static str>;
    fn bind_pri<'q>(&'q self, sql: &'q str) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_pri_to_query<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_pri_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
    fn bind_opt<'q>(&'q self, sql: &'q str) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_opt_to_query<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_opt_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
}

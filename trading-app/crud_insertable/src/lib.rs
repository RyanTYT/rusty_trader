use sqlx::{Postgres, postgres::PgArguments, query::Query, query::QueryAs};

pub trait Insertable {
    fn table_name() -> &'static str;
    fn pri_column_names(&self) -> Vec<&'static str>;
    fn pri_column_names_wo_time(&self) -> Vec<&'static str>;
    fn opt_column_names(&self) -> Vec<&'static str>;
    fn bind_pri<'q>(&'q self, sql: &'q str) -> Query<'q, Postgres, PgArguments>;
    fn bind_pri_to_query<'q>(
        &'q self,
        query: Query<'q, Postgres, PgArguments>,
    ) -> Query<'q, Postgres, PgArguments>;
    fn bind_pri_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
    fn bind_opt<'q>(&'q self, sql: &'q str) -> Query<'q, Postgres, PgArguments>;
    fn bind_opt_to_query<'q>(
        &'q self,
        query: Query<'q, Postgres, PgArguments>,
    ) -> Query<'q, Postgres, PgArguments>;
    fn bind_opt_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
}

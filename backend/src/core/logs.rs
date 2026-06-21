use axum::{
    Json,
    extract::{Path, Query, State},
    response::IntoResponse,
};
use chrono::NaiveDate;
use http::StatusCode;
use regex::Regex;
use sqlx::QueryBuilder;
use std::collections::HashMap;

#[derive(Debug, serde::Deserialize)]
pub struct LogFilter {
    level: Option<String>,
    name: Option<String>,
    exclude_name: Option<String>,
    limit: Option<usize>,
    start: Option<usize>,
}

fn parse_log_line(line: &str) -> Option<HashMap<String, String>> {
    // Adjust this regex to match your format exactly
    let pattern = Regex::new(
        // r"(?x)
        // ^(?P<asctime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) -
        // (?P<levelname>\w+) -
        // (?P<name>[\w\.-]+) -
        // (?P<module>[\w\-\.]+)\.(?P<funcName>\w+):(?P<lineno>\d+) -
        // (?P<message>.+)$",
        r"(?sx) # 's' for singleline (dotall), 'x' for free-spacing comments
        ^(?P<asctime>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\s-\s
        (?P<levelname>\w+)\s-\s
        (?P<name>[\w\.-]+)\s-\s
        (?P<module>[\w\-\.]+)\.(?P<funcName>\w+):(?P<lineno>\d+)\s-\s
        (?P<message>.*?)$",
    )
    .unwrap();

    // Naming of keys aligns with the names for the logging in python
    pattern.captures(line).map(|caps| {
        let keys = [
            "asctime",
            "levelname",
            "name",
            "module",
            "funcName",
            "lineno",
            "message",
        ];
        keys.iter()
            .map(|&k| {
                (
                    k.to_string(),
                    caps.name(k).map_or("", |m| m.as_str()).trim().to_string(),
                )
            })
            .collect()
    })
}

pub async fn list_logs(State(state): State<crate::AppState>) -> impl IntoResponse {
    let logs_dates_query = "SELECT DISTINCT (time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE FROM trading.logs";
    match sqlx::query_scalar::<sqlx::postgres::Postgres, chrono::NaiveDate>(&logs_dates_query)
        .fetch_all(&state.db)
        .await
    {
        Ok(logs_list) => Json(logs_list).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("No logs in Database currently: {}", err),
        )
            .into_response(),
    }
}

pub async fn read_log(
    State(state): State<crate::AppState>,
    Path(date_str): Path<String>,
    Query(filter): Query<LogFilter>,
) -> Result<Json<Vec<crate::models::Logs>>, (StatusCode, String)> {
    let mut builder = QueryBuilder::new(
        "SELECT * FROM trading.logs WHERE (time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE = ",
    );
    let date = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("Date format passed not correct: {}", err),
        )
    })?;
    builder.push_bind(&date);

    if let Some(level) = filter.level {
        builder.push(" AND LEVEL = ");
        builder.push_bind(level.clone());
    }
    if let Some(name) = filter.name {
        builder.push(" AND name ILIKE ");
        builder.push_bind(name.clone());
    }
    if let Some(exclude_name) = filter.exclude_name {
        builder.push(" AND name NOT ILIKE ");
        builder.push_bind(exclude_name.clone());
    }

    let limit = filter.limit.unwrap_or(100);
    let offset = filter.start.unwrap_or(0);
    builder.push(" LIMIT ");
    builder.push_bind(limit as i64);
    builder.push(" OFFSET ");
    builder.push_bind(offset as i64);

    let sql_query = builder.build_query_as::<crate::models::Logs>();

    let logs = sql_query.fetch_all(&state.db).await.map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to retrieve logs: {}", err),
        )
    })?;

    Ok(Json(logs))
}

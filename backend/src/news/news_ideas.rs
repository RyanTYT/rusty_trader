// backend/src/news_ideas.rs
//
// Two new feature areas:
//   1. POST /positions/update  — manual position update (stock or option)
//   2. POST /news_ideas/*      — proxy to LLM service, streamed back to caller
//   3. GET/PATCH /news_ideas/settings — settings pass-through to LLM service

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use reqwest::Client;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeOwned, Error},
};
use serde_json::Value;

use crate::{AppState, models::CurrentStockPositions};

// ── Router ─────────────────────────────────────────────────────────────────

pub fn router(state: AppState) -> Router {
    Router::new()
        // Manual position update
        .route("/positions/update", post(update_positions))
        // LLM function proxies
        .route("/news_ideas/update_macro", post(proxy_update_macro))
        .route(
            "/news_ideas/update_industries",
            post(proxy_update_industries),
        )
        .route("/news_ideas/ticker_selector", post(proxy_ticker_selector))
        .route("/news_ideas/idea_generator", post(proxy_idea_generator))
        .route("/news_ideas/deep_dive", post(proxy_deep_dive))
        .route(
            "/news_ideas/positions_proposer",
            post(proxy_positions_proposer),
        )
        .route("/news_ideas/counter_proposer", post(proxy_counter_proposer))
        // Settings
        .route("/news_ideas/settings", get(proxy_get_settings))
        .route(
            "/news_ideas/settings",
            axum::routing::put(proxy_put_settings),
        )
        .route(
            "/news_ideas/settings/options_mode",
            patch(proxy_patch_options_mode),
        )
        // KB browser
        .route("/news_ideas/kb/tree", axum::routing::get(proxy_kb_tree))
        .route("/news_ideas/kb/file", axum::routing::get(proxy_kb_file))
        .with_state(state)
}

// ── Manual positions update ────────────────────────────────────────────────

fn deserialize_via_value<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    serde_json::from_value(serde_json::Value::deserialize(deserializer)?).map_err(D::Error::custom)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "asset_type")]
pub enum PositionUpdatePayload {
    #[serde(rename = "stock")]
    Stock(StockPositionUpdate),
    #[serde(rename = "option")]
    Option(OptionPositionUpdate),
}

#[derive(Debug, Deserialize)]
pub struct StockPositionUpdate {
    pub stock: String,
    pub primary_exchange: String,
    pub strategy: String,
    #[serde(deserialize_with = "deserialize_via_value")]
    #[serde(default)]
    pub quantity: Option<f64>,
    #[serde(deserialize_with = "deserialize_via_value")]
    #[serde(default)]
    pub avg_price: Option<f64>,

    pub currency: String,
    /// "upsert" | "delete"
    pub operation: String,
}

#[derive(Debug, Deserialize)]
pub struct OptionPositionUpdate {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub strategy: String,
    pub expiry: String,
    #[serde(deserialize_with = "deserialize_via_value")]
    pub strike: f64,
    pub multiplier: String,
    pub option_type: String, // "C" | "P"
    #[serde(deserialize_with = "deserialize_via_value")]
    #[serde(default)]
    pub quantity: Option<f64>,
    #[serde(deserialize_with = "deserialize_via_value")]
    #[serde(default)]
    pub avg_price: Option<f64>,
    pub operation: String,
}

#[derive(Deserialize, Debug)]
pub struct UpdatePositions {
    pub positions: Vec<PositionUpdatePayload>,
    pub counter_proposal: serde_json::Value,
}
async fn update_positions(
    State(state): State<AppState>,
    Json(payloads): Json<UpdatePositions>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // 1. Separate keys for batch deletion before processing upserts
    let mut stock_keys = Vec::new();
    let mut option_keys = Vec::new();

    for payload in &payloads.positions {
        match payload {
            PositionUpdatePayload::Stock(update) => {
                stock_keys.push((
                    &update.stock,
                    &update.primary_exchange,
                    &update.currency,
                    &update.strategy,
                ));
            }
            PositionUpdatePayload::Option(update) => {
                option_keys.push((
                    &update.stock,
                    &update.primary_exchange,
                    &update.currency,
                    &update.strategy,
                    &update.expiry,
                    &update.strike,
                    &update.multiplier,
                    &update.option_type,
                ));
            }
        }
    }

    // 2. Perform the "Delete others" logic if keys are provided.
    // If the payload specifically targets stock updates, prune the rest.
    if !stock_keys.is_empty() {
        let mut query_builder = sqlx::QueryBuilder::new(
            "DELETE FROM trading.target_stock_positions WHERE (stock, primary_exchange, currency, strategy) NOT IN ",
        );

        // Formats as NOT IN (( $1, $2, $3, $4 ), ( $5, $6, $7, $8 ))
        query_builder.push_tuples(stock_keys, |mut b, tuple| {
            b.push_bind(tuple.0)
                .push_bind(tuple.1)
                .push_bind(tuple.2)
                .push_bind(tuple.3);
        });

        query_builder
            .build()
            .execute(&state.db)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to prune stocks: {e}"),
                )
            })?;
    }

    // If the payload specifically targets option updates, prune the rest.
    if !option_keys.is_empty() {
        let mut query_builder = sqlx::QueryBuilder::new(
            "DELETE FROM trading.target_option_positions WHERE (stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type) NOT IN ",
        );

        query_builder.push_tuples(option_keys, |mut b, tuple| {
            b.push_bind(tuple.0)
                .push_bind(tuple.1)
                .push_bind(tuple.2)
                .push_bind(tuple.3)
                .push_bind(tuple.4)
                .push_bind(tuple.5)
                .push_bind(tuple.6)
                .push_bind(tuple.7.to_string() + "::option_type"); // Cast if using a custom PG enum type
        });

        query_builder
            .build()
            .execute(&state.db)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to prune options: {e}"),
                )
            })?;
    }

    for payload in payloads.positions {
        match payload {
            PositionUpdatePayload::Stock(update) => match update.operation.as_str() {
                "upsert" => {
                    sqlx::query(
                        "INSERT INTO trading.target_stock_positions \
                         (stock, primary_exchange, currency, strategy, quantity, avg_price) \
                         VALUES ($1, $2, $3, $4, $5, $6) \
                         ON CONFLICT (stock, primary_exchange, currency, strategy) \
                         DO UPDATE SET quantity = EXCLUDED.quantity, avg_price = EXCLUDED.avg_price",
                    )
                    .bind(&update.stock)
                    .bind(&update.primary_exchange)
                    .bind(&update.currency)
                    .bind(&update.strategy)
                    .bind(update.quantity)
                    .bind(update.avg_price)
                    .execute(&state.db)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
                "delete" => {
                    sqlx::query(
                        "DELETE FROM trading.target_stock_positions \
                         WHERE stock=$1 AND primary_exchange=$2 AND currency=$3 AND strategy=$4",
                    )
                    .bind(&update.stock)
                    .bind(&update.primary_exchange)
                    .bind(&update.currency)
                    .bind(&update.strategy)
                    .execute(&state.db)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
                op => return Err((StatusCode::BAD_REQUEST, format!("Unknown operation: {op}"))),
            },
            PositionUpdatePayload::Option(update) => match update.operation.as_str() {
                "upsert" => {
                    sqlx::query(
                        "INSERT INTO trading.target_option_positions \
                         (stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type, quantity, avg_price) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7::option_type, $8, $9, $10) \
                         ON CONFLICT (stock, primary_exchange, currency, strategy, expiry, strike, multiplier, option_type) \
                         DO UPDATE SET quantity = EXCLUDED.quantity, avg_price = EXCLUDED.avg_price",
                    )
                    .bind(&update.stock)
                    .bind(&update.primary_exchange)
                    .bind(&update.currency)
                    .bind(&update.strategy)
                    .bind(&update.expiry)
                    .bind(update.strike)
                    .bind(&update.multiplier)
                    .bind(&update.option_type)
                    .bind(update.quantity)
                    .bind(update.avg_price)
                    .execute(&state.db)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
                "delete" => {
                    sqlx::query(
                        "DELETE FROM trading.target_option_positions \
                         WHERE stock=$1 AND primary_exchange=$2 AND currency=$3 AND strategy=$4 \
                         AND expiry=$5 AND strike=$6 AND multiplier=$7 AND option_type=$8::option_type",
                    )
                    .bind(&update.stock)
                    .bind(&update.primary_exchange)
                    .bind(&update.currency)
                    .bind(&update.strategy)
                    .bind(&update.expiry)
                    .bind(update.strike)
                    .bind(&update.multiplier)
                    .bind(&update.option_type)
                    .execute(&state.db)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
                op => return Err((StatusCode::BAD_REQUEST, format!("Unknown operation: {op}"))),
            },
        }
    }

    match proxy_post(
        &Client::new(),
        "/positions/update",
        payloads.counter_proposal,
    )
    .await
    {
        Ok(resp) => {
            if resp.status() != StatusCode::OK {
                return Err((resp.status(), "Not sure what went wrong".to_string()));
            }
        }
        Err(e) => {
            return Err(e);
        }
    };

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "status": "updated" })),
    ))
}

// ── LLM service proxy helpers ──────────────────────────────────────────────

fn llm_url() -> String {
    std::env::var("LLM_SERVICE_URL").unwrap_or_else(|_| "http://llm_service:8001".into())
}

async fn proxy_post(
    client: &Client,
    path: &str,
    body: Value,
) -> Result<Response, (StatusCode, String)> {
    let url = format!("{}{}", llm_url(), path);
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body_bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    Ok((status, body_bytes).into_response())
}

pub async fn proxy_get(client: &Client, path: &str) -> Result<Response, (StatusCode, String)> {
    let url = format!("{}{}", llm_url(), path);
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok((status, bytes).into_response())
}

// ── Proxy handlers ─────────────────────────────────────────────────────────

async fn proxy_update_macro(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    let client = Client::new();
    proxy_post(&client, "/functions/update_macro", body).await
}

async fn proxy_update_industries(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    proxy_post(&Client::new(), "/functions/update_industries", body).await
}

async fn proxy_ticker_selector(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    proxy_post(&Client::new(), "/functions/ticker_selector", body).await
}

async fn proxy_idea_generator(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    proxy_post(&Client::new(), "/functions/idea_generator", body).await
}

async fn proxy_deep_dive(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    proxy_post(&Client::new(), "/functions/deep_dive", body).await
}

async fn proxy_positions_proposer(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    proxy_post(&Client::new(), "/functions/positions_proposer", body).await
}

async fn proxy_counter_proposer(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    proxy_post(
        &Client::new(),
        "/functions/positions_counter_proposer",
        body,
    )
    .await
}

async fn proxy_get_settings(State(_): State<AppState>) -> Result<Response, (StatusCode, String)> {
    proxy_get(&Client::new(), "/settings").await
}

async fn proxy_put_settings(
    State(_): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    let url = format!("{}/settings", llm_url());
    let resp = Client::new()
        .put(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok((status, bytes).into_response())
}

async fn proxy_kb_tree(State(_): State<AppState>) -> Result<Response, (StatusCode, String)> {
    proxy_get(&Client::new(), "/kb/tree").await
}

async fn proxy_kb_file(
    State(_): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response, (StatusCode, String)> {
    let path = params.get("path").map(|s| s.as_str()).unwrap_or("");
    let encoded = url::form_urlencoded::byte_serialize(path.as_bytes()).collect::<String>();
    proxy_get(&Client::new(), &format!("/kb/file?path={encoded}")).await
}

async fn proxy_patch_options_mode(
    State(_): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    let url = format!("{}/settings/options_mode", llm_url());
    let resp = Client::new()
        .patch(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok((status, bytes).into_response())
}

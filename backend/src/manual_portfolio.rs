// The run() function now accepts
// get_current_price,
// get_average_price,
// get_position_opened_date,
// get_position_size_usd,
// and get_capital_level as injected callables.
//
// You'll need to pass these in from your caller — the signatures are async (ticker: str) -> float for prices/sizes, async (ticker: str, direction: str) -> float for average price, and async () -> float for capital.
//
// get_average_price(ticker, direction) → avg cost basis for a position
// get_current_price(ticker) → live/last price
// get_capital_level() → total AUM or available capital (to size transaction cost impact)

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    models::{CurrentStockPositions, CurrentStockPositionsFullKeys},
    news::news_ideas::proxy_get,
};

// ── Router ─────────────────────────────────────────────────────────────────

pub fn router(state: AppState) -> Router {
    Router::new()
        // Alpaca call
        .route("/ticker/price", get(get_current_price))
        // DB calls
        .route("/strategy/ticker", get(get_strategy_stock_details))
        .route("/strategy/capital", get(get_strategy_capital))
        // .route("/current_stock_positions/strategy", get(get_curr_pos_strat))
        .route(
            "/counter_proposal/latest",
            get(proxy_get_latest_counter_proposal),
        )
        .route("/proposal/latest", get(proxy_get_latest_proposal))
        .route("/current_positions", get(get_current_positions))
        .route("/contracts/stock", get(get_possible_contracts))
        .route("/exchange_rate", get(get_exchange_rate))
        .with_state(state)
}

// ── Manual positions update ────────────────────────────────────────────────

// #[derive(Deserialize, Debug)]
// struct AlpacaTrade {
//     #[serde(rename = "p")]
//     price: f64,
//     #[serde(rename = "t")]
//     timestamp: String,
// }
//
// #[derive(Deserialize, Debug)]
// struct AlpacaLatestTradeResponse {
//     symbol: String,
//     trade: AlpacaTrade,
// }

// ── Handlers ───────────────────────────────────────────────────────────────

// async fn _get_current_price(
//     state: AppState,
//     ticker: String,
// ) -> Result<AlpacaLatestTradeResponse, (StatusCode, String)> {
//     let client = Client::new();
//     // Alpaca Market Data V2 endpoint for the latest trade
//     let url = format!(
//         "https://data.alpaca.markets/v2/stocks/{}/trades/latest",
//         ticker
//     );
//     let response = client
//         .get(&url)
//         .header("APCA-API-KEY-ID", &state.alpaca_key)
//         .header("APCA-API-SECRET-KEY", &state.alpaca_secret)
//         .send()
//         .await
//         .map_err(|e| {
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Request failed: {}", e),
//             )
//         })?;
//
//     if !response.status().is_success() {
//         let err_text = response.text().await.unwrap_or_default();
//         return Err((
//             StatusCode::BAD_GATEWAY,
//             format!("Alpaca API error: {}", err_text),
//         ));
//     }
//
//     response
//         .json::<AlpacaLatestTradeResponse>()
//         .await
//         .map_err(|e| {
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Deserialization failed: {}", e),
//             )
//         })
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Price {
    price: f64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContractDets {
    stock: String,
    primary_exchange: String,
    currency: String,
}
async fn _get_current_price(
    state: AppState,
    stock: String,
    primary_exchange: String,
    currency: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let client = Client::new();
    let url = format!("{}/contract/price", state.trading_bot_url);
    let response = client
        .get(&url)
        .query(&[
            ("stock", stock),
            ("primary_exchange", primary_exchange),
            ("currency", currency),
        ])
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Request to trading bot server failed: {}", e),
            )
        })?;

    if !response.status().is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Trading bot server returned an error: {}", err_text),
        ));
    }

    match response.json::<Price>().await {
        Ok(res) => Ok((StatusCode::OK, Json(serde_json::json!(res)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Deserialization failed: {}", e),
        )),
    }
}

async fn get_current_price(
    State(state): State<AppState>,
    Query(payload): Query<ContractDets>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    _get_current_price(
        state,
        payload.stock,
        payload.primary_exchange,
        payload.currency,
    )
    .await
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Ticker {
    strategy: String,
    stock: String,
}
async fn get_strategy_stock_details(
    State(state): State<AppState>,
    Query(stock): Query<Ticker>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let current_stock_position = sqlx::query_as!(
        CurrentStockPositionsFullKeys,
        "SELECT stock, primary_exchange, currency, strategy, quantity, avg_price, last_updated FROM trading.current_stock_positions \
         WHERE strategy=$1 AND stock=$2",
         &stock.strategy,
         &stock.stock
    )
    .fetch_one(&state.db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!(current_stock_position)),
    ))
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StrategyValue {
    sgd_value: f64,
}
// Should ideally use Strategy table when that is updated to be in sync
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Strategy {
    strategy: String,
}
async fn get_strategy_capital(
    State(state): State<AppState>,
    Query(strategy): Query<Strategy>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let client = Client::new();
    let url = format!("{}/strategy/capital", state.trading_bot_url);
    let response = client
        .get(&url)
        .query(&[("strategy", strategy.strategy)])
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Request to trading bot server failed: {}", e),
            )
        })?;

    if !response.status().is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Trading bot server returned an error: {}", err_text),
        ));
    }

    match response.json::<StrategyValue>().await {
        Ok(res) => Ok((StatusCode::OK, Json(serde_json::json!(res)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Deserialization failed: {}", e),
        )),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CurrencyVal {
    price: f64,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Currencies {
    currency: String,
    quote: String,
}
async fn get_exchange_rate(
    State(state): State<AppState>,
    Query(payload): Query<Currencies>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let client = Client::new();
    let url = format!("{}/exchange_rate", state.trading_bot_url);
    let response = client
        .get(&url)
        .query(&[("currency", payload.currency), ("quote", payload.quote)])
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Request to trading bot server failed: {}", e),
            )
        })?;

    if !response.status().is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Trading bot server returned an error: {}", err_text),
        ));
    }

    match response.json::<CurrencyVal>().await {
        Ok(res) => Ok((StatusCode::OK, Json(serde_json::json!(res)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Deserialization failed: {}", e),
        )),
    }
}

// async fn get_curr_pos_strat(
//     State(state): State<AppState>,
//     Query(stock): Query<Strategy>,
// ) -> Result<impl IntoResponse, (StatusCode, String)> {
//     let current_stock_positions = sqlx::query_as!(
//         CurrentStockPositionsFullKeys,
//         "SELECT stock, primary_exchange, currency, strategy, quantity, avg_price, last_updated FROM trading.current_stock_positions \
//          WHERE strategy=$1",
//          &stock.strategy,
//     )
//     .fetch_all(&state.db)
//     .await
//     .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
//
//     Ok((
//         StatusCode::OK,
//         Json(serde_json::json!(current_stock_positions)),
//     ))
// }

async fn proxy_get_latest_counter_proposal(
    State(_): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    proxy_get(&Client::new(), "/positions").await
}

async fn proxy_get_latest_proposal(
    State(_): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    proxy_get(&Client::new(), "/proposal/latest").await
}

#[derive(Deserialize, Serialize)]
pub struct CurrentStockPositionsWPrice {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub strategy: String,
    pub avg_price: f64,
    pub quantity: f64,
    pub current_price: f64,
    pub last_updated: DateTime<Utc>,
}

async fn get_current_positions(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let res_raw = sqlx::query_as!(
        CurrentStockPositions,
        "SELECT * FROM trading.current_stock_positions \
         WHERE strategy = 'manual'",
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let res = join_all(
        res_raw
            .iter()
            .filter(|pos| pos.stock.strip_prefix("CASH:").is_none())
            .filter(|pos| pos.stock.strip_prefix("FX:").is_none())
            .map(async |pos| {
                let client = Client::new();
                let url = format!("{}/contract/price", state.trading_bot_url);
                let raw_response = client
                    .get(&url)
                    .query(&[
                        ("stock", pos.stock.clone()),
                        ("primary_exchange", pos.primary_exchange.clone()),
                        ("currency", pos.currency.clone()),
                    ])
                    .send()
                    .await
                    .ok();
                if raw_response.is_none() {
                    return None;
                }
                let response = raw_response.unwrap();

                if !response.status().is_success() {
                    // let err_text = response.text().await.unwrap_or_default();
                    return None;
                    // return Err((
                    //     StatusCode::BAD_GATEWAY,
                    //     format!("Trading bot server returned an error: {}", err_text),
                    // ));
                }

                #[derive(Deserialize, Serialize, Debug, Clone)]
                struct PriceResp {
                    price: f64,
                }
                Some(CurrentStockPositionsWPrice {
                    current_price: response.json::<PriceResp>().await.unwrap().price,
                    stock: pos.stock.clone(),
                    primary_exchange: pos.primary_exchange.clone(),
                    currency: pos.currency.clone(),
                    strategy: pos.strategy.clone(),
                    quantity: pos.quantity.unwrap_or(0.0),
                    avg_price: pos.avg_price.unwrap_or(0.0),
                    last_updated: pos.last_updated.unwrap_or_default(),
                })
            }),
    )
    .await;
    if res.iter().all(|v| v.is_some()) {
        Ok((StatusCode::OK, Json(serde_json::json!(res))))
    } else {
        Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to fetch from trading-bot".to_string(),
        ))
    }
}

// async fn get_current_capital(
//     State(state): State<AppState>,
// ) -> Result<impl IntoResponse, (StatusCode, String)> {
//     let res = sqlx::query_as!(
//         CurrentStockPositions,
//         "SELECT * FROM trading.current_stock_positions \
//          WHERE strategy = 'manual'",
//     )
//     .fetch_all(&state.db)
//     .await
//     .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
//
//     Ok((StatusCode::OK, Json(serde_json::json!(res))))
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NakedContract {
    stock: String,
    primary_exchange: String,
    currency: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MinimalContract {
    stock: String,
    primary_exchange: String,
    currency: String,
    current_price: f64,
}

async fn get_possible_contracts(
    State(state): State<AppState>,
    Query(payload): Query<NakedContract>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let client = Client::new();
    let url = format!("{}/contracts/stock", state.trading_bot_url);
    let response = client
        .get(&url)
        .query(&[
            ("stock", payload.stock),
            ("primary_exchange", payload.primary_exchange),
            ("currency", payload.currency),
        ])
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Request to trading bot server failed: {}", e),
            )
        })?;

    if !response.status().is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Trading bot server returned an error: {}", err_text),
        ));
    }

    match response.json::<Vec<MinimalContract>>().await {
        Ok(res) => Ok((StatusCode::OK, Json(serde_json::json!(res)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Deserialization failed: {}", e),
        )),
    }
}

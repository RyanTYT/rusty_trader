// main.rs
use axum::{
    Json, Router,
    extract::State,
    extract::ws::{Message, WebSocket},
    http::Request,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{any, delete, get, post, put},
};
use futures::stream::SplitSink;
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use sqlx::{
    PgPool, Postgres,
    postgres::{PgArguments, PgPoolOptions},
    query::QueryAs,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;

mod core;
mod manual_portfolio;
mod models;
mod news;

use core::crud::CRUDTrait as _;
use http::header::CONTENT_TYPE;
use reqwest::Client;
use tower_http::cors::{Any, CorsLayer};

use crate::{
    core::{
        crud_impl::*,
        strategy::{pause_strategy, resume_strategy},
        websocket::{broadcast_to_websocket, ws_handler},
    },
    news::news_ideas,
};

#[async_trait::async_trait]
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

#[derive(Clone)]
struct AppState {
    auth_token: Arc<String>,
    db: PgPool,
    clients: Arc<Mutex<HashMap<Uuid, Arc<Mutex<SplitSink<WebSocket, Message>>>>>>,
    trading_bot_url: String,
    // alpaca_key: String,
    // alpaca_secret: String, // pub notification_tx: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "backend=debug,tower_http=debug".into()),
        )
        .try_init()
    {
        eprintln!("Failed to initialise tracing_subscriber");
    };
    eprintln!("Finished initialising tracing_subscriber");
    tracing::info!("Backend service starting");

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let trading_bot_url = std::env::var("TRADING_BOT_URL").expect("TRADING_BOT_URL must be set");
    let bearer_token = std::env::var("BEARER_TOKEN").expect("BEARER_TOKEN must be set");
    let server_host = std::env::var("SERVER_HOST").expect("SERVER_HOST must be set");
    let alpaca_api_key = std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY must be set");
    let alpaca_api_secret =
        std::env::var("ALPACA_API_SECRET").expect("ALPACA_API_SECRET must be set");

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any)
        .allow_headers([CONTENT_TYPE]);

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");

    // let (notification_tx, _) = broadcast::channel::<String>(64);

    let state = AppState {
        auth_token: Arc::new(bearer_token),
        db,
        clients: Arc::new(Mutex::new(HashMap::new())),
        trading_bot_url,
        // alpaca_key: alpaca_api_key,
        // alpaca_secret: alpaca_api_secret,
        // notification_tx
    };

    let auth_routes = Router::new()
        .route("/send_notification", post(broadcast_notification))
        .route("/send/positions_mismatch", post(positions_mismatch_alert))
        .route("/current_position/fix", post(fix_current_positions))
        .route(
            "/get_portfolio/strategy",
            get(get_portfolio_value_for_strategy),
        )
        .route("/get_portfolio", get(get_overall_portfolio_value))
        .route("/strategy/pause", post(pause_strategy))
        .route("/strategy/resume", post(resume_strategy))
        .route("/account/pause", post(pause_account))
        .route("/strategy", post(create_strategy))
        .route("/strategy", get(read_strategy))
        .route("/strategy/all", get(read_all_strategy))
        .route("/strategy", put(update_strategy))
        .route("/strategy", delete(delete_strategy))
        .route("/logs", get(crate::core::logs::list_logs))
        .route("/logs/:filename", get(crate::core::logs::read_log))
        .route(
            "/current_stock_positions",
            post(create_current_stock_positions),
        )
        .route(
            "/current_stock_positions",
            get(read_current_stock_positions),
        )
        .route(
            "/current_stock_positions/all",
            get(read_all_current_stock_positions),
        )
        .route(
            "/current_stock_positions",
            put(update_current_stock_positions),
        )
        .route(
            "/current_stock_positions",
            delete(delete_current_stock_positions),
        )
        .route(
            "/current_option_positions",
            post(create_current_option_positions),
        )
        .route(
            "/current_option_positions",
            get(read_current_option_positions),
        )
        .route(
            "/current_option_positions/all",
            get(read_all_current_option_positions),
        )
        .route(
            "/current_option_positions",
            put(update_current_option_positions),
        )
        .route(
            "/current_option_positions",
            delete(delete_current_option_positions),
        )
        .route(
            "/target_stock_positions",
            post(create_target_stock_positions),
        )
        .route("/target_stock_positions", get(read_target_stock_positions))
        .route(
            "/target_stock_positions/all",
            get(read_all_target_stock_positions),
        )
        .route(
            "/target_stock_positions",
            put(update_target_stock_positions),
        )
        .route(
            "/target_stock_positions",
            delete(delete_target_stock_positions),
        )
        .route(
            "/target_option_positions",
            post(create_target_option_positions),
        )
        .route(
            "/target_option_positions",
            get(read_target_option_positions),
        )
        .route(
            "/target_option_positions/all",
            get(read_all_target_option_positions),
        )
        .route(
            "/target_option_positions",
            put(update_target_option_positions),
        )
        .route(
            "/target_option_positions",
            delete(delete_target_option_positions),
        )
        .route("/open_stock_orders", post(create_open_stock_orders))
        .route("/open_stock_orders", get(read_open_stock_orders))
        .route("/open_stock_orders/all", get(read_all_open_stock_orders))
        .route("/open_stock_orders", put(update_open_stock_orders))
        .route("/open_stock_orders", delete(delete_open_stock_orders))
        .route("/open_option_orders", post(create_open_option_orders))
        .route("/open_option_orders", get(read_open_option_orders))
        .route("/open_option_orders/all", get(read_all_open_option_orders))
        .route("/open_option_orders", put(update_open_option_orders))
        .route("/open_option_orders", delete(delete_open_option_orders))
        .route("/stock_transactions", post(create_stock_transactions))
        .route("/stock_transactions", get(read_stock_transactions))
        .route("/stock_transactions/all", get(read_all_stock_transactions))
        .route("/stock_transactions", put(update_stock_transactions))
        .route("/stock_transactions", delete(delete_stock_transactions))
        .route("/option_transactions", post(create_option_transactions))
        .route("/option_transactions", get(read_option_transactions))
        .route(
            "/option_transactions/all",
            get(read_all_option_transactions),
        )
        .route("/option_transactions", put(update_option_transactions))
        .route("/option_transactions", delete(delete_option_transactions))
        .route("/historical_data", post(create_historical_data))
        .route("/historical_data", get(read_historical_data))
        .route("/historical_data/all", get(read_all_historical_data))
        .route("/historical_data", put(update_historical_data))
        .route("/historical_data", delete(delete_historical_data))
        // .route("/historical_volatility_data", post(create_historical_volatility_data))
        // .route("/historical_volatility_data", get(read_historical_volatility_data))
        // .route("/historical_volatility_data/all", get(read_all_historical_volatility_data))
        // .route("/historical_volatility_data", put(update_historical_volatility_data))
        // .route("/historical_volatility_data", delete(delete_historical_volatility_data))
        .route(
            "/historical_options_data",
            post(create_historical_options_data),
        )
        .route(
            "/historical_options_data",
            get(read_historical_options_data),
        )
        .route(
            "/historical_options_data/all",
            get(read_all_historical_options_data),
        )
        .route(
            "/historical_options_data",
            put(update_historical_options_data),
        )
        .route(
            "/historical_options_data",
            delete(delete_historical_options_data),
        )
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let news_ideas_routes = news_ideas::router(state.clone());

    let ticker_routes = manual_portfolio::router(state.clone());
    //     .layer(
    //     axum::middleware::from_fn_with_state(state.clone(), auth_middleware),
    // );

    let public_routes = Router::new()
        .route("/check-health", any(check_health))
        .route("/ws", any(ws_handler))
        .with_state(state.clone());

    let app = public_routes
        .merge(auth_routes)
        .merge(news_ideas_routes)
        .merge(ticker_routes)
        .layer(cors);

    // run it with hyper
    let listener = tokio::net::TcpListener::bind(format!("{}:3000", server_host))
        .await
        .unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn check_health() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({ "status": "ok" })),
    )
}

async fn auth_middleware(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    let expected_token = format!("Bearer {}", state.auth_token);

    match request.headers().get("Authorization") {
        Some(hv) if hv.to_str().unwrap_or("invalid") == expected_token => {
            Ok(next.run(request).await)
        }
        _ => Err((StatusCode::UNAUTHORIZED, "Invalid or missing token")),
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TauriNotification {
    pub title: String,
    pub body: String,
    pub alert_type: String, // "news_ideas" | "error" | "info"
    pub function: String,   // Which LLM function triggered this
    pub timestamp: String,
}

async fn broadcast_notification(
    State(state): State<AppState>,
    // Json(payload): Json<models::NotificationFullKeys>,
    Json(payload): Json<models::NotificationFullKeys>,
) -> impl IntoResponse {
    let json_notification = match serde_json::to_string(&serde_json::json!({
        "type": "notification",
        "payload": TauriNotification {
            title: payload.title,
            body: payload.body,
            alert_type: payload.alert_type,
            function: payload.function,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    })) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to serialize notification".into_response(),
            );
        }
    };

    broadcast_to_websocket(state, json_notification).await
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct Quantity {
    pub quantity: f64,
    pub strategy: String,
}

// VERY BAD FUNCTION CURRENTLY
async fn positions_mismatch_alert(
    State(state): State<AppState>,
    Json(broker_positions): Json<HashMap<String, f64>>,
) -> (StatusCode, Response<axum::body::Body>) {
    let mut mismatched_positions = HashMap::<String, Vec<models::MismatchedPosition>>::new();
    for (stock, broker_position) in broker_positions.iter() {
        let sql = format!(
            "SELECT SUM(quantity) AS quantity, strategy FROM trading.current_positions WHERE stock={} GROUP BY strategy",
            stock
        );
        let query = sqlx::query_as::<_, Quantity>(&sql);
        let result = query.fetch_all(&state.db).await;
        match result {
            Ok(local_positions) => {
                local_positions.iter().for_each(|strategy_position| {
                    mismatched_positions
                        .entry(stock.clone())
                        .or_insert_with(Vec::new)
                        .push(models::MismatchedPosition {
                            strategy: strategy_position.strategy.clone(),
                            broker: *broker_position,
                            local: strategy_position.quantity,
                            fix: strategy_position.quantity,
                        });
                });
            }
            Err(_error) => {
                println!("ERROR IN POSITIONS MISMATCH ALERT")
            }
        }
    }

    broadcast_to_websocket(state, serde_json::to_string(&mismatched_positions).unwrap()).await
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
struct PauseAccount {
    graceful: bool,
}

async fn pause_account(
    State(state): State<AppState>,
    Json(pause_account_details): Json<PauseAccount>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let status = if pause_account_details.graceful {
        "Stopping Gracefully"
    } else {
        "Inactive"
    };
    sqlx::query("UPDATE trading.strategy SET status = $1")
        .bind(status)
        .execute(&state.db)
        .await
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error occurred during update-all-orders request: {}", err),
            )
        })?;

    let url = format!("{}/update-all-orders", state.trading_bot_url);

    let client = Client::new();
    let response_unparsed = client
        .post(url)
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error occurred during update-all-orders request: {}", err),
            )
        })?;

    response_unparsed.error_for_status().map_err(|err| {
        (
            err.status()
                .unwrap_or_else(|| StatusCode::INTERNAL_SERVER_ERROR),
            format!(
                "Error occurred during update-all-orders request: {}",
                err.to_string()
            ),
        )
    })?;

    Ok(((StatusCode::OK), "Paused Account Accordingly!"))
}
async fn fix_current_positions(
    State(state): State<AppState>,
    Json(mismatched_positions): Json<
        HashMap<(String, String, String), Vec<models::MismatchedPosition>>,
    >,
) -> impl IntoResponse {
    let current_position_crud =
        core::crud::CRUD::<
            models::CurrentStockPositionsFullKeys,
            models::CurrentStockPositionsPrimaryKeys,
            models::CurrentStockPositionsUpdateKeys,
        >::new(state.db.clone(), "trading.current_positions".to_string());
    for (stock_and_pri_exch, mismatched_position) in &mismatched_positions {
        for mismatched_position_strategy in mismatched_position {
            let primary_keys = models::CurrentStockPositionsPrimaryKeys {
                stock: stock_and_pri_exch.0.clone(),
                primary_exchange: stock_and_pri_exch.1.clone(),
                currency: stock_and_pri_exch.2.clone(),
                strategy: mismatched_position_strategy.strategy.clone(),
            };
            let update_keys = models::CurrentStockPositionsUpdateKeys {
                quantity: Some(mismatched_position_strategy.fix).clone(),
                avg_price: None,
                last_updated: None,
            };
            if let Err(err) = current_position_crud
                .update(&primary_keys, &update_keys)
                .await
            {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error when sending message to client: {}", err).into_response(),
                );
            }
        }
    }

    broadcast_to_websocket(state, "Current Positions Mismatch Updated!".to_string()).await
}

#[derive(Deserialize, Serialize)]
pub struct StrategyQueryParams {
    strategy: String,
    cutoff: u64,
}

async fn get_portfolio_value_for_strategy(
    State(state): State<AppState>,
    axum::extract::Query(strategy_query_params): axum::extract::Query<StrategyQueryParams>,
) -> Result<
    (
        StatusCode,
        Json<core::portfolio_values::PortfolioValueStrategy>,
    ),
    (StatusCode, String),
> {
    match core::portfolio_values::compute_portfolio_value_for_strategy(
        state,
        core::portfolio_values::Strategy {
            strategy: strategy_query_params.strategy,
        },
        strategy_query_params.cutoff,
        true,
    )
    .await
    {
        Ok(res) => Ok((StatusCode::OK, res)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

#[derive(Deserialize, Serialize)]
pub struct PortfolioValueParams {
    cutoff: u64,
}
async fn get_overall_portfolio_value(
    State(state): State<AppState>,
    axum::extract::Query(portfolio_value_params): axum::extract::Query<PortfolioValueParams>,
) -> Result<(StatusCode, Json<core::portfolio_values::PortfolioValue>), (StatusCode, String)> {
    match core::portfolio_values::compute_overall_portfolio_value(
        state,
        portfolio_value_params.cutoff,
    )
    .await
    {
        Ok(res) => Ok((StatusCode::OK, res)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

make_crud_handlers!(
    create_strategy,
    read_strategy,
    read_all_strategy,
    update_strategy,
    delete_strategy,
    models::StrategyFullKeys,
    models::StrategyPrimaryKeys,
    models::StrategyUpdateKeys,
    "trading.strategy"
);
make_crud_handlers!(
    create_current_stock_positions,
    read_current_stock_positions,
    read_all_current_stock_positions,
    update_current_stock_positions,
    delete_current_stock_positions,
    models::CurrentStockPositionsFullKeys,
    models::CurrentStockPositionsPrimaryKeys,
    models::CurrentStockPositionsUpdateKeys,
    "trading.current_stock_positions"
);
make_crud_handlers!(
    create_current_option_positions,
    read_current_option_positions,
    read_all_current_option_positions,
    update_current_option_positions,
    delete_current_option_positions,
    models::CurrentOptionPositionsFullKeys,
    models::CurrentOptionPositionsPrimaryKeys,
    models::CurrentOptionPositionsUpdateKeys,
    "trading.current_option_positions"
);
make_crud_handlers!(
    create_target_stock_positions,
    read_target_stock_positions,
    read_all_target_stock_positions,
    update_target_stock_positions,
    delete_target_stock_positions,
    models::TargetStockPositionsFullKeys,
    models::TargetStockPositionsPrimaryKeys,
    models::TargetStockPositionsUpdateKeys,
    "trading.target_stock_positions"
);
make_crud_handlers!(
    create_target_option_positions,
    read_target_option_positions,
    read_all_target_option_positions,
    update_target_option_positions,
    delete_target_option_positions,
    models::TargetOptionPositionsFullKeys,
    models::TargetOptionPositionsPrimaryKeys,
    models::TargetOptionPositionsUpdateKeys,
    "trading.target_option_positions"
);
make_crud_handlers!(
    create_open_stock_orders,
    read_open_stock_orders,
    read_all_open_stock_orders,
    update_open_stock_orders,
    delete_open_stock_orders,
    models::OpenStockOrdersFullKeys,
    models::OpenStockOrdersPrimaryKeys,
    models::OpenStockOrdersUpdateKeys,
    "trading.open_stock_orders"
);
make_crud_handlers!(
    create_open_option_orders,
    read_open_option_orders,
    read_all_open_option_orders,
    update_open_option_orders,
    delete_open_option_orders,
    models::OpenOptionOrdersFullKeys,
    models::OpenOptionOrdersPrimaryKeys,
    models::OpenOptionOrdersUpdateKeys,
    "trading.open_option_orders"
);
make_crud_handlers!(
    create_stock_transactions,
    read_stock_transactions,
    read_all_stock_transactions,
    update_stock_transactions,
    delete_stock_transactions,
    models::StockTransactionsFullKeys,
    models::StockTransactionsPrimaryKeys,
    models::StockTransactionsUpdateKeys,
    "trading.stock_transactions"
);
make_crud_handlers!(
    create_option_transactions,
    read_option_transactions,
    read_all_option_transactions,
    update_option_transactions,
    delete_option_transactions,
    models::OptionTransactionsFullKeys,
    models::OptionTransactionsPrimaryKeys,
    models::OptionTransactionsUpdateKeys,
    "trading.option_transactions"
);
make_crud_handlers!(
    create_historical_data,
    read_historical_data,
    read_all_historical_data,
    update_historical_data,
    delete_historical_data,
    models::HistoricalDataFullKeys,
    models::HistoricalDataPrimaryKeys,
    models::HistoricalDataUpdateKeys,
    "market_data.historical_data"
);
// make_crud_handlers!(
//     create_historical_volatility_data,
//     read_historical_volatility_data,
//     read_all_historical_volatility_data,
//     update_historical_volatility_data,
//     delete_historical_volatility_data,
//     models::HistoricalVolatilityDataFullKeys,
//     models::HistoricalVolatilityDataPrimaryKeys,
//     models::HistoricalVolatilityDataUpdateKeys,
//     "market_data.historical_volatility_data"
// );
make_crud_handlers!(
    create_historical_options_data,
    read_historical_options_data,
    read_all_historical_options_data,
    update_historical_options_data,
    delete_historical_options_data,
    models::HistoricalOptionsDataFullKeys,
    models::HistoricalOptionsDataPrimaryKeys,
    models::HistoricalOptionsDataUpdateKeys,
    "market_data.historical_options_data"
);

// main.rs
use axum::{
    Json, Router,
    extract::{State, Query},
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::{IntoResponse,Response},
    routing::{get, post, put, delete, any},
    http::Request,
    middleware::Next,
};
use http::{StatusCode, Method};
use sqlx::{postgres::{PgArguments, PgPoolOptions}, query::QueryAs, PgPool, Postgres};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
mod crud;
mod crud_impl;
// mod models;
use crud::CRUDTrait as _;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use http::header::CONTENT_TYPE;
use tower_http::cors::{Any, CorsLayer};
// use futures::future::join_all;
use reqwest::Client;

mod models;
mod portfolio_values;
mod logs;

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
    client: Arc<Mutex<Option<WebSocket>>>
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let bearer_token = std::env::var("BEARER_TOKEN").expect("BEARER_TOKEN must be set");
    let server_host = std::env::var("SERVER_HOST").expect("SERVER_HOST must be set");

    let cors = CorsLayer::new()
       .allow_methods([Method::GET, Method::POST])
       .allow_origin(Any)
       .allow_headers([CONTENT_TYPE]);


    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");

    let state = AppState {
        auth_token: Arc::new(bearer_token),
        db,
        client: Arc::new(Mutex::new(None))
    };

    let auth_routes = Router::new()
        .route("/send_notification", post(send_notification))

        .route("/send/positions_mismatch", post(positions_mismatch_alert))
        .route("/current_position/fix", post(fix_current_positions))

        .route("/get_portfolio/strategy", get(get_portfolio_value_for_strategy))
        .route("/get_portfolio", get(get_overall_portfolio_value))

        .route("/strategy/pause", post(pause_strategy))
        .route("/strategy/resume", post(resume_strategy))
        .route("/account/pause", post(pause_account))

        .route("/strategy", post(create_strategy))
        .route("/strategy", get(read_strategy))
        .route("/strategy/all", get(read_all_strategy))
        .route("/strategy", put(update_strategy))
        .route("/strategy", delete(delete_strategy))

        .route("/logs", get(crate::logs::list_logs))
        .route("/logs/:filename", get(crate::logs::read_log))

        .route("/current_stock_positions", post(create_current_stock_positions))
        .route("/current_stock_positions", get(read_current_stock_positions))
        .route("/current_stock_positions/all", get(read_all_current_stock_positions))
        .route("/current_stock_positions", put(update_current_stock_positions))
        .route("/current_stock_positions", delete(delete_current_stock_positions))

        .route("/current_option_positions", post(create_current_option_positions))
        .route("/current_option_positions", get(read_current_option_positions))
        .route("/current_option_positions/all", get(read_all_current_option_positions))
        .route("/current_option_positions", put(update_current_option_positions))
        .route("/current_option_positions", delete(delete_current_option_positions))

        .route("/target_stock_positions", post(create_target_stock_positions))
        .route("/target_stock_positions", get(read_target_stock_positions))
        .route("/target_stock_positions/all", get(read_all_target_stock_positions))
        .route("/target_stock_positions", put(update_target_stock_positions))
        .route("/target_stock_positions", delete(delete_target_stock_positions))

        .route("/target_option_positions", post(create_target_option_positions))
        .route("/target_option_positions", get(read_target_option_positions))
        .route("/target_option_positions/all", get(read_all_target_option_positions))
        .route("/target_option_positions", put(update_target_option_positions))
        .route("/target_option_positions", delete(delete_target_option_positions))

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
        .route("/option_transactions/all", get(read_all_option_transactions))
        .route("/option_transactions", put(update_option_transactions))
        .route("/option_transactions", delete(delete_option_transactions))

        .route("/historical_data", post(create_historical_data))
        .route("/historical_data", get(read_historical_data))
        .route("/historical_data/all", get(read_all_historical_data))
        .route("/historical_data", put(update_historical_data))
        .route("/historical_data", delete(delete_historical_data))

        .route("/historical_volatility_data", post(create_historical_volatility_data))
        .route("/historical_volatility_data", get(read_historical_volatility_data))
        .route("/historical_volatility_data/all", get(read_all_historical_volatility_data))
        .route("/historical_volatility_data", put(update_historical_volatility_data))
        .route("/historical_volatility_data", delete(delete_historical_volatility_data))

        .route("/historical_options_data", post(create_historical_options_data))
        .route("/historical_options_data", get(read_historical_options_data))
        .route("/historical_options_data/all", get(read_all_historical_options_data))
        .route("/historical_options_data", put(update_historical_options_data))
        .route("/historical_options_data", delete(delete_historical_options_data))

        .route("/phantom_portfolio_value", post(create_phantom_portfolio_value))
        .route("/phantom_portfolio_value", get(read_phantom_portfolio_value))
        .route("/phantom_portfolio_value/all", get(read_all_phantom_portfolio_value))
        .route("/phantom_portfolio_value", put(update_phantom_portfolio_value))
        .route("/phantom_portfolio_value", delete(delete_phantom_portfolio_value))

        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware));

    let public_routes = Router::new()
        .route("/check-health", any(check_health))
        .route("/ws", any(ws_handler))
        .with_state(state.clone());

    let app = public_routes
        .merge(auth_routes)
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
    (StatusCode::OK, axum::Json(serde_json::json!({ "status": "ok" })))
}

async fn auth_middleware(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    let expected_token = format!("Bearer {}", state.auth_token);

    match request.headers().get("Authorization") {
        Some(hv) if hv.to_str().unwrap_or("invalid") == expected_token => Ok(next.run(request).await),
        _ => Err((StatusCode::UNAUTHORIZED, "Invalid or missing token")),
    }
}

#[derive(serde::Deserialize)]
struct WsQuery {
    token: String,
}

async fn ws_handler(
    ws: WebSocketUpgrade, 
    Query(WsQuery { token }): Query<WsQuery>, 
    State(state): State<AppState>
) -> impl IntoResponse {
    let expected_token = format!("Bearer {}", state.auth_token);
    if token != expected_token {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.on_upgrade(|web_socket| {insert_client(web_socket, state)})
}

async fn insert_client(mut socket: WebSocket, state: AppState) {
    let mut client_guard = state.client.lock().await;
    socket.send(Message::Text("Hello bb".into())).await.ok();
    client_guard.replace(socket);
}

async fn send_notification(
    State(state): State<AppState>,
    Json(payload): Json<models::NotificationFullKeys>,
) -> impl IntoResponse {
    let notification = &payload;

    // Get the client
    let mut client_guard = state.client.lock().await;
    let client_optional = client_guard.as_mut();

    // only if client exists
    if let Some(client) = client_optional {
         let json_notification = match serde_json::to_string(notification) {
            Ok(s) => s,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to serialize notification".into_response(),
                );
            }
        };

        match client.send(Message::Text(json_notification)).await {
            Ok(_) => return (StatusCode::OK, "Notification passed along!".into_response()),
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error when sending message to client: {}", err).into_response(),
                );
            }
        } 
    } else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Client not connected yet!".into_response(),
        );
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct Quantity {
    pub quantity: f64,
    pub strategy: String,
}

// VERY BAD FUNCTION CURRENTLY
async fn positions_mismatch_alert(
    State(state): State<AppState>, 
    Json(broker_positions): Json<HashMap<String, f64>>
) {
    let mut mismatched_positions = HashMap::<String, Vec<models::MismatchedPosition>>::new();
    for (stock, broker_position) in  broker_positions.iter() {
        let sql = format!("SELECT SUM(quantity) AS quantity, strategy FROM trading.current_positions WHERE stock={} GROUP BY strategy", stock);
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
                            fix: strategy_position.quantity
                        });
                });
            },
            Err(_error) => {
                println!("ERROR IN POSITIONS MISMATCH ALERT")
            }
        }
    };

    // Get the client
    let mut client_guard = state.client.lock().await;
    let client_optional = client_guard.as_mut();

    // only if client exists
    if let Some(client) = client_optional {
        match client.send(serde_json::to_string(&mismatched_positions).unwrap().into()).await {
            Ok(_) => {},
            Err(_error) => {println!("ERROR");}
        };
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Client not connected yet!".into_response(),
        );
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
struct PauseAccount{
    graceful: bool
}

async fn pause_account(
    State(state): State<AppState>,
    Json(pause_account_details): Json<PauseAccount>
   ) -> Result<impl IntoResponse, (StatusCode, String)> {
    let status = if pause_account_details.graceful{ "Stopping Gracefully" } else { "Inactive" };
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

    let url = format!(
        "http://{}/update-all-orders",
        env!("TRADING_BOT_URL")
    );

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
                .unwrap_or_else(|| StatusCode::INTERNAL_SERVER_ERROR)
                ,
            format!("Error occurred during update-all-orders request: {}", err.to_string()),
        )
    })?;

    Ok((
        (StatusCode::OK),
        "Paused Account Accordingly!"
    ))
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
struct PauseStrategy{
    strategy: String,
    graceful: bool
}

async fn pause_strategy(
    State(state): State<AppState>,
    Json(pause_strategy_details): Json<PauseStrategy>
   ) -> Result<impl IntoResponse, (StatusCode, String)> {
    let strategy_crud = crud::CRUD::<models::StrategyFullKeys, models::StrategyPrimaryKeys, models::StrategyUpdateKeys>::new(state.db.clone(), "trading.strategy".to_string());

    if pause_strategy_details.graceful{
        strategy_crud.update(&models::StrategyPrimaryKeys{
            strategy: pause_strategy_details.strategy
        }, &models::StrategyUpdateKeys{
            capital: None,
            initial_capital: None,
            status: Some(models::Status::Stopping)
        }).await.map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to update the Strategy Database: {}", err)
             ).into()
        })?;
    } else {
        strategy_crud.update(&models::StrategyPrimaryKeys{
            strategy: pause_strategy_details.strategy
        }, &models::StrategyUpdateKeys{
            capital: None,
            initial_capital: None,
            status: Some(models::Status::Inactive)
        }).await.map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to update the Strategy Database: {}", err)
             ).into()
        })?;
    }

    let url = format!(
        "http://{}/update-all-orders",
        env!("TRADING_BOT_URL")
    );

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
            ).into()
        })?;

    let response = response_unparsed.error_for_status().map_err(|err| {
        (
            err.status()
                .unwrap_or_else(|| StatusCode::INTERNAL_SERVER_ERROR)
                ,
            format!("Error occurred during update-all-orders request: {}", err.to_string()),
        ).into()
    })?;

    Ok((
        (StatusCode::OK),
        "Paused Strategy Accordingly!"
    ))
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
struct ResumeStrategy{
    strategy: String,
}

async fn resume_strategy(
    State(state): State<AppState>,
    Json(resume_strategy_details): Json<ResumeStrategy>
   ) -> Result<impl IntoResponse, (StatusCode, String)> {
    let strategy_crud = crud::CRUD::<models::StrategyFullKeys, models::StrategyPrimaryKeys, models::StrategyUpdateKeys>::new(state.db.clone(), "trading.strategy".to_string());

    strategy_crud.update(&models::StrategyPrimaryKeys{
        strategy: resume_strategy_details.strategy
    }, &models::StrategyUpdateKeys{
        capital: None,
        initial_capital: None,
        status: Some(models::Status::Active)
    }).await.map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to update the Strategy Database: {}", err)
         ).into()
    })?;

    let url = format!(
        "http://{}/update-all-orders",
        env!("TRADING_BOT_URL")
    );

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
            ).into()
        })?;

    let response = response_unparsed.error_for_status().map_err(|err| {
        (
            err.status()
                .unwrap_or_else(|| StatusCode::INTERNAL_SERVER_ERROR)
                ,
            format!("Error occurred during update-all-orders request: {}", err.to_string()),
        ).into()
    })?;

    Ok((
        (StatusCode::OK),
        "Paused Strategy Accordingly!"
    ))
}

async fn fix_current_positions(
    State(state): State<AppState>,
    Json(mismatched_positions): Json<HashMap<(String, String), Vec<models::MismatchedPosition>>>,
) -> impl IntoResponse {

    let current_position_crud = crud::CRUD::<models::CurrentStockPositionsFullKeys, models::CurrentStockPositionsPrimaryKeys, models::CurrentStockPositionsUpdateKeys>::new(state.db.clone(), "trading.current_positions".to_string());
    for (stock_and_pri_exch, mismatched_position) in &mismatched_positions {
        for mismatched_position_strategy in mismatched_position {
            let primary_keys = models::CurrentStockPositionsPrimaryKeys {
                stock: stock_and_pri_exch.0.clone(),
                primary_exchange: stock_and_pri_exch.1.clone(),
                strategy: mismatched_position_strategy.strategy.clone(),
            };
            let update_keys = models::CurrentStockPositionsUpdateKeys {
                quantity: Some(mismatched_position_strategy.fix).clone(),
                avg_price: None,
            };
            if let Err(err) = current_position_crud.update(&primary_keys, &update_keys).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error when sending message to client: {}", err).into_response(),
                );
            }
        }
    }

    // Get the client
    let mut client_guard = state.client.lock().await;
    let client_optional = client_guard.as_mut();

    // only if client exists
    if let Some(client) = client_optional {
        match client.send(Message::Text("Current Positions Mismatch Updated!".to_string())).await {
            Ok(_) => return (StatusCode::OK, "Notification passed along!".into_response()),
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error when sending message to client: {}", err).into_response(),
                );
            }
        } 
    } else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Client not connected yet!".into_response(),
        );
    }
}

async fn get_portfolio_value_for_strategy(
    State(state): State<AppState>,
    axum::extract::Query(strategy): axum::extract::Query<portfolio_values::Strategy>,
) ->  Result<(StatusCode, Json<portfolio_values::PortfolioValueStrategy>), (StatusCode, String)>{
    match portfolio_values::compute_portfolio_value_for_strategy(state, strategy).await {
        Ok(res) => Ok((StatusCode::OK, res)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e))
    }
}

async fn get_overall_portfolio_value(
    State(state): State<AppState>,
) ->  Result<(StatusCode, Json<portfolio_values::PortfolioValue>), (StatusCode, String)>{
    match portfolio_values::compute_overall_portfolio_value(state).await {
        Ok(res) => Ok((StatusCode::OK, res)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e))
    }
}

macro_rules! make_crud_handlers {
    (
        $create_name:ident,
        $read_name: ident,
        $read_all_name: ident,
        $update_name: ident,
        $delete_name: ident,
        $full_ty:ty, 
        $primary_ty:ty, 
        $update_ty:ty, 
        $table:expr
     ) => {
        crate::crud_impl::make_create_handler!(
            $create_name,
            $full_ty,
            $primary_ty,
            $update_ty,
            $table
        );
        crate::crud_impl::make_read_handler!(
            $read_name, 
            $full_ty, 
            $primary_ty, 
            $update_ty, 
            $table
        );
        crate::crud_impl::make_read_all_handler!(
            $read_all_name, 
            $full_ty, 
            $primary_ty, 
            $update_ty, 
            $table
        );
        crate::crud_impl::make_update_handler!(
            $update_name,
            $full_ty,
            $primary_ty,
            $update_ty,
            $table
        );
        crate::crud_impl::make_delete_handler!(
            $delete_name,
            $full_ty,
            $primary_ty,
            $update_ty,
            $table
        );
    };
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
make_crud_handlers!(
    create_historical_volatility_data,
    read_historical_volatility_data,
    read_all_historical_volatility_data,
    update_historical_volatility_data,
    delete_historical_volatility_data,
    models::HistoricalVolatilityDataFullKeys,
    models::HistoricalVolatilityDataPrimaryKeys,
    models::HistoricalVolatilityDataUpdateKeys, 
    "market_data.historical_volatility_data"
);
make_crud_handlers!(
    create_historical_options_data,
    read_historical_options_data,
    read_all_historical_options_data,
    update_historical_options_data,
    delete_historical_options_data,
    models::HistoricalOptionsDataFullKeys,
    models::HistoricalOptionsDataPrimaryKeys,
    models::HistoricalOptionsDataUpdateKeys, 
    "phantom_trading.historical_options_data"
);
make_crud_handlers!(
    create_phantom_portfolio_value,
    read_phantom_portfolio_value,
    read_all_phantom_portfolio_value,
    update_phantom_portfolio_value,
    delete_phantom_portfolio_value,
    models::PhantomPortfolioValueFullKeys,
    models::PhantomPortfolioValuePrimaryKeys,
    models::PhantomPortfolioValueUpdateKeys, 
    "phantom_trading.phantom_portfolio_value"
);

use crate::{
    database::{
        models::CurrentStockPositionsFullKeys,
        models_crud::current_positions::current_positions::CurrentPositionsFullKeys,
    },
    helpers::{
        contract::{
            LocalContractTypes::{self, CurrentPosFk},
            get_contract_from,
        },
        sync_timeout::timeout,
    },
    init_app::ApplicationState,
    market_data::traits::{current_price::PriceSupplier, strategy_value::GetStrategyValue},
    schedule::broker_scheduler::IbkrRegion,
};
use axum::{
    Json, Router,
    extract::{Query, State},
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use ibapi::prelude::{Contract, Symbol};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    sync::{Arc, Weak},
    thread,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc::Receiver};

#[derive(Clone, Debug)]
struct AppState {
    trading_app_state: Arc<Mutex<Option<Weak<ApplicationState>>>>,
}

async fn extract_application_state(
    state: AppState,
) -> Result<Arc<ApplicationState>, (StatusCode, String)> {
    let trading_app_state = state.trading_app_state.lock().await;
    if trading_app_state.is_none() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Trading App is currently not running!".to_string(),
        ));
    }

    let application_state_ref = trading_app_state.as_ref().unwrap();
    let application_state_opt = application_state_ref.upgrade();
    if application_state_opt.is_none() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Trading App is currently not running!".to_string(),
        ));
    }

    let general_application_state = application_state_opt.unwrap();
    Ok(general_application_state)
    // match general_application_state.as_ref() {
    //     ApplicationState::IbkrState(application_state) => Ok(application_state.clone()),
    // }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ContractDetails {
    stock: String,
    primary_exchange: String,
    currency: String,
}
async fn get_current_price(
    State(state): State<AppState>,
    Query(payload): Query<ContractDetails>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let consolidator = {
        let general_application_state = extract_application_state(state).await?;
        match general_application_state.as_ref() {
            ApplicationState::IbkrState(application_state) => {
                application_state.consolidator.clone()
            }
        }
    };

    consolidator
        .get_current_price(
            get_contract_from(&LocalContractTypes::CurrentPosFk(
                CurrentPositionsFullKeys::Stock(CurrentStockPositionsFullKeys {
                    stock: payload.stock,
                    primary_exchange: payload.primary_exchange,
                    currency: payload.currency,
                    strategy: "".to_string(),
                    quantity: -1.0,
                    avg_price: -1.0,
                    last_updated: Utc::now(),
                }),
            )),
            false,
            &[],
        )
        .map_or_else(
            |e| Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
            |v| Ok(Json(serde_json::json!({"price": v}))),
        )
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ContractWithDetails {
    stock: String,
    primary_exchange: String,
    currency: String,
    current_price: f64,
}
async fn get_possible_stock_contracts(
    State(state): State<AppState>,
    Query(payload): Query<ContractDetails>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let contract = {
        let contract_builder = Contract::stock(payload.stock);
        let contract_w_prim = if payload.primary_exchange != "" {
            contract_builder.primary(payload.primary_exchange)
        } else {
            contract_builder
        };
        let full_contract = if payload.currency != "" {
            contract_w_prim.in_currency(payload.currency)
        } else {
            contract_w_prim
        };
        full_contract.build()
    };
    let cloned_contract = contract.clone();

    let (client, consolidator) = {
        let general_application_state = extract_application_state(state).await?;
        match general_application_state.as_ref() {
            ApplicationState::IbkrState(application_state) => (
                application_state.consolidator.client.clone(),
                application_state.consolidator.clone(),
            ),
        }
    };
    let contracts = match timeout(Duration::from_secs(1), move || {
        client.contract_details(&cloned_contract)
    }) {
        Ok(validated_contracts) => {
            if validated_contracts.len() == 0 {
                Vec::new()
            } else {
                validated_contracts
            }
        }
        Err(e) => {
            tracing::error!(
                message=%format!(
                    "Error occurred requesting contract details for {}: {}",
                    &contract.symbol,
                    e
                )
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch contract details from IBKR: {e:?}"),
            ));
        }
    };

    let possible_contracts = contracts
        .iter()
        .map(|contract| {
            let current_price =
                consolidator.get_current_price(contract.contract.clone(), false, &[]);
            if let Err(e) = current_price {
                return None;
            }
            Some(ContractWithDetails {
                stock: contract.contract.symbol.to_string(),
                primary_exchange: contract.contract.primary_exchange.to_string(),
                currency: contract.contract.currency.to_string(),
                current_price: current_price.unwrap(),
            })
        })
        .collect::<Vec<Option<ContractWithDetails>>>();
    if possible_contracts.iter().all(|v| v.is_some()) {
        Ok((
            StatusCode::OK,
            Json(serde_json::json!(
                possible_contracts
                    .iter()
                    .map(|v| v.clone().unwrap())
                    .collect::<Vec<ContractWithDetails>>()
            )),
        ))
    } else {
        Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not get current price for one or more contracts".to_string(),
        ))
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
    let consolidator = {
        let general_application_state = extract_application_state(state).await?;
        match general_application_state.as_ref() {
            ApplicationState::IbkrState(application_state) => {
                application_state.consolidator.clone()
            }
        }
    };
    let quote = payload.quote;
    let currency = payload.currency;
    match consolidator.get_current_price(
        Contract {
            symbol: Symbol::new(quote),
            security_type: ibapi::prelude::SecurityType::ForexPair,
            exchange: "IDEALPRO".into(),
            currency: ibapi::prelude::Currency(currency),
            ..Default::default()
        },
        false,
        &[],
    ) {
        Ok(price) => Ok(Json(serde_json::json!(CurrencyVal { price }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to fetch exchange rate: {e:?}"),
        )),
    }
    // consolidator
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StrategyValue {
    sgd_value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StrategyQuery {
    strategy: String,
}
async fn get_strategy_value(
    State(state): State<AppState>,
    Query(payload): Query<StrategyQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let consolidator = {
        let general_application_state = extract_application_state(state).await?;
        match general_application_state.as_ref() {
            ApplicationState::IbkrState(application_state) => {
                application_state.consolidator.clone()
            }
        }
    };
    consolidator
        .get_strategy_sgd_value(&payload.strategy)
        .map_or_else(
            |e| Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
            |v| Ok(Json(serde_json::json!(StrategyValue { sgd_value: v }))),
        )
    // .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
    // let current_stock_positions_crud =
    //     get_specific_current_stock_positions_crud(state.pool.clone());
    // let positions = current_stock_positions_crud
    //     .get_pos_by_strat(&payload.strategy)
    //     .await
    //     .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    // let consolidator = extract_application_state(state).await?.consolidator;
    //
    // let sgd_value = 0.0;
    // exchange_rates = HashMap::new();
    // for position in positions {
    //     let contract = get_contract_from_local_symbol(
    //         &position.stock,
    //         &position.primary_exchange,
    //         &position.currency,
    //     );
    //     if contract.security_type == SecurityType::ForexPair {
    //         let hash_contract = HashContract { contract };
    //         if !exchange_rates.contains_key(&hash_contract) {
    //             exchange_rates.insert(
    //                 hash_contract.clone(),
    //                 consolidator.get_current_price(&contract, &false, &[]),
    //             );
    //         }
    //         sgd_value += exchange_rates.get(&hash_contract).unwrap() * position.quantity;
    //         continue;
    //     }
    //
    //     if position.currency != "SGD" {
    //         let fx_contract =
    //             get_contract_from_local_symbol(&format!("FX:{}/SGD", position.currency), "", "SGD");
    //         let hash_contract = HashContract { contract };
    //         if !exchange_rates.contains_key(&hash_contract) {
    //             exchange_rates.insert(
    //                 hash_contract.clone(),
    //                 consolidator.get_current_price(&contract, &false, &[]),
    //             );
    //         }
    //
    //         // Market Value
    //         let mkt_value =
    //             consolidator.get_current_price(&contract, &false, &[]) * position.quantity;
    //
    //         sgd_value += exchange_rates.get(&hash_contract).unwrap() * mkt_value;
    //     }
    // }
    //
    // Ok(ValueAndData {
    //     sgd_value,
    //     exchange_rates: exchange_rates
    //         .iter()
    //         .map(|(k, v)| ExchangeRate { quote: k, price: v })
    //         .collect::<Vec<ExchangeRate>>(),
    // });
}

async fn check_health(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let trading_app_state = extract_application_state(state).await?;
    let is_ibkr_up = !IbkrRegion::Apac.is_in_maintenance(Utc::now());
    match trading_app_state.as_ref() {
        ApplicationState::IbkrState(application_state) => {
            if (is_ibkr_up && application_state.consolidator.client.is_connected()) || !is_ibkr_up {
                Ok((
                    StatusCode::OK,
                    axum::Json(serde_json::json!({ "status": "ok" })),
                ))
            } else {
                Ok((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({ "status": "not connected to IBKR" })),
                ))
            }
        }
    }
}

pub fn init_server(mut app_state_rcx: Receiver<Weak<ApplicationState>>) {
    thread::spawn(move || {
        let axum_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        axum_runtime.block_on(async {
            let app_state = AppState {
                trading_app_state: Arc::new(Mutex::new(None)),
            };
            let cloned_app_state = app_state.clone();
            tokio::spawn(async move {
                while let Some(application_state) = app_state_rcx.recv().await {
                    let mut trading_app_state = cloned_app_state.trading_app_state.lock().await;
                    trading_app_state.replace(application_state);
                }
            });

            let app = Router::new()
                .route(
                    "/contracts/stock",
                    axum::routing::get(get_possible_stock_contracts),
                )
                .route("/strategy/capital", axum::routing::get(get_strategy_value))
                .route("/exchange_rate", axum::routing::get(get_exchange_rate))
                .route("/contract/price", axum::routing::get(get_current_price))
                .route("/check-health", axum::routing::get(check_health))
                .with_state(app_state);

            let addr = "0.0.0.0:8000";
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            tracing::info!("[Axum Thread] Server listening on http://{}", addr);

            axum::serve(listener, app).await.unwrap();
        });
    });
}

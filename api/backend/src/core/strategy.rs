// main.rs
use axum::{Json, extract::State, response::IntoResponse};
use http::StatusCode;
use reqwest::Client;

use crate::{
    AppState,
    core::{self, crud::CRUDTrait},
    models,
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct PauseStrategy {
    strategy: String,
    graceful: bool,
}

pub async fn pause_strategy(
    State(state): State<AppState>,
    Json(pause_strategy_details): Json<PauseStrategy>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let strategy_crud = core::crud::CRUD::<
        models::StrategyFullKeys,
        models::StrategyPrimaryKeys,
        models::StrategyUpdateKeys,
    >::new(state.db.clone(), "trading.strategy".to_string());

    if pause_strategy_details.graceful {
        strategy_crud
            .update(
                &models::StrategyPrimaryKeys {
                    strategy: pause_strategy_details.strategy,
                },
                &models::StrategyUpdateKeys {
                    status: Some(models::Status::Stopping),
                },
            )
            .await
            .map_err(|err| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to update the Strategy Database: {}", err),
                )
                    .into()
            })?;
    } else {
        strategy_crud
            .update(
                &models::StrategyPrimaryKeys {
                    strategy: pause_strategy_details.strategy,
                },
                &models::StrategyUpdateKeys {
                    status: Some(models::Status::Inactive),
                },
            )
            .await
            .map_err(|err| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to update the Strategy Database: {}", err),
                )
                    .into()
            })?;
    }

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
                .into()
        })?;

    let _ = response_unparsed.error_for_status().map_err(|err| {
        (
            err.status()
                .unwrap_or_else(|| StatusCode::INTERNAL_SERVER_ERROR),
            format!(
                "Error occurred during update-all-orders request: {}",
                err.to_string()
            ),
        )
            .into()
    })?;

    Ok(((StatusCode::OK), "Paused Strategy Accordingly!"))
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct ResumeStrategy {
    strategy: String,
}

pub async fn resume_strategy(
    State(state): State<AppState>,
    Json(resume_strategy_details): Json<ResumeStrategy>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let strategy_crud = core::crud::CRUD::<
        models::StrategyFullKeys,
        models::StrategyPrimaryKeys,
        models::StrategyUpdateKeys,
    >::new(state.db.clone(), "trading.strategy".to_string());

    strategy_crud
        .update(
            &models::StrategyPrimaryKeys {
                strategy: resume_strategy_details.strategy,
            },
            &models::StrategyUpdateKeys {
                status: Some(models::Status::Active),
            },
        )
        .await
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to update the Strategy Database: {}", err),
            )
                .into()
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
                .into()
        })?;

    let _ = response_unparsed.error_for_status().map_err(|err| {
        (
            err.status()
                .unwrap_or_else(|| StatusCode::INTERNAL_SERVER_ERROR),
            format!(
                "Error occurred during update-all-orders request: {}",
                err.to_string()
            ),
        )
            .into()
    })?;

    Ok(((StatusCode::OK), "Paused Strategy Accordingly!"))
}

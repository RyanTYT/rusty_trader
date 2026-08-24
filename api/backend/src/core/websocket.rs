// main.rs
use axum::{
    extract::ws::{Message, WebSocketUpgrade},
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use futures::{SinkExt, StreamExt, future::join_all};
use http::StatusCode;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::interval};
use uuid::Uuid;

use crate::AppState;

// use core::crud::CRUDTrait as _;

// NOTE: Bearer cannot be sent as header via Websocket.connect() - thus, sent as query parameter
#[derive(serde::Deserialize)]
pub struct WsQuery {
    token: String,
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(WsQuery { token }): Query<WsQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let expected_token = format!("{}", state.auth_token);
    if token != expected_token {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    tracing::info!("Received Websocket connect request");
    ws.on_upgrade(|mut web_socket| async move {
        if let Err(e) = web_socket
            .send(Message::Text("Websocket Connect Success".into()))
            .await
        {
            tracing::error!("Failed to send websocket connect success msg: {e:?}");
        };

        let (sender, mut receiver) = web_socket.split();

        let client_id = Uuid::new_v4();
        let arced_sender = Arc::new(Mutex::new(sender));
        let cloned_arced_sender = arced_sender.clone();
        let cloned_client_id = client_id.clone();
        let cloned_clients = state.clients.clone();
        // let cloned_sender = sender.cl
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            let mut last_pong = Utc::now();

            loop {
                tokio::select! {
                    // 1. Tick the interval to send a Ping
                    _ = interval.tick() => {
                        let last_pong_diff = (Utc::now() - last_pong).num_seconds();
                        tracing::info!("Diff from last pong is: {last_pong_diff:?}s");
                        if last_pong_diff > 30 * 4 {
                            tracing::warn!("Websocket connection dropped: {cloned_client_id:?}");
                            break;
                        }
                        {
                            let mut cloned_sender = cloned_arced_sender.lock().await;
                            if cloned_sender.send(Message::Ping(vec![].into())).await.is_err() {
                                tracing::error!("Websocket connection dropped from ping failure");
                                break; // Connection lost
                            }
                        }
                    }
                    // 2. Listen for incoming messages (including Pongs)
                    Some(Ok(msg)) = receiver.next() => {
                        match msg {
                            Message::Pong(_) => {
                                last_pong = Utc::now();
                            }
                            Message::Text(t) => { tracing::warn!("Received message from websocket for some reason: {t:?}"); }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    else => break,
                }
            }

            {
                tracing::info!("Client dropped: {cloned_client_id:?}");
                let mut clients = cloned_clients.lock().await;
                clients.remove(&cloned_client_id);
            }
        });

        let mut clients = state.clients.lock().await;
        clients.insert(client_id, arced_sender);
    })
}

pub async fn broadcast_to_websocket(
    state: AppState,
    payload: String,
) -> (StatusCode, Response<axum::body::Body>) {
    // Get the client
    let mut clients = state.clients.lock().await;
    tracing::info!(
        "Received Broadcast to Websocket request for {}",
        clients.len()
    );

    if clients.is_empty() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Client not connected yet!".into_response(),
        );
    }

    let notifs_futures = clients.iter_mut().map(async |(id, socket_mutex)| {
        let mut socket = socket_mutex.lock().await;
        match socket.send(Message::Text(payload.clone())).await {
            Ok(_) => (
                StatusCode::OK,
                "Websocket data passed along!".into_response(),
            ),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error when sending message to client: {}", err).into_response(),
            ),
        }
    });
    let notifs_successes = join_all(notifs_futures).await;

    for notif_sent in notifs_successes {
        if notif_sent.0 == StatusCode::OK {
            continue;
        };

        let error_description = notif_sent.1;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "Some error occurred sending msg to clients (1st error is {error_description:?})"
            )
            .into_response(),
        );
    }

    (
        StatusCode::OK,
        "Websocket data sent successfully".into_response(),
    )
}

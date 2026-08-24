// // backend/src/notifications.rs
// //
// // Handles push notifications from the LLM service → Rust backend → WebSocket → Tauri frontend.
// // The LLM service POSTs to /send_notification when a pipeline completes.
// // The backend fans this out to all connected WebSocket clients.
//
// use axum::{Json, extract::State, response::IntoResponse};
// use serde::{Deserialize, Serialize};
// use tracing::info;
//
// use crate::{AppState, broadcast_to_websocket};
//
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct Notification {
//     pub title: String,
//     pub body: String,
//     pub alert_type: String, // "news_ideas" | "error" | "info"
//     pub function: String,   // Which LLM function triggered this
//     pub timestamp: String,
// }
//
// // Called by LLM service via POST /send_notification
// pub async fn broadcast_llm_notification(
//     State(state): State<AppState>,
//     Json(payload): Json<serde_json::Value>,
// ) -> impl IntoResponse {
//     let notification = Notification {
//         title: payload["title"]
//             .as_str()
//             .unwrap_or("AutoTrader")
//             .to_string(),
//         body: payload["body"].as_str().unwrap_or("").to_string(),
//         alert_type: payload["alert_type"].as_str().unwrap_or("info").to_string(),
//         function: payload["function"].as_str().unwrap_or("").to_string(),
//         timestamp: chrono::Utc::now().to_rfc3339(),
//     };
//
//     info!(
//         "Broadcasting notification: {} — {}",
//         notification.title, notification.body
//     );
//
//     // Broadcast to all connected WebSocket clients
//     // AppState.notification_tx is a broadcast::Sender<String>
//     let msg = serde_json::to_string(&serde_json::json!({
//         "type": "notification",
//         "payload": notification,
//     }))
//     .unwrap_or_default();
//
//     broadcast_to_websocket(state, msg).await
// }

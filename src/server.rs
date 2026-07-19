use crate::config::Config;
use crate::model::{JournaledEvent, SendJob, SendRequest, SendTarget};
use crate::runtime_state::RuntimeState;
use crate::store::Store;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tower_http::trace::TraceLayer;
use whatsapp_rust::Client;

#[derive(Clone)]
pub struct AppState {
    pub send_tx: mpsc::Sender<SendJob>,
    pub runtime: Arc<RuntimeState>,
    pub store: Store,
    pub events: broadcast::Sender<JournaledEvent>,
    pub config: Arc<Config>,
    pub client: Option<Arc<Client>>,
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    #[serde(default)]
    since: i64,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ChatsQuery {
    #[serde(default = "default_chat_limit")]
    limit: u64,
}

fn default_chat_limit() -> u64 {
    100
}

#[derive(Debug, Deserialize)]
struct MessagesQuery {
    from: String,
    #[serde(default = "default_message_limit")]
    limit: u64,
    before: Option<i64>,
}

fn default_message_limit() -> u64 {
    50
}

#[derive(Debug, Deserialize)]
struct HistorySyncRequest {
    from: String,
    #[serde(default = "default_sync_count")]
    count: i32,
}

fn default_sync_count() -> i32 {
    100
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/messages", post(post_messages))
        .route("/events", get(get_events))
        .route("/events/stream", get(stream_events))
        .route("/chats", get(get_chats))
        .route("/messages/history", get(get_messages))
        .route("/history/sync", post(post_history_sync))
        .route("/status", get(get_status))
        .route("/pairing", get(get_pairing))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn get_chats(State(state): State<AppState>, Query(query): Query<ChatsQuery>) -> Response {
    match state.store.list_chats(query.limit).await {
        Ok(chats) => Json(chats).into_response(),
        Err(error) => internal_error(error),
    }
}

async fn get_messages(
    State(state): State<AppState>,
    Query(query): Query<MessagesQuery>,
) -> Response {
    let chat_id = match state.store.resolve_chat_id(&query.from).await {
        Ok(Some(chat_id)) => chat_id,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "chat not found; wait for history sync or live traffic" })),
            )
                .into_response();
        }
        Err(error) => return internal_error(error),
    };
    match state
        .store
        .list_messages(&chat_id, query.before, query.limit)
        .await
    {
        Ok(messages) => Json(messages).into_response(),
        Err(error) => internal_error(error),
    }
}

async fn post_history_sync(
    State(state): State<AppState>,
    Json(request): Json<HistorySyncRequest>,
) -> Response {
    if !(1..=1_000).contains(&request.count) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "count must be between 1 and 1000" })),
        )
            .into_response();
    }
    let Some(client) = state.client.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "WhatsApp client unavailable" })),
        )
            .into_response();
    };
    if !client.is_connected() || !client.is_logged_in() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "WhatsApp is not connected" })),
        )
            .into_response();
    }

    let chat_id = match state.store.resolve_chat_id(&request.from).await {
        Ok(Some(chat_id)) => chat_id,
        Ok(None) => match crate::transport::target_jid(&SendTarget::Handle(request.from.clone())) {
            Ok(jid) => jid.to_string(),
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": error.to_string() })),
                )
                    .into_response();
            }
        },
        Err(error) => return internal_error(error),
    };
    let jid = match chat_id.parse() {
        Ok(jid) => jid,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid chat JID: {error}") })),
            )
                .into_response();
        }
    };
    let anchor = match state.store.oldest_message(&chat_id).await {
        Ok(anchor) => anchor,
        Err(error) => return internal_error(error),
    };
    let (oldest_id, from_me, timestamp_ms) = anchor
        .map(|message| (message.message_id, message.from_me, message.timestamp_ms))
        .unwrap_or_else(|| (String::new(), false, chrono::Utc::now().timestamp_millis()));

    match client
        .fetch_message_history(&jid, &oldest_id, from_me, timestamp_ms, request.count)
        .await
    {
        Ok(request_id) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "status": "requested",
                "request_id": request_id,
                "chat_id": chat_id,
                "count": request.count,
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

fn internal_error(error: anyhow::Error) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error.to_string() })),
    )
        .into_response()
}

async fn post_messages(
    State(state): State<AppState>,
    Json(request): Json<SendRequest>,
) -> impl IntoResponse {
    let target = match (request.to, request.chat_id) {
        (Some(to), None) if !to.trim().is_empty() => SendTarget::Handle(to),
        (None, Some(chat_id)) if !chat_id.trim().is_empty() => SendTarget::Chat(chat_id),
        (Some(_), Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "provide either to or chat_id, not both" })),
            );
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "provide to or chat_id" })),
            );
        }
    };

    if request.text.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "text must not be empty" })),
        );
    }

    let message_id = uuid::Uuid::new_v4().to_string();
    let job = SendJob {
        message_id: message_id.clone(),
        target,
        text: request.text,
        protocol: request.protocol,
        client_ref: request.client_ref,
    };
    if state.send_tx.send(job).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "send worker unavailable" })),
        );
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({ "message_id": message_id, "status": "queued" })),
    )
}

async fn get_events(State(state): State<AppState>, Query(query): Query<EventsQuery>) -> Response {
    match state
        .store
        .list_events_since(query.since, query.limit)
        .await
    {
        Ok(events) => Json(events).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn stream_events(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> Response {
    let store = state.store.clone();
    let mut receiver = state.events.subscribe();
    let mut last_sent = query.since;

    let stream = async_stream::stream! {
        if let Ok(events) = store.list_events_since(last_sent, None).await {
            for event in events {
                last_sent = event.id;
                if let Ok(line) = serde_json::to_string(&event) {
                    yield Ok::<Bytes, Infallible>(Bytes::from(format!("{line}\n")));
                }
            }
        }

        loop {
            match receiver.recv().await {
                Ok(event) => {
                    if event.id <= last_sent {
                        continue;
                    }
                    last_sent = event.id;
                    if let Ok(line) = serde_json::to_string(&event) {
                        yield Ok::<Bytes, Infallible>(Bytes::from(format!("{line}\n")));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if let Ok(events) = store.list_events_since(last_sent, None).await {
                        for event in events {
                            last_sent = event.id;
                            if let Ok(line) = serde_json::to_string(&event) {
                                yield Ok::<Bytes, Infallible>(Bytes::from(format!("{line}\n")));
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Response::builder()
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn get_status(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.snapshot().await;
    Json(json!({
        "product": "greenski",
        "version": env!("CARGO_PKG_VERSION"),
        "transport": "whatsapp-rust",
        "port": state.config.port,
        "webhook_configured": state.config.webhook_url.is_some(),
        "connection": {
            "status": runtime.status,
            "account": runtime.account,
            "last_error": runtime.last_error,
            "pairing_expires_at": runtime.pairing.expires_at,
            "uptime_secs": runtime.uptime_secs,
        },
    }))
}

async fn get_pairing(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.snapshot().await;
    Json(json!({
        "status": runtime.status,
        "account": runtime.account,
        "last_error": runtime.last_error,
        "code": runtime.pairing.code,
        "expires_at": runtime.pairing.expires_at,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tempfile::NamedTempFile;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        let path = NamedTempFile::new()
            .unwrap()
            .into_temp_path()
            .keep()
            .unwrap();
        let store = Store::open(&path).await.unwrap();
        let (send_tx, _send_rx) = mpsc::channel(4);
        let (events, _) = broadcast::channel(4);
        AppState {
            send_tx,
            runtime: Arc::new(RuntimeState::new()),
            store,
            events,
            config: Arc::new(Config::default()),
            client: None,
        }
    }

    #[tokio::test]
    async fn status_identifies_service() {
        let response = router(test_state().await)
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["product"], "greenski");
        assert!(value["connection"].get("pairing").is_none());
        assert!(value["connection"].get("code").is_none());
    }

    #[tokio::test]
    async fn send_rejects_missing_target() {
        let response = router(test_state().await)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"text":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

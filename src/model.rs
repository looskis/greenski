use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_protocol() -> String {
    "whatsapp".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SendRequest {
    pub to: Option<String>,
    pub chat_id: Option<String>,
    pub text: String,
    #[serde(alias = "service", default = "default_protocol")]
    pub protocol: String,
    pub client_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SendTarget {
    Handle(String),
    Chat(String),
}

impl SendTarget {
    pub fn display(&self) -> &str {
        match self {
            Self::Handle(value) | Self::Chat(value) => value,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SendJob {
    pub message_id: String,
    pub target: SendTarget,
    pub text: String,
    pub protocol: String,
    pub client_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event: String,
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Event {
    pub fn new(event: impl Into<String>, message_id: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            message_id: message_id.into(),
            provider_message_id: None,
            client_ref: None,
            handle: None,
            chat_id: None,
            text: None,
            protocol: None,
            status: None,
            reason: None,
            timestamp: Utc::now(),
            data: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournaledEvent {
    pub id: i64,
    #[serde(flatten)]
    pub event: Event,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chat {
    pub chat_id: String,
    pub phone_number: Option<String>,
    pub name: Option<String>,
    pub is_group: bool,
    pub archived: bool,
    pub unread_count: u32,
    pub last_message_at: Option<i64>,
    pub last_message_id: Option<String>,
    pub history_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    pub chat_id: String,
    pub message_id: String,
    pub sender_id: Option<String>,
    pub from_me: bool,
    pub timestamp_ms: i64,
    pub text: Option<String>,
    pub message_type: String,
    pub push_name: Option<String>,
    pub status: Option<String>,
    pub is_history: bool,
}

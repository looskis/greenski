use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    Starting,
    Pairing,
    Connected,
    Disconnected,
    LoggedOut,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairingSnapshot {
    pub code: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct Inner {
    status: ConnectionStatus,
    account: Option<String>,
    last_error: Option<String>,
    pairing: PairingSnapshot,
}

pub struct RuntimeState {
    start: Instant,
    inner: RwLock<Inner>,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            inner: RwLock::new(Inner {
                status: ConnectionStatus::Starting,
                account: None,
                last_error: None,
                pairing: PairingSnapshot {
                    code: None,
                    expires_at: None,
                },
            }),
        }
    }

    pub async fn set_status(&self, status: ConnectionStatus) {
        self.inner.write().await.status = status;
    }

    pub async fn set_connected(&self, account: Option<String>) {
        let mut inner = self.inner.write().await;
        inner.status = ConnectionStatus::Connected;
        inner.account = account;
        inner.last_error = None;
        inner.pairing.code = None;
        inner.pairing.expires_at = None;
    }

    pub async fn set_error(&self, status: ConnectionStatus, error: impl Into<String>) {
        let mut inner = self.inner.write().await;
        inner.status = status;
        inner.last_error = Some(error.into());
    }

    pub async fn set_qr(&self, code: String, valid_for: Duration) {
        let mut inner = self.inner.write().await;
        inner.status = ConnectionStatus::Pairing;
        inner.pairing.code = Some(code);
        inner.pairing.expires_at = chrono::Duration::from_std(valid_for)
            .ok()
            .map(|duration| Utc::now() + duration);
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        let inner = self.inner.read().await;
        RuntimeSnapshot {
            status: inner.status.clone(),
            account: inner.account.clone(),
            last_error: inner.last_error.clone(),
            pairing: inner.pairing.clone(),
            uptime_secs: self.start.elapsed().as_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSnapshot {
    pub status: ConnectionStatus,
    pub account: Option<String>,
    pub last_error: Option<String>,
    pub pairing: PairingSnapshot,
    pub uptime_secs: u64,
}

use crate::model::{Event, JournaledEvent};
use crate::store::Store;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

const QUEUE_CAPACITY: usize = 1024;
const WEBHOOK_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::Sender<SinkItem>,
}

impl EventSink {
    pub async fn emit(&self, event: Event) {
        if self.tx.send(SinkItem::New(event)).await.is_err() {
            tracing::warn!("event sink stopped before event could be recorded");
        }
    }

    pub async fn publish(&self, event: JournaledEvent) {
        if self.tx.send(SinkItem::Committed(event)).await.is_err() {
            tracing::warn!("event sink stopped before committed event could be published");
        }
    }
}

enum SinkItem {
    New(Event),
    Committed(JournaledEvent),
}

pub fn spawn(
    store: Store,
    webhook_url: Option<String>,
    hmac_secret: String,
    events: broadcast::Sender<JournaledEvent>,
) -> EventSink {
    let (tx, mut rx) = mpsc::channel::<SinkItem>(QUEUE_CAPACITY);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("valid webhook HTTP client configuration");
    let secret = Arc::new(hmac_secret);

    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            let journaled = match item {
                SinkItem::New(event) => match store.record_event(&event).await {
                    Ok(journaled) => journaled,
                    Err(error) => {
                        tracing::error!(%error, event = %event.event, "failed to journal event");
                        continue;
                    }
                },
                SinkItem::Committed(journaled) => journaled,
            };

            tracing::info!(
                event_id = journaled.id,
                event = %journaled.event.event,
                message_id = %journaled.event.message_id,
                "event"
            );
            let _ = events.send(journaled.clone());

            if let Some(url) = webhook_url.as_deref() {
                deliver_webhook(&client, url, &secret, &journaled).await;
            }
        }
    });

    EventSink { tx }
}

async fn deliver_webhook(
    client: &reqwest::Client,
    url: &str,
    secret: &str,
    event: &JournaledEvent,
) {
    let body = match serde_json::to_vec(event) {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!(%error, "failed to serialize webhook event");
            return;
        }
    };
    let signature = sign(secret, &body);

    for attempt in 1..=WEBHOOK_ATTEMPTS {
        let response = client
            .post(url)
            .header("X-Greenski-Signature", &signature)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone())
            .send()
            .await;

        match response {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => tracing::warn!(
                status = %response.status(),
                attempt,
                "webhook returned non-success status"
            ),
            Err(error) => tracing::warn!(%error, attempt, "webhook delivery failed"),
        }

        if attempt < WEBHOOK_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }
    }
}

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::sign;

    #[test]
    fn signature_is_stable() {
        assert_eq!(
            sign("secret", b"body"),
            "dc46983557fea127b43af721467eb9b3fde2338fe3e14f51952aa8478c13d355"
        );
    }
}

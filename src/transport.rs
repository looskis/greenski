use crate::event_sink::EventSink;
use crate::model::{Chat, Event, SendJob, SendTarget, StoredMessage};
use crate::runtime_state::{ConnectionStatus, RuntimeState};
use crate::store::Store;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use whatsapp_rust::prelude::*;
use whatsapp_rust::types::durability_hook::InboundDurabilityHook;
use whatsapp_rust::types::events::{Event as WaEvent, InboundMessage};
use whatsapp_rust::types::presence::ReceiptType;
use whatsapp_rust::wacore::store::DevicePropsOverride;

pub async fn build_bot(
    database_path: &std::path::Path,
    store: Store,
    sink: EventSink,
    state: Arc<RuntimeState>,
) -> Result<Bot> {
    let database_url = database_path
        .to_str()
        .context("WhatsApp database path is not valid UTF-8")?;
    let backend = SqliteStore::new(database_url)
        .await
        .context("open WhatsApp protocol store")?;

    let hook = DurableInbox {
        store: store.clone(),
        sink: sink.clone(),
    };
    let event_store = store.clone();
    let event_sink = sink.clone();
    let event_state = state.clone();

    let bot = Bot::builder()
        .with_backend(backend)
        .with_device_props(
            DevicePropsOverride::new()
                .with_os("Greenski")
                .with_platform_type(wa::device_props::PlatformType::CHROME),
        )
        .with_inbound_durability_hook(hook)
        .with_event_delivery(EventDelivery::Ordered { capacity: 1024 })
        .on_event(move |event, client| {
            let store = event_store.clone();
            let sink = event_sink.clone();
            let state = event_state.clone();
            async move {
                handle_whatsapp_event(&event, &client, &store, &sink, &state).await;
            }
        })
        .build()
        .await
        .context("build WhatsApp client")?;

    Ok(bot)
}

pub fn spawn_send_worker(
    mut rx: mpsc::Receiver<SendJob>,
    client: Arc<Client>,
    store: Store,
    sink: EventSink,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            if let Err(error) = store.record_pending(&job).await {
                tracing::error!(%error, message_id = %job.message_id, "failed to persist queued send");
            }

            let mut queued = event_for_job("message.queued", &job);
            queued.status = Some("queued".into());
            sink.emit(queued).await;

            let result = async {
                if job.protocol != "whatsapp" {
                    bail!(
                        "unsupported protocol '{}'; expected 'whatsapp'",
                        job.protocol
                    );
                }
                if !client.is_connected() || !client.is_logged_in() {
                    bail!("WhatsApp is not connected");
                }
                let jid = target_jid(&job.target)?;
                client
                    .send_text(jid, job.text.clone())
                    .await
                    .map_err(anyhow::Error::from)
            }
            .await;

            match result {
                Ok(sent) => {
                    if let Err(error) = store
                        .bind_provider_message(&job.message_id, &sent.message_id)
                        .await
                    {
                        tracing::error!(%error, message_id = %job.message_id, "failed to bind WhatsApp message id");
                    }
                    let mut event = event_for_job("message.sent", &job);
                    event.provider_message_id = Some(sent.message_id.clone());
                    event.status = Some("sent".into());
                    event.chat_id = Some(sent.to.to_string());
                    sink.emit(event).await;

                    let chat_id = sent.to.to_string();
                    let stored = StoredMessage {
                        chat_id: chat_id.clone(),
                        message_id: sent.message_id.clone(),
                        sender_id: None,
                        from_me: true,
                        timestamp_ms: chrono::Utc::now().timestamp_millis(),
                        text: Some(job.text.clone()),
                        message_type: "text".into(),
                        push_name: None,
                        status: Some("sent".into()),
                        is_history: false,
                    };
                    let chat = chat_from_message(&stored, None, false, 0, false);
                    if let Err(error) = store.upsert_chat(&chat, Some(&stored)).await {
                        tracing::warn!(%error, %chat_id, "failed to store outbound message");
                    }
                }
                Err(error) => {
                    if let Err(store_error) = store.mark_failed(&job.message_id).await {
                        tracing::error!(%store_error, message_id = %job.message_id, "failed to mark send failed");
                    }
                    let mut event = event_for_job("message.failed", &job);
                    event.status = Some("failed".into());
                    event.reason = Some(error.to_string());
                    sink.emit(event).await;
                }
            }
        }
    })
}

fn event_for_job(name: &str, job: &SendJob) -> Event {
    let mut event = Event::new(name, job.message_id.clone());
    event.client_ref = job.client_ref.clone();
    event.handle = Some(job.target.display().to_string());
    event.text = Some(job.text.clone());
    event.protocol = Some(job.protocol.clone());
    event
}

pub fn target_jid(target: &SendTarget) -> Result<Jid> {
    match target {
        SendTarget::Chat(chat_id) => chat_id
            .parse::<Jid>()
            .with_context(|| format!("invalid WhatsApp chat id '{chat_id}'")),
        SendTarget::Handle(handle) if handle.contains('@') => handle
            .parse::<Jid>()
            .with_context(|| format!("invalid WhatsApp JID '{handle}'")),
        SendTarget::Handle(handle) => {
            let digits: String = handle
                .chars()
                .filter(|character| character.is_ascii_digit())
                .collect();
            let contains_invalid = handle.chars().any(|character| {
                !character.is_ascii_digit()
                    && !matches!(character, '+' | '-' | '(' | ')' | ' ' | '.')
            });
            if contains_invalid || digits.len() < 6 {
                bail!(
                    "invalid phone number '{handle}'; use an international number such as +14155551234"
                );
            }
            Ok(Jid::pn(digits))
        }
    }
}

#[derive(Clone)]
struct DurableInbox {
    store: Store,
    sink: EventSink,
}

#[async_trait]
impl InboundDurabilityHook for DurableInbox {
    async fn on_messages(&self, _client: Arc<Client>, batch: &[InboundMessage]) -> Result<()> {
        persist_inbound_batch(batch, &self.store, &self.sink).await
    }
}

async fn persist_inbound_batch(
    batch: &[InboundMessage],
    store: &Store,
    sink: &EventSink,
) -> Result<()> {
    for message in batch {
        let chat_id = message.info.source.chat.to_string();
        let stored = live_stored_message(message);
        let chat = chat_from_message(&stored, None, message.info.source.is_group, 0, false);
        store.upsert_chat(&chat, Some(&stored)).await?;
        if message.info.source.is_from_me {
            continue;
        }
        let event = inbound_event(message);
        if let Some(journaled) = store
            .record_inbound_event_if_new(&chat_id, &message.info.id, &event)
            .await?
        {
            sink.publish(journaled).await;
        }
    }
    Ok(())
}

fn live_stored_message(message: &InboundMessage) -> StoredMessage {
    StoredMessage {
        chat_id: message.info.source.chat.to_string(),
        message_id: message.info.id.clone(),
        sender_id: (!message.info.source.is_from_me)
            .then(|| message.info.source.sender.to_string()),
        from_me: message.info.source.is_from_me,
        timestamp_ms: message.info.timestamp.timestamp_millis(),
        text: message.message.text_content().map(ToOwned::to_owned),
        message_type: format!("{:?}", message.info.r#type).to_lowercase(),
        push_name: (!message.info.push_name.is_empty()).then(|| message.info.push_name.clone()),
        status: Some(
            if message.info.source.is_from_me {
                "sent"
            } else {
                "received"
            }
            .into(),
        ),
        is_history: false,
    }
}

fn inbound_event(message: &InboundMessage) -> Event {
    let info = &message.info;
    let mut event = Event::new("message.received", info.id.clone());
    event.provider_message_id = Some(info.id.clone());
    event.handle = Some(info.source.sender.to_string());
    event.chat_id = Some(info.source.chat.to_string());
    event.text = message.message.text_content().map(ToOwned::to_owned);
    event.protocol = Some("whatsapp".into());
    event.status = Some("received".into());
    event.timestamp = info.timestamp;
    event.data = Some(json!({
        "push_name": info.push_name,
        "message_type": info.r#type,
        "is_group": info.source.is_group,
        "is_offline": info.is_offline,
        "media_type": info.media_type,
    }));
    event
}

async fn handle_whatsapp_event(
    event: &WaEvent,
    client: &Arc<Client>,
    store: &Store,
    sink: &EventSink,
    state: &Arc<RuntimeState>,
) {
    match event {
        WaEvent::PairingQrCode(qr) => {
            state.set_qr(qr.code.clone(), qr.timeout).await;
            let mut event = Event::new("connection.qr", "connection");
            event.data = Some(json!({ "valid_for_secs": qr.timeout.as_secs() }));
            sink.emit(event).await;
        }
        WaEvent::PairSuccess(pair) => {
            state.set_status(ConnectionStatus::Starting).await;
            let mut event = Event::new("connection.paired", "connection");
            event.data = Some(json!({
                "account": pair.id.to_string(),
                "lid": pair.lid.to_string(),
                "platform": pair.platform,
                "business_name": pair.business_name,
            }));
            sink.emit(event).await;
        }
        WaEvent::PairError(error) => {
            state
                .set_error(ConnectionStatus::Failed, error.error.clone())
                .await;
            let mut event = Event::new("connection.error", "connection");
            event.reason = Some(error.error.clone());
            sink.emit(event).await;
        }
        WaEvent::Connected(_) => {
            let account = client.get_pn().map(|jid| jid.to_string());
            state.set_connected(account.clone()).await;
            let mut event = Event::new("connection.open", "connection");
            event.data = Some(json!({ "account": account }));
            sink.emit(event).await;
        }
        WaEvent::Disconnected(disconnected) => {
            state.set_status(ConnectionStatus::Disconnected).await;
            let mut event = Event::new("connection.closed", "connection");
            event.reason = Some(format!("{:?}", disconnected.reason));
            sink.emit(event).await;
        }
        WaEvent::LoggedOut(logged_out) => {
            let reason = format!("{:?}", logged_out.reason);
            state
                .set_error(ConnectionStatus::LoggedOut, reason.clone())
                .await;
            let mut event = Event::new("connection.logged_out", "connection");
            event.reason = Some(reason);
            sink.emit(event).await;
        }
        WaEvent::ConnectFailure(failure) => {
            let reason = failure
                .message
                .clone()
                .unwrap_or_else(|| format!("{:?}", failure.reason));
            state
                .set_error(ConnectionStatus::Failed, reason.clone())
                .await;
            let mut event = Event::new("connection.error", "connection");
            event.reason = Some(reason);
            sink.emit(event).await;
        }
        WaEvent::StreamError(error) => {
            let mut event = Event::new("connection.stream_error", "connection");
            event.reason = Some(error.code.clone());
            sink.emit(event).await;
        }
        WaEvent::TemporaryBan(ban) => {
            let mut event = Event::new("account.temporary_ban", "account");
            event.reason = Some(ban.code.to_string());
            event.data = Some(json!({ "expires_in_secs": ban.expire.num_seconds() }));
            sink.emit(event).await;
        }
        WaEvent::Messages(batch) => {
            if let Err(error) = persist_inbound_batch(&batch.messages, store, sink).await {
                tracing::error!(%error, "failed to journal incoming WhatsApp messages");
            }
        }
        WaEvent::HistorySync(history) => {
            ingest_history_sync((**history).clone(), store, sink).await;
        }
        WaEvent::Receipt(receipt) => {
            if let Some(status) = receipt_status(&receipt.r#type) {
                for provider_message_id in &receipt.message_ids {
                    match store
                        .advance_provider_status(provider_message_id, status)
                        .await
                    {
                        Ok(Some(binding)) => {
                            let mut event = Event::new("message.status", binding.message_id);
                            event.provider_message_id = Some(provider_message_id.clone());
                            event.client_ref = binding.client_ref;
                            event.handle = Some(binding.handle);
                            event.chat_id = Some(receipt.source.chat.to_string());
                            event.protocol = Some(binding.protocol);
                            event.status = Some(status.into());
                            event.timestamp = receipt.timestamp;
                            sink.emit(event).await;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(%error, %provider_message_id, "failed to process receipt")
                        }
                    }
                }
            }
        }
        WaEvent::ServerAck(ack)
            if ack.class.as_deref() == Some("message") && ack.error.is_some() =>
        {
            let status = "failed";
            match store.advance_provider_status(&ack.id, status).await {
                Ok(Some(binding)) => {
                    let mut event = Event::new("message.failed", binding.message_id);
                    event.provider_message_id = Some(ack.id.clone());
                    event.client_ref = binding.client_ref;
                    event.handle = Some(binding.handle);
                    event.protocol = Some(binding.protocol);
                    event.status = Some(status.into());
                    event.reason = ack.error.clone();
                    sink.emit(event).await;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, provider_message_id = %ack.id, "failed to process server nack")
                }
            }
        }
        _ => {}
    }
}

async fn ingest_history_sync(
    history: whatsapp_rust::wacore::types::events::LazyHistorySync,
    store: &Store,
    sink: &EventSink,
) {
    let sync_type = history.sync_type();
    let chunk_order = history.chunk_order();
    let progress = history.progress();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<(Chat, Vec<StoredMessage>)>>(8);

    let producer = tokio::task::spawn_blocking(move || {
        let mut stream = history.stream();
        loop {
            match stream.next_conversation() {
                Ok(Some(conversation)) => {
                    if tx
                        .blocking_send(Ok(parse_history_conversation(conversation)))
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = tx.blocking_send(Err(error.into()));
                    break;
                }
            }
        }
    });

    let mut chat_count = 0_u64;
    let mut message_count = 0_u64;
    let mut failure = None;
    while let Some(batch) = rx.recv().await {
        match batch {
            Ok((chat, messages)) => {
                message_count += messages.len() as u64;
                chat_count += 1;
                if let Err(error) = store.upsert_messages(&chat, &messages).await {
                    failure = Some(error.to_string());
                    break;
                }
            }
            Err(error) => {
                failure = Some(error.to_string());
                break;
            }
        }
    }
    if let Err(error) = producer.await {
        failure = Some(error.to_string());
    }

    let mut event = Event::new(
        if failure.is_some() {
            "history.failed"
        } else {
            "history.synced"
        },
        "history",
    );
    event.reason = failure;
    event.data = Some(json!({
        "sync_type": sync_type,
        "chunk_order": chunk_order,
        "progress": progress,
        "chats": chat_count,
        "messages": message_count,
    }));
    sink.emit(event).await;
}

fn parse_history_conversation(
    conversation: whatsapp_rust::waproto::whatsapp::Conversation,
) -> (Chat, Vec<StoredMessage>) {
    let fallback_chat_id = conversation.id.clone();
    let mut messages = Vec::new();
    for history_message in conversation.messages {
        let Some(web_message) = history_message.message.into_option() else {
            continue;
        };
        let Some(key) = web_message.key.as_option() else {
            continue;
        };
        let Some(message_id) = key.id.clone() else {
            continue;
        };
        let chat_id = key
            .remote_jid
            .clone()
            .unwrap_or_else(|| fallback_chat_id.clone());
        let from_me = key.from_me.unwrap_or(false);
        let sender_id = if from_me {
            None
        } else {
            key.participant.clone().or_else(|| Some(chat_id.clone()))
        };
        let text = web_message
            .message
            .as_option()
            .and_then(|message| message.text_content())
            .map(ToOwned::to_owned);
        messages.push(StoredMessage {
            chat_id,
            message_id,
            sender_id,
            from_me,
            timestamp_ms: web_message
                .message_timestamp
                .and_then(|timestamp| i64::try_from(timestamp).ok())
                .unwrap_or_default()
                .saturating_mul(1_000),
            message_type: if text.is_some() {
                "text".into()
            } else if web_message.message_stub_type.is_some() {
                "system".into()
            } else {
                "unknown".into()
            },
            text,
            push_name: web_message.push_name,
            status: web_message
                .status
                .map(|status| format!("{status:?}").to_lowercase()),
            is_history: true,
        });
    }
    messages.sort_by_key(|message| message.timestamp_ms);
    let latest = messages.last();
    let chat_id = if fallback_chat_id.is_empty() {
        latest
            .map(|message| message.chat_id.clone())
            .unwrap_or_default()
    } else {
        fallback_chat_id
    };
    let last_message_at = conversation
        .last_msg_timestamp
        .and_then(|timestamp| i64::try_from(timestamp).ok())
        .map(|timestamp| timestamp.saturating_mul(1_000))
        .or_else(|| latest.map(|message| message.timestamp_ms));
    let last_message_id = latest.map(|message| message.message_id.clone());
    let phone_number = conversation
        .pn_jid
        .as_deref()
        .and_then(phone_from_jid)
        .or_else(|| phone_from_jid(&chat_id));
    let chat = Chat {
        is_group: chat_id.ends_with("@g.us"),
        chat_id,
        phone_number,
        name: conversation.display_name.or(conversation.name),
        archived: conversation.archived.unwrap_or(false),
        unread_count: conversation.unread_count.unwrap_or(0),
        last_message_at,
        last_message_id,
        history_complete: conversation.end_of_history_transfer.unwrap_or(false),
    };
    for message in &mut messages {
        message.chat_id.clone_from(&chat.chat_id);
    }
    (chat, messages)
}

fn chat_from_message(
    message: &StoredMessage,
    name: Option<String>,
    is_group: bool,
    unread_count: u32,
    history_complete: bool,
) -> Chat {
    Chat {
        chat_id: message.chat_id.clone(),
        phone_number: phone_from_jid(&message.chat_id),
        name: name.or_else(|| message.push_name.clone()),
        is_group,
        archived: false,
        unread_count,
        last_message_at: Some(message.timestamp_ms),
        last_message_id: Some(message.message_id.clone()),
        history_complete,
    }
}

fn phone_from_jid(jid: &str) -> Option<String> {
    if !jid.ends_with("@s.whatsapp.net") {
        return None;
    }
    let user = jid.split('@').next()?.split(':').next()?;
    let digits: String = user.chars().filter(char::is_ascii_digit).collect();
    (!digits.is_empty()).then_some(digits)
}

fn receipt_status(receipt: &ReceiptType) -> Option<&'static str> {
    match receipt {
        ReceiptType::Delivered => Some("delivered"),
        ReceiptType::Read | ReceiptType::ReadSelf => Some("read"),
        ReceiptType::Played | ReceiptType::PlayedSelf => Some("played"),
        ReceiptType::Sent | ReceiptType::Sender => Some("sent"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_international_phone_number() {
        let jid = target_jid(&SendTarget::Handle("+1 (415) 555-1234".into())).unwrap();
        assert_eq!(jid.to_string(), "14155551234@s.whatsapp.net");
    }

    #[test]
    fn preserves_group_jid() {
        let jid = target_jid(&SendTarget::Chat("120363000000000000@g.us".into())).unwrap();
        assert_eq!(jid.to_string(), "120363000000000000@g.us");
    }

    #[test]
    fn parses_history_conversation_text() {
        let conversation = wa::Conversation {
            id: "15551234567@s.whatsapp.net".into(),
            name: Some("Ada".into()),
            pn_jid: Some("15551234567@s.whatsapp.net".into()),
            messages: vec![wa::HistorySyncMsg {
                message: MessageField::some(wa::WebMessageInfo {
                    key: MessageField::some(wa::MessageKey {
                        remote_jid: Some("15551234567@s.whatsapp.net".into()),
                        id: Some("ABC123".into()),
                        from_me: Some(false),
                        ..Default::default()
                    }),
                    message: MessageField::some(wa::Message::text("historic hello")),
                    message_timestamp: Some(1_700_000_000),
                    push_name: Some("Ada".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        let (chat, messages) = parse_history_conversation(conversation);
        assert_eq!(chat.phone_number.as_deref(), Some("15551234567"));
        assert_eq!(chat.name.as_deref(), Some("Ada"));
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].text.as_deref(), Some("historic hello"));
        assert_eq!(messages[0].timestamp_ms, 1_700_000_000_000);
    }
}

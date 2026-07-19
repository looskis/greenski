use crate::config;
use crate::model::{Chat, Event, JournaledEvent, SendJob, StoredMessage};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use libsql::{Builder, Connection, Database, params};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
}

#[derive(Debug, Clone)]
pub struct OutboundBinding {
    pub message_id: String,
    pub client_ref: Option<String>,
    pub handle: String,
    pub protocol: String,
    pub last_status: String,
}

impl Store {
    pub async fn open(path: &Path) -> Result<Self> {
        let database_url = path
            .to_str()
            .context("application database path is not valid UTF-8")?;
        let db = Builder::new_local(database_url).build().await?;
        let store = Self { db: Arc::new(db) };
        store.migrate().await?;
        config::secure_file(path)?;
        Ok(store)
    }

    fn conn(&self) -> Result<Connection> {
        Ok(self.db.connect()?)
    }

    async fn migrate(&self) -> Result<()> {
        self.conn()?
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE IF NOT EXISTS events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload_json TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS outbound (
                     message_id TEXT PRIMARY KEY,
                     provider_message_id TEXT UNIQUE,
                     client_ref TEXT,
                     handle TEXT NOT NULL,
                     protocol TEXT NOT NULL,
                     last_status TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS inbound_seen (
                     chat_id TEXT NOT NULL,
                     provider_message_id TEXT NOT NULL,
                     PRIMARY KEY (chat_id, provider_message_id)
                 );
                 CREATE TABLE IF NOT EXISTS chats (
                     chat_id TEXT PRIMARY KEY,
                     phone_number TEXT,
                     name TEXT,
                     is_group INTEGER NOT NULL DEFAULT 0,
                     archived INTEGER NOT NULL DEFAULT 0,
                     unread_count INTEGER NOT NULL DEFAULT 0,
                     last_message_at INTEGER,
                     last_message_id TEXT,
                     history_complete INTEGER NOT NULL DEFAULT 0,
                     updated_at TEXT NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS idx_chats_phone ON chats(phone_number);
                 CREATE INDEX IF NOT EXISTS idx_chats_last_message
                     ON chats(last_message_at DESC);
                 CREATE TABLE IF NOT EXISTS messages (
                     chat_id TEXT NOT NULL,
                     message_id TEXT NOT NULL,
                     sender_id TEXT,
                     from_me INTEGER NOT NULL,
                     timestamp_ms INTEGER NOT NULL,
                     text TEXT,
                     message_type TEXT NOT NULL,
                     push_name TEXT,
                     status TEXT,
                     is_history INTEGER NOT NULL DEFAULT 0,
                     PRIMARY KEY (chat_id, message_id)
                 );
                 CREATE INDEX IF NOT EXISTS idx_messages_chat_time
                     ON messages(chat_id, timestamp_ms DESC);
                 CREATE INDEX IF NOT EXISTS idx_messages_sender
                     ON messages(sender_id, timestamp_ms DESC);",
            )
            .await?;
        Ok(())
    }

    pub async fn record_event(&self, event: &Event) -> Result<JournaledEvent> {
        let payload = serde_json::to_string(event)?;
        let created_at = Utc::now();
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO events (payload_json, created_at) VALUES (?1, ?2)",
            params![payload, created_at.to_rfc3339()],
        )
        .await?;
        Ok(JournaledEvent {
            id: conn.last_insert_rowid(),
            event: event.clone(),
            created_at,
        })
    }

    pub async fn list_events_since(
        &self,
        since: i64,
        limit: Option<u64>,
    ) -> Result<Vec<JournaledEvent>> {
        let conn = self.conn()?;
        let effective_limit = limit.unwrap_or(10_000).min(10_000) as i64;
        let mut rows = conn
            .query(
                "SELECT id, payload_json, created_at
                 FROM events WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
                params![since, effective_limit],
            )
            .await?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            let payload: String = row.get(1)?;
            let created_at: String = row.get(2)?;
            events.push(JournaledEvent {
                id: row.get(0)?,
                event: serde_json::from_str(&payload)?,
                created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }
        Ok(events)
    }

    pub async fn record_pending(&self, job: &SendJob) -> Result<()> {
        self.conn()?
            .execute(
                "INSERT OR REPLACE INTO outbound
                 (message_id, provider_message_id, client_ref, handle, protocol, last_status, created_at)
                 VALUES (?1, NULL, ?2, ?3, ?4, 'queued', ?5)",
                params![
                    job.message_id.clone(),
                    job.client_ref.clone(),
                    job.target.display().to_string(),
                    job.protocol.clone(),
                    Utc::now().to_rfc3339(),
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn bind_provider_message(
        &self,
        message_id: &str,
        provider_message_id: &str,
    ) -> Result<()> {
        self.conn()?
            .execute(
                "UPDATE outbound SET provider_message_id = ?1, last_status = 'sent'
                 WHERE message_id = ?2",
                params![provider_message_id, message_id],
            )
            .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, message_id: &str) -> Result<()> {
        self.conn()?
            .execute(
                "UPDATE outbound SET last_status = 'failed' WHERE message_id = ?1",
                params![message_id],
            )
            .await?;
        Ok(())
    }

    pub async fn advance_provider_status(
        &self,
        provider_message_id: &str,
        status: &str,
    ) -> Result<Option<OutboundBinding>> {
        let conn = self.conn()?;
        let mut rows = conn
            .query(
                "SELECT message_id, client_ref, handle, protocol, last_status
                 FROM outbound WHERE provider_message_id = ?1",
                params![provider_message_id],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let mut binding = OutboundBinding {
            message_id: row.get(0)?,
            client_ref: row.get(1)?,
            handle: row.get(2)?,
            protocol: row.get(3)?,
            last_status: row.get(4)?,
        };
        drop(rows);
        if status_rank(status) <= status_rank(&binding.last_status) {
            return Ok(None);
        }
        conn.execute(
            "UPDATE outbound SET last_status = ?1 WHERE provider_message_id = ?2",
            params![status, provider_message_id],
        )
        .await?;
        conn.execute(
            "UPDATE messages SET status = ?1 WHERE message_id = ?2",
            params![status, provider_message_id],
        )
        .await?;
        binding.last_status = status.to_string();
        Ok(Some(binding))
    }

    pub async fn record_inbound_event_if_new(
        &self,
        chat_id: &str,
        provider_message_id: &str,
        event: &Event,
    ) -> Result<Option<JournaledEvent>> {
        let payload = serde_json::to_string(event)?;
        let created_at = Utc::now();
        let conn = self.conn()?;
        let transaction = conn.transaction().await?;
        let changed = transaction
            .execute(
                "INSERT OR IGNORE INTO inbound_seen (chat_id, provider_message_id) VALUES (?1, ?2)",
                params![chat_id, provider_message_id],
            )
            .await?;
        if changed == 0 {
            transaction.rollback().await?;
            return Ok(None);
        }
        transaction
            .execute(
                "INSERT INTO events (payload_json, created_at) VALUES (?1, ?2)",
                params![payload, created_at.to_rfc3339()],
            )
            .await?;
        let id = transaction.last_insert_rowid();
        transaction.commit().await?;
        Ok(Some(JournaledEvent {
            id,
            event: event.clone(),
            created_at,
        }))
    }

    pub async fn upsert_chat(&self, chat: &Chat, message: Option<&StoredMessage>) -> Result<()> {
        let conn = self.conn()?;
        let transaction = conn.transaction().await?;
        transaction
            .execute(
                "INSERT INTO chats
                 (chat_id, phone_number, name, is_group, archived, unread_count,
                  last_message_at, last_message_id, history_complete, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(chat_id) DO UPDATE SET
                   phone_number = COALESCE(excluded.phone_number, chats.phone_number),
                   name = COALESCE(excluded.name, chats.name),
                   is_group = excluded.is_group,
                   archived = excluded.archived,
                   unread_count = excluded.unread_count,
                   last_message_at = CASE
                     WHEN chats.last_message_at IS NULL THEN excluded.last_message_at
                     WHEN excluded.last_message_at IS NULL THEN chats.last_message_at
                     ELSE MAX(chats.last_message_at, excluded.last_message_at) END,
                   last_message_id = CASE
                     WHEN excluded.last_message_at IS NOT NULL AND
                          (chats.last_message_at IS NULL OR excluded.last_message_at >= chats.last_message_at)
                     THEN excluded.last_message_id ELSE chats.last_message_id END,
                   history_complete = MAX(chats.history_complete, excluded.history_complete),
                   updated_at = excluded.updated_at",
                params![
                    chat.chat_id.clone(),
                    chat.phone_number.clone(),
                    chat.name.clone(),
                    i64::from(chat.is_group),
                    i64::from(chat.archived),
                    i64::from(chat.unread_count),
                    chat.last_message_at,
                    chat.last_message_id.clone(),
                    i64::from(chat.history_complete),
                    Utc::now().to_rfc3339(),
                ],
            )
            .await?;
        if let Some(message) = message {
            insert_message(&transaction, message).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn upsert_messages(&self, chat: &Chat, messages: &[StoredMessage]) -> Result<()> {
        let conn = self.conn()?;
        let transaction = conn.transaction().await?;
        transaction
            .execute(
                "INSERT INTO chats
                 (chat_id, phone_number, name, is_group, archived, unread_count,
                  last_message_at, last_message_id, history_complete, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(chat_id) DO UPDATE SET
                   phone_number = COALESCE(excluded.phone_number, chats.phone_number),
                   name = COALESCE(excluded.name, chats.name),
                   archived = excluded.archived,
                   unread_count = excluded.unread_count,
                   last_message_at = CASE
                     WHEN chats.last_message_at IS NULL THEN excluded.last_message_at
                     WHEN excluded.last_message_at IS NULL THEN chats.last_message_at
                     ELSE MAX(chats.last_message_at, excluded.last_message_at) END,
                   last_message_id = CASE
                     WHEN excluded.last_message_at IS NOT NULL AND
                          (chats.last_message_at IS NULL OR excluded.last_message_at >= chats.last_message_at)
                     THEN excluded.last_message_id ELSE chats.last_message_id END,
                   history_complete = MAX(chats.history_complete, excluded.history_complete),
                   updated_at = excluded.updated_at",
                params![
                    chat.chat_id.clone(),
                    chat.phone_number.clone(),
                    chat.name.clone(),
                    i64::from(chat.is_group),
                    i64::from(chat.archived),
                    i64::from(chat.unread_count),
                    chat.last_message_at,
                    chat.last_message_id.clone(),
                    i64::from(chat.history_complete),
                    Utc::now().to_rfc3339(),
                ],
            )
            .await?;
        for message in messages {
            insert_message(&transaction, message).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn list_chats(&self, limit: u64) -> Result<Vec<Chat>> {
        let conn = self.conn()?;
        let mut rows = conn
            .query(
                "SELECT chat_id, phone_number, name, is_group, archived, unread_count,
                        last_message_at, last_message_id, history_complete
                 FROM chats ORDER BY COALESCE(last_message_at, 0) DESC LIMIT ?1",
                params![limit.min(1_000) as i64],
            )
            .await?;
        let mut chats = Vec::new();
        while let Some(row) = rows.next().await? {
            chats.push(Chat {
                chat_id: row.get(0)?,
                phone_number: row.get(1)?,
                name: row.get(2)?,
                is_group: row.get::<i64>(3)? != 0,
                archived: row.get::<i64>(4)? != 0,
                unread_count: row.get::<i64>(5)?.max(0) as u32,
                last_message_at: row.get(6)?,
                last_message_id: row.get(7)?,
                history_complete: row.get::<i64>(8)? != 0,
            });
        }
        Ok(chats)
    }

    pub async fn resolve_chat_id(&self, value: &str) -> Result<Option<String>> {
        let digits: String = value.chars().filter(char::is_ascii_digit).collect();
        let conn = self.conn()?;
        let mut rows = conn
            .query(
                "SELECT chat_id FROM chats
                 WHERE chat_id = ?1 OR phone_number = ?2 OR chat_id LIKE (?2 || '@%')
                 ORDER BY COALESCE(last_message_at, 0) DESC LIMIT 1",
                params![value, digits],
            )
            .await?;
        Ok(rows.next().await?.map(|row| row.get(0)).transpose()?)
    }

    pub async fn list_messages(
        &self,
        chat_id: &str,
        before: Option<i64>,
        limit: u64,
    ) -> Result<Vec<StoredMessage>> {
        let conn = self.conn()?;
        let before = before.unwrap_or(i64::MAX);
        let mut rows = conn
            .query(
                "SELECT chat_id, message_id, sender_id, from_me, timestamp_ms, text,
                        message_type, push_name, status, is_history
                 FROM messages WHERE chat_id = ?1 AND timestamp_ms < ?2
                 ORDER BY timestamp_ms DESC LIMIT ?3",
                params![chat_id, before, limit.min(1_000) as i64],
            )
            .await?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next().await? {
            messages.push(StoredMessage {
                chat_id: row.get(0)?,
                message_id: row.get(1)?,
                sender_id: row.get(2)?,
                from_me: row.get::<i64>(3)? != 0,
                timestamp_ms: row.get(4)?,
                text: row.get(5)?,
                message_type: row.get(6)?,
                push_name: row.get(7)?,
                status: row.get(8)?,
                is_history: row.get::<i64>(9)? != 0,
            });
        }
        Ok(messages)
    }

    pub async fn oldest_message(&self, chat_id: &str) -> Result<Option<StoredMessage>> {
        let conn = self.conn()?;
        let mut rows = conn
            .query(
                "SELECT chat_id, message_id, sender_id, from_me, timestamp_ms, text,
                        message_type, push_name, status, is_history
                 FROM messages WHERE chat_id = ?1
                 ORDER BY timestamp_ms ASC LIMIT 1",
                params![chat_id],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(StoredMessage {
            chat_id: row.get(0)?,
            message_id: row.get(1)?,
            sender_id: row.get(2)?,
            from_me: row.get::<i64>(3)? != 0,
            timestamp_ms: row.get(4)?,
            text: row.get(5)?,
            message_type: row.get(6)?,
            push_name: row.get(7)?,
            status: row.get(8)?,
            is_history: row.get::<i64>(9)? != 0,
        }))
    }
}

async fn insert_message(conn: &Connection, message: &StoredMessage) -> Result<()> {
    conn.execute(
        "INSERT INTO messages
         (chat_id, message_id, sender_id, from_me, timestamp_ms, text,
          message_type, push_name, status, is_history)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(chat_id, message_id) DO UPDATE SET
           sender_id = COALESCE(excluded.sender_id, messages.sender_id),
           text = COALESCE(excluded.text, messages.text),
           message_type = excluded.message_type,
           push_name = COALESCE(excluded.push_name, messages.push_name),
           status = COALESCE(excluded.status, messages.status),
           is_history = MIN(messages.is_history, excluded.is_history)",
        params![
            message.chat_id.clone(),
            message.message_id.clone(),
            message.sender_id.clone(),
            i64::from(message.from_me),
            message.timestamp_ms,
            message.text.clone(),
            message.message_type.clone(),
            message.push_name.clone(),
            message.status.clone(),
            i64::from(message.is_history),
        ],
    )
    .await?;
    Ok(())
}

fn status_rank(status: &str) -> u8 {
    match status {
        "queued" => 0,
        "sent" => 1,
        "delivered" => 2,
        "read" => 3,
        "played" => 4,
        "failed" => 5,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{SendJob, SendTarget};

    async fn test_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).await.unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn journal_uses_monotonic_cursors() {
        let (_dir, store) = test_store().await;
        let first = store.record_event(&Event::new("one", "1")).await.unwrap();
        let second = store.record_event(&Event::new("two", "2")).await.unwrap();
        assert!(second.id > first.id);
        let listed = store.list_events_since(first.id, None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].event.event, "two");
    }

    #[tokio::test]
    async fn inbound_event_commit_is_idempotent() {
        let (_dir, store) = test_store().await;
        let event = Event::new("message.received", "message");
        assert!(
            store
                .record_inbound_event_if_new("chat", "message", &event)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .record_inbound_event_if_new("chat", "message", &event)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(store.list_events_since(0, None).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn receipt_status_only_advances() {
        let (_dir, store) = test_store().await;
        let job = SendJob {
            message_id: "local".into(),
            target: SendTarget::Handle("+15551234567".into()),
            text: "hello".into(),
            protocol: "whatsapp".into(),
            client_ref: Some("ref".into()),
        };
        store.record_pending(&job).await.unwrap();
        store.bind_provider_message("local", "wa").await.unwrap();
        assert!(
            store
                .advance_provider_status("wa", "delivered")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .advance_provider_status("wa", "sent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn stores_and_resolves_messages_by_phone() {
        let (_dir, store) = test_store().await;
        let chat = Chat {
            chat_id: "15551234567@s.whatsapp.net".into(),
            phone_number: Some("15551234567".into()),
            name: Some("Test".into()),
            is_group: false,
            archived: false,
            unread_count: 0,
            last_message_at: Some(123),
            last_message_id: Some("m1".into()),
            history_complete: false,
        };
        let message = StoredMessage {
            chat_id: chat.chat_id.clone(),
            message_id: "m1".into(),
            sender_id: Some(chat.chat_id.clone()),
            from_me: false,
            timestamp_ms: 123,
            text: Some("hello".into()),
            message_type: "text".into(),
            push_name: Some("Test".into()),
            status: None,
            is_history: true,
        };
        store.upsert_chat(&chat, Some(&message)).await.unwrap();
        assert_eq!(
            store.resolve_chat_id("+1 (555) 123-4567").await.unwrap(),
            Some(chat.chat_id.clone())
        );
        assert_eq!(
            store
                .list_messages(&chat.chat_id, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

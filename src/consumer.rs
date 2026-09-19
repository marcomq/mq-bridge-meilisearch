use std::{
    any::Any,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::anyhow;
use async_trait::async_trait;
use mq_bridge::{
    errors::ConsumerError,
    traits::{BatchCommitFunc, EndpointStatus, MessageConsumer, MessageDisposition},
    CanonicalMessage, ReceivedBatch,
};
use tracing::{info, warn};

use crate::{
    checkpoint::{self, FileCheckpoint},
    client::{MeiliClient, MeiliError},
    config::{self, MeilisearchConfig},
};

/// Delay before re-reading an index the scan has reached the end of, growing to
/// `max_polling_interval_ms` while nothing new appears.
struct Backoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(base: Duration, max: Option<Duration>) -> Self {
        Self {
            base,
            max: max.unwrap_or(base).max(base),
            current: base,
        }
    }

    fn idle_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(self.max);
        delay
    }

    fn reset(&mut self) {
        self.current = self.base;
    }
}

/// A rejected key cannot heal by reconnecting; a missing index or a busy
/// instance can, so those stay connection-level and the route retries them.
fn consumer_error(error: MeiliError) -> ConsumerError {
    let permanent = matches!(
        &error,
        MeiliError::Status { status, .. }
            if (400..500).contains(status) && !matches!(status, 404 | 408 | 429)
    );
    let reported = anyhow!("{error}");
    if permanent {
        ConsumerError::Permanent(reported)
    } else {
        ConsumerError::Connection(reported)
    }
}

fn stringify(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

struct MeilisearchConsumer {
    client: MeiliClient,
    index: String,
    primary_key: Option<String>,
    fields: Option<String>,
    exit_on_empty: bool,
    offset: Arc<Mutex<u64>>,
    total: Arc<Mutex<u64>>,
    checkpoint: Option<Arc<FileCheckpoint>>,
    backoff: Backoff,
}

impl MeilisearchConsumer {
    fn to_message(&self, document: &serde_json::Value) -> CanonicalMessage {
        let payload = serde_json::to_vec(document).unwrap_or_default();
        let mut message = CanonicalMessage::new(payload, None);
        message
            .metadata
            .insert("meilisearch.index".to_owned(), self.index.clone());
        if let Some(id) = self
            .primary_key
            .as_deref()
            .and_then(|key| document.get(key))
        {
            message
                .metadata
                .insert("meilisearch.document_id".to_owned(), stringify(id));
        }
        message
    }
}

/// Resolves where the durable resume position is kept.
fn build_checkpoint(
    settings: &MeilisearchConfig,
    index: &str,
) -> anyhow::Result<Option<Arc<FileCheckpoint>>> {
    let Some(cursor_id) = &settings.cursor_id else {
        warn!(
            index,
            "Meilisearch reader has no `cursor_id`; resume is disabled and every restart re-reads the index from the first document."
        );
        return Ok(None);
    };
    let Some(spec) = &settings.checkpoint_store else {
        warn!(
            index,
            "Meilisearch reader has `cursor_id` but no `checkpoint_store`; resume is disabled. Set one (file://, postgres://, mongodb://) to persist progress."
        );
        return Ok(None);
    };
    let path = checkpoint::parse_spec(spec)?;
    checkpoint::check_writable(&path)?;
    Ok(Some(Arc::new(FileCheckpoint::new(path, index, cursor_id))))
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessageConsumer>> {
    let (settings, index) = config::resolve_for_consumer(route_name, value)?;
    let client = MeiliClient::new(&settings)
        .map_err(|error| anyhow::Error::new(ConsumerError::Permanent(error)))?;
    let checkpoint = build_checkpoint(&settings, &index)
        .map_err(|error| anyhow::Error::new(ConsumerError::Permanent(error)))?;
    let offset = match &checkpoint {
        Some(store) => store.load().await?.and_then(|value| {
            let parsed = value.parse::<u64>().ok();
            if parsed.is_none() {
                warn!(value, "Ignoring an unparseable Meilisearch scan offset; starting from the first document");
            }
            parsed
        }),
        None => None,
    };
    info!(
        index,
        offset = offset.unwrap_or(0),
        "Meilisearch reader connected"
    );
    Ok(Box::new(MeilisearchConsumer {
        client,
        index,
        primary_key: settings.primary_key,
        fields: settings.fields,
        exit_on_empty: false,
        offset: Arc::new(Mutex::new(offset.unwrap_or(0))),
        total: Arc::new(Mutex::new(0)),
        checkpoint,
        backoff: Backoff::new(
            Duration::from_millis(settings.polling_interval_ms),
            settings.max_polling_interval_ms.map(Duration::from_millis),
        ),
    }))
}

#[async_trait]
impl MessageConsumer for MeilisearchConsumer {
    /// The scan position is one cumulative offset, so a later document cannot
    /// be committed before an earlier one.
    fn commit_requires_order(&self) -> bool {
        true
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }

        let start = *self.offset.lock().unwrap();
        let page = self
            .client
            .get_documents(&self.index, start, max_messages, self.fields.as_deref())
            .await
            .map_err(consumer_error)?;
        *self.total.lock().unwrap() = page.total;

        if page.results.is_empty() {
            // The empty batch is what `exit_on_empty` ends a drain on, so a
            // draining route must not be slowed by the idle delay first.
            if !self.exit_on_empty {
                tokio::time::sleep(self.backoff.idle_delay()).await;
            }
            return Ok(ReceivedBatch::empty());
        }
        self.backoff.reset();

        let messages: Vec<CanonicalMessage> = page
            .results
            .iter()
            .map(|document| self.to_message(document))
            .collect();
        let count = messages.len();
        // Advance optimistically; the commit rolls back to the acknowledged
        // boundary so nothing a batch did not ack is skipped.
        *self.offset.lock().unwrap() = start + count as u64;

        let offset = Arc::clone(&self.offset);
        let checkpoint = self.checkpoint.clone();
        let commit: BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                let acked = dispositions
                    .iter()
                    .take(count)
                    .take_while(|disposition| {
                        matches!(
                            disposition,
                            MessageDisposition::Ack | MessageDisposition::Reply(_)
                        )
                    })
                    .count();
                let boundary = start + acked as u64;
                if acked < count {
                    *offset.lock().unwrap() = boundary;
                }
                if let Some(store) = checkpoint {
                    if let Err(error) = store.save(&boundary.to_string()).await {
                        warn!(error = %error, "Failed to persist the Meilisearch scan offset. Documents may be re-read on restart.");
                    }
                }
                Ok(())
            })
        });
        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.client.health().await {
            Ok(()) => (true, None),
            Err(error) => (false, Some(error.to_string())),
        };
        let offset = *self.offset.lock().unwrap();
        EndpointStatus {
            healthy,
            target: self.index.clone(),
            error,
            pending: Some(self.total.lock().unwrap().saturating_sub(offset) as usize),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consumer(primary_key: Option<&str>) -> MeilisearchConsumer {
        MeilisearchConsumer {
            client: MeiliClient::new(
                &serde_json::from_value(serde_json::json!({"url": "http://localhost:7700"}))
                    .unwrap(),
            )
            .unwrap(),
            index: "movies".to_owned(),
            primary_key: primary_key.map(str::to_owned),
            fields: None,
            exit_on_empty: false,
            offset: Arc::new(Mutex::new(0)),
            total: Arc::new(Mutex::new(0)),
            checkpoint: None,
            backoff: Backoff::new(Duration::from_millis(100), None),
        }
    }

    #[test]
    fn a_document_becomes_its_own_payload_plus_locating_metadata() {
        let document = serde_json::json!({"id": 7, "title": "x"});
        let message = consumer(Some("id")).to_message(&document);

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&message.payload).unwrap(),
            document
        );
        assert_eq!(
            message
                .metadata
                .get("meilisearch.index")
                .map(String::as_str),
            Some("movies")
        );
        assert_eq!(
            message
                .metadata
                .get("meilisearch.document_id")
                .map(String::as_str),
            Some("7")
        );
    }

    #[test]
    fn without_a_primary_key_no_document_id_is_invented() {
        let message = consumer(None).to_message(&serde_json::json!({"id": 7}));
        assert!(!message.metadata.contains_key("meilisearch.document_id"));
    }

    #[test]
    fn a_string_document_id_keeps_its_own_text() {
        let message = consumer(Some("id")).to_message(&serde_json::json!({"id": "abc"}));
        assert_eq!(
            message
                .metadata
                .get("meilisearch.document_id")
                .map(String::as_str),
            Some("abc")
        );
    }

    #[test]
    fn the_idle_delay_stays_put_without_a_configured_maximum() {
        let mut backoff = Backoff::new(Duration::from_millis(100), None);
        assert_eq!(backoff.idle_delay(), Duration::from_millis(100));
        assert_eq!(backoff.idle_delay(), Duration::from_millis(100));
    }

    #[test]
    fn the_idle_delay_backs_off_to_the_configured_maximum_and_resets() {
        let mut backoff =
            Backoff::new(Duration::from_millis(100), Some(Duration::from_millis(300)));
        assert_eq!(backoff.idle_delay(), Duration::from_millis(100));
        assert_eq!(backoff.idle_delay(), Duration::from_millis(200));
        assert_eq!(backoff.idle_delay(), Duration::from_millis(300));
        assert_eq!(backoff.idle_delay(), Duration::from_millis(300));

        backoff.reset();
        assert_eq!(backoff.idle_delay(), Duration::from_millis(100));
    }

    #[test]
    fn a_rejected_key_is_permanent_but_a_missing_index_is_retried() {
        let status = |status| MeiliError::Status {
            status,
            code: String::new(),
            message: String::new(),
        };
        assert!(matches!(
            consumer_error(status(403)),
            ConsumerError::Permanent(_)
        ));
        assert!(matches!(
            consumer_error(status(404)),
            ConsumerError::Connection(_)
        ));
        assert!(matches!(
            consumer_error(status(429)),
            ConsumerError::Connection(_)
        ));
        assert!(matches!(
            consumer_error(MeiliError::Transport(anyhow!("offline"))),
            ConsumerError::Connection(_)
        ));
    }
}

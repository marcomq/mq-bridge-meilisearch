use std::any::Any;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mq_bridge::{
    errors::PublisherError,
    traits::{EndpointStatus, MessagePublisher},
    CanonicalMessage, SentBatch,
};

use crate::{
    client::{MeiliClient, MeiliError},
    config::{self, WriteMethod},
};

/// A Postgres `truncate` carries no row, so it cannot be turned into a
/// document write; it is surfaced rather than silently indexed as an upsert.
const TRUNCATE: &str = "truncate";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Upsert,
    Delete,
}

/// A maximal stretch of consecutive messages sharing one operation. Runs are
/// issued in order: reordering them would turn `insert(id=7); delete(id=7)`
/// into a document that stays in the index forever, or the reverse into one
/// that vanishes from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Run {
    op: Op,
    start: usize,
    end: usize,
}

/// Why a run could not be applied, already classified for the route.
#[derive(Debug)]
struct RunFailure {
    retryable: bool,
    reason: String,
}

impl RunFailure {
    fn permanent(error: anyhow::Error) -> Self {
        Self {
            retryable: false,
            reason: format!("{error:#}"),
        }
    }
}

impl From<MeiliError> for RunFailure {
    fn from(error: MeiliError) -> Self {
        Self {
            retryable: is_retryable(&error),
            reason: describe(&error),
        }
    }
}

/// Which Meilisearch failures can heal on their own. A rejected key or a
/// malformed document cannot, and retrying it would only delay the dead letter.
fn is_retryable(error: &MeiliError) -> bool {
    match error {
        MeiliError::Transport(_) => true,
        MeiliError::Status { status, .. } => matches!(status, 429 | 500 | 502 | 503 | 504),
        MeiliError::Task { .. } => false,
    }
}

fn describe(error: &MeiliError) -> String {
    match error.code() {
        Some("payload_too_large") => format!(
            "{error}; lower the route's `batch_size` so one request stays under Meilisearch's payload size limit"
        ),
        _ => error.to_string(),
    }
}

/// Resolves `${metadata:<key>}` and `${payload:<field>}` against one message;
/// anything else is a literal. A key the message does not carry yields `None`.
fn resolve(token: &str, message: &CanonicalMessage) -> Option<String> {
    let Some(inner) = token.strip_prefix("${").and_then(|t| t.strip_suffix('}')) else {
        return Some(token.to_owned());
    };
    let Some((prefix, name)) = inner.split_once(':') else {
        return Some(token.to_owned());
    };
    match prefix.trim() {
        "metadata" => message.metadata.get(name.trim()).cloned(),
        "payload" => payload_field(message, name.trim()).map(stringify),
        _ => Some(token.to_owned()),
    }
}

fn payload_field(message: &CanonicalMessage, field: &str) -> Option<serde_json::Value> {
    serde_json::from_slice::<serde_json::Value>(&message.payload)
        .ok()?
        .get(field)
        .cloned()
}

fn stringify(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text,
        other => other.to_string(),
    }
}

/// Appends one message as an NDJSON line. Payloads are already JSON objects, so
/// the bytes are normally copied through untouched; only a payload carrying
/// newlines is re-serialized, since those would break the line framing.
fn append_document(body: &mut Vec<u8>, message: &CanonicalMessage) -> anyhow::Result<()> {
    let payload = message.payload.as_ref().trim_ascii();
    if payload.first() != Some(&b'{') {
        return Err(anyhow!(
            "Meilisearch stores JSON objects; this message's payload is not one. Use a `transform` middleware to shape it before the sink."
        ));
    }
    if payload.contains(&b'\n') || payload.contains(&b'\r') {
        let document: serde_json::Value =
            serde_json::from_slice(payload).context("message payload is not valid JSON")?;
        serde_json::to_writer(&mut *body, &document)
            .context("failed to re-serialize a multi-line JSON payload")?;
    } else {
        body.extend_from_slice(payload);
    }
    body.push(b'\n');
    Ok(())
}

/// Everything from `from` on is handed back for retry or dead-lettering: the
/// run that failed, plus every later run, which was never sent. Reporting only
/// the failing run would let the route acknowledge writes that never happened.
fn fail_from(mut messages: Vec<CanonicalMessage>, from: usize, failure: RunFailure) -> SentBatch {
    let failed = messages
        .split_off(from)
        .into_iter()
        .map(|message| {
            let error = anyhow!("{}", failure.reason);
            let classified = if failure.retryable {
                PublisherError::Retryable(error)
            } else {
                PublisherError::NonRetryable(error)
            };
            (message, classified)
        })
        .collect();
    SentBatch::Partial {
        responses: None,
        failed,
    }
}

struct MeilisearchPublisher {
    client: MeiliClient,
    index: String,
    primary_key: Option<String>,
    method: WriteMethod,
    operation: Option<String>,
    delete_values: Vec<String>,
    wait_for_task: bool,
}

impl MeilisearchPublisher {
    fn classify(&self, message: &CanonicalMessage) -> anyhow::Result<Op> {
        let Some(template) = &self.operation else {
            return Ok(Op::Upsert);
        };
        let operation = resolve(template, message).unwrap_or_default();
        if operation.eq_ignore_ascii_case(TRUNCATE) {
            return Err(anyhow!(
                "Meilisearch cannot apply a `{TRUNCATE}` operation; delete the index or filter the operation out upstream"
            ));
        }
        if self
            .delete_values
            .iter()
            .any(|value| value.eq_ignore_ascii_case(&operation))
        {
            Ok(Op::Delete)
        } else {
            Ok(Op::Upsert)
        }
    }

    /// Splits the batch into contiguous same-operation runs, stopping at the
    /// first message whose operation cannot be applied.
    fn plan(&self, messages: &[CanonicalMessage]) -> (Vec<Run>, Option<(usize, anyhow::Error)>) {
        let mut runs: Vec<Run> = Vec::new();
        for (index, message) in messages.iter().enumerate() {
            let op = match self.classify(message) {
                Ok(op) => op,
                Err(error) => return (runs, Some((index, error))),
            };
            match runs.last_mut() {
                Some(run) if run.op == op => run.end = index + 1,
                _ => runs.push(Run {
                    op,
                    start: index,
                    end: index + 1,
                }),
            }
        }
        (runs, None)
    }

    fn delete_ids(&self, messages: &[CanonicalMessage]) -> anyhow::Result<Vec<serde_json::Value>> {
        let key = self.primary_key.as_deref().ok_or_else(|| {
            anyhow!("Meilisearch `primary_key` is required to delete a document by id")
        })?;
        messages
            .iter()
            .map(|message| {
                payload_field(message, key).ok_or_else(|| {
                    anyhow!(
                        "a delete carries no `{key}` field, so the document to remove is unknown"
                    )
                })
            })
            .collect()
    }

    async fn send_run(&self, op: Op, messages: &[CanonicalMessage]) -> Result<(), RunFailure> {
        let task = match op {
            Op::Upsert => {
                let mut body = Vec::new();
                for message in messages {
                    append_document(&mut body, message).map_err(RunFailure::permanent)?;
                }
                self.client
                    .add_documents(&self.index, self.primary_key.as_deref(), self.method, body)
                    .await?
            }
            Op::Delete => {
                let ids = self.delete_ids(messages).map_err(RunFailure::permanent)?;
                self.client.delete_documents(&self.index, &ids).await?
            }
        };
        if self.wait_for_task {
            self.client.wait_for_task(task).await?;
        }
        Ok(())
    }
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessagePublisher>> {
    let (settings, index) = config::resolve_for_publisher(route_name, value)?;
    let client = MeiliClient::new(&settings)
        .map_err(|error| anyhow::Error::new(PublisherError::NonRetryable(error)))?;
    if settings.create_index {
        if let Err(error) = client
            .create_index(&index, settings.primary_key.as_deref())
            .await
        {
            let reported = anyhow!("failed to create Meilisearch index '{index}': {error}");
            return Err(if is_retryable(&error) {
                reported
            } else {
                anyhow::Error::new(PublisherError::NonRetryable(reported))
            });
        }
    }
    Ok(Box::new(MeilisearchPublisher {
        client,
        index,
        primary_key: settings.primary_key,
        method: settings.method,
        operation: settings.operation,
        delete_values: settings.delete_values,
        wait_for_task: settings.wait_for_task,
    }))
}

#[async_trait]
impl MessagePublisher for MeilisearchPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        let (runs, stopped) = self.plan(&messages);
        for run in &runs {
            if let Err(failure) = self.send_run(run.op, &messages[run.start..run.end]).await {
                return Ok(fail_from(messages, run.start, failure));
            }
        }
        match stopped {
            Some((index, error)) => Ok(fail_from(messages, index, RunFailure::permanent(error))),
            None => Ok(SentBatch::Ack),
        }
    }

    /// Meilisearch keys documents by primary key, so two batches touching one
    /// document must reach it in source order. Honoured only when this crate is
    /// linked directly: plugin ABI 1.0 carries no publisher-side ordering slot,
    /// so a plugin-loaded route must be started at `concurrency` 1 — which the
    /// mq-bridge CLI and MCP tools do not default to.
    fn requires_ordered_publish(&self) -> bool {
        true
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.client.health().await {
            Ok(()) => (true, None),
            Err(error) => (false, Some(error.to_string())),
        };
        EndpointStatus {
            healthy,
            target: self.index.clone(),
            error,
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

    fn publisher(operation: Option<&str>) -> MeilisearchPublisher {
        MeilisearchPublisher {
            client: MeiliClient::new(
                &serde_json::from_value(serde_json::json!({"url": "http://localhost:7700"}))
                    .unwrap(),
            )
            .unwrap(),
            index: "movies".to_owned(),
            primary_key: Some("id".to_owned()),
            method: WriteMethod::Replace,
            operation: operation.map(str::to_owned),
            delete_values: vec!["delete".to_owned()],
            wait_for_task: true,
        }
    }

    fn message(id: u64, operation: Option<&str>) -> CanonicalMessage {
        let mut message =
            CanonicalMessage::from(serde_json::json!({"id": id, "title": "x"}).to_string());
        if let Some(operation) = operation {
            message
                .metadata
                .insert("postgres.operation".to_owned(), operation.to_owned());
        }
        message
    }

    #[test]
    fn tokens_resolve_from_metadata_payload_or_literally() {
        let message = message(7, Some("update"));
        assert_eq!(
            resolve("${metadata:postgres.operation}", &message).as_deref(),
            Some("update")
        );
        assert_eq!(resolve("${payload:id}", &message).as_deref(), Some("7"));
        assert_eq!(resolve("${payload:title}", &message).as_deref(), Some("x"));
        assert_eq!(resolve("insert", &message).as_deref(), Some("insert"));
        assert_eq!(resolve("${metadata:absent}", &message), None);
    }

    #[test]
    fn without_an_operation_mapping_every_message_is_an_upsert() {
        let publisher = publisher(None);
        let messages = vec![message(1, Some("delete")), message(2, None)];
        let (runs, stopped) = publisher.plan(&messages);
        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![Run {
                op: Op::Upsert,
                start: 0,
                end: 2
            }]
        );
    }

    /// The case that makes run order load-bearing: replaying these two in the
    /// wrong order leaves document 7 in the index for good.
    #[test]
    fn an_insert_and_a_delete_of_one_key_stay_in_separate_ordered_runs() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages = vec![message(7, Some("insert")), message(7, Some("delete"))];

        let (runs, stopped) = publisher.plan(&messages);

        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![
                Run {
                    op: Op::Upsert,
                    start: 0,
                    end: 1
                },
                Run {
                    op: Op::Delete,
                    start: 1,
                    end: 2
                }
            ]
        );
    }

    #[test]
    fn a_cdc_batch_of_one_operation_becomes_a_single_request() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages: Vec<_> = (1..=100).map(|id| message(id, Some("update"))).collect();
        let (runs, _) = publisher.plan(&messages);
        assert_eq!(runs.len(), 1);
    }

    #[test]
    fn truncate_stops_the_plan_rather_than_indexing_an_empty_row() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages = vec![message(1, Some("insert")), message(2, Some("truncate"))];

        let (runs, stopped) = publisher.plan(&messages);

        assert_eq!(runs.len(), 1);
        let (index, error) = stopped.expect("truncate should stop the plan");
        assert_eq!(index, 1);
        assert!(error.to_string().contains("truncate"));
    }

    #[test]
    fn a_delete_names_the_document_by_its_primary_key() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let ids = publisher.delete_ids(&[message(7, Some("delete"))]).unwrap();
        assert_eq!(ids, vec![serde_json::json!(7)]);

        let without_key = CanonicalMessage::from(r#"{"title":"x"}"#);
        assert!(publisher.delete_ids(&[without_key]).is_err());
    }

    #[test]
    fn documents_are_concatenated_without_being_re_serialized() {
        let mut body = Vec::new();
        append_document(&mut body, &CanonicalMessage::from(r#"{"id":1}"#)).unwrap();
        append_document(&mut body, &CanonicalMessage::from(r#"  {"id":2}  "#)).unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "{\"id\":1}\n{\"id\":2}\n");
    }

    /// A pretty-printed payload would otherwise be framed as several documents.
    #[test]
    fn a_multi_line_payload_is_compacted_so_the_framing_survives() {
        let mut body = Vec::new();
        append_document(&mut body, &CanonicalMessage::from("{\n  \"id\": 1\n}")).unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "{\"id\":1}\n");
    }

    #[test]
    fn a_payload_that_is_not_a_json_object_is_rejected() {
        let mut body = Vec::new();
        assert!(append_document(&mut body, &CanonicalMessage::from("plain text")).is_err());
        assert!(append_document(&mut body, &CanonicalMessage::from("[1,2]")).is_err());
    }

    #[test]
    fn transient_statuses_are_retried_and_rejections_are_not() {
        let status = |status| MeiliError::Status {
            status,
            code: String::new(),
            message: String::new(),
        };
        assert!(is_retryable(&MeiliError::Transport(anyhow!("offline"))));
        assert!(is_retryable(&status(429)));
        assert!(is_retryable(&status(503)));
        assert!(!is_retryable(&status(401)));
        assert!(!is_retryable(&status(413)));
        assert!(!is_retryable(&MeiliError::Task {
            code: "missing_document_id".to_owned(),
            message: String::new(),
        }));
    }

    #[test]
    fn an_oversized_request_names_the_setting_that_fixes_it() {
        let reason = describe(&MeiliError::Status {
            status: 413,
            code: "payload_too_large".to_owned(),
            message: "too big".to_owned(),
        });
        assert!(reason.contains("batch_size"), "{reason}");
    }

    /// A run that fails takes the untried runs behind it with it, so the route
    /// never treats a write it did not issue as delivered.
    #[test]
    fn a_failed_run_fails_everything_after_it_too() {
        let messages: Vec<_> = (1..=5).map(|id| message(id, None)).collect();
        let batch = fail_from(
            messages,
            2,
            RunFailure {
                retryable: true,
                reason: "boom".to_owned(),
            },
        );

        let SentBatch::Partial { failed, .. } = batch else {
            panic!("a failed run must report a partial batch");
        };
        assert_eq!(failed.len(), 3);
        assert!(failed
            .iter()
            .all(|(_, error)| matches!(error, PublisherError::Retryable(_))));
    }
}

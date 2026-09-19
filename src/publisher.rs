use std::{any::Any, collections::HashSet};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mq_bridge::{
    errors::PublisherError,
    traits::{EndpointStatus, MessagePublisher},
    CanonicalMessage, SentBatch,
};

use tokio::sync::Mutex;

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

/// A maximal stretch of consecutive messages sharing one operation and one
/// target index — the unit one HTTP request can carry. Runs are issued in
/// order: reordering them would turn `insert(id=7); delete(id=7)` into a
/// document that stays in the index forever, or the reverse into one that
/// vanishes from it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Run {
    op: Op,
    index: String,
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

/// Substitutes every `${metadata:<key>}` and `${payload:<field>}` in a template
/// against one message, leaving the text around them alone, so `movies` and
/// `app_${metadata:postgres.table}` are both usable. A key the message does not
/// carry yields `None`: there is no sensible value to put in its place.
fn resolve(template: &str, message: &CanonicalMessage) -> Option<String> {
    let mut resolved = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find("${") {
        let Some(close) = rest[open..].find('}').map(|offset| open + offset) else {
            break;
        };
        resolved.push_str(&rest[..open]);
        resolved.push_str(&lookup(&rest[open + 2..close], message)?);
        rest = &rest[close + 1..];
    }
    resolved.push_str(rest);
    Some(resolved)
}

/// One `${...}` token's value. A token naming no source is left as it was
/// written rather than blanked, so a stray `${` cannot quietly eat a name.
fn lookup(inner: &str, message: &CanonicalMessage) -> Option<String> {
    let Some((source, name)) = inner.split_once(':') else {
        return Some(format!("${{{inner}}}"));
    };
    match source.trim() {
        "metadata" => message.metadata.get(name.trim()).cloned(),
        "payload" => payload_field(message, name.trim()).map(stringify),
        _ => Some(format!("${{{inner}}}")),
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

/// Frames messages as NDJSON request bodies, starting a new one whenever the
/// current body would grow past `max_bytes`. A single document larger than the
/// limit is still sent on its own: splitting it is impossible, and Meilisearch
/// naming the payload limit beats this endpoint guessing at the cause.
fn chunk_documents(
    messages: &[CanonicalMessage],
    max_bytes: usize,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut bodies: Vec<Vec<u8>> = Vec::new();
    let mut body = Vec::new();
    for message in messages {
        let mark = body.len();
        append_document(&mut body, message)?;
        if mark > 0 && body.len() > max_bytes {
            let carried = body.split_off(mark);
            bodies.push(std::mem::replace(&mut body, carried));
        }
    }
    if !body.is_empty() {
        bodies.push(body);
    }
    Ok(bodies)
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
    /// Either a literal index UID or a `${...}` template resolved per message.
    index: String,
    routed: bool,
    primary_key: Option<String>,
    method: WriteMethod,
    operation: Option<String>,
    delete_values: Vec<String>,
    wait_for_task: bool,
    max_request_bytes: usize,
    create_index: bool,
    settings: Option<serde_json::Map<String, serde_json::Value>>,
    /// Indexes this publisher has already created, so a routed batch pays for
    /// the check once per index rather than once per run.
    known_indexes: Mutex<HashSet<String>>,
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

    /// The index one message is written to. A literal `index` is the same for
    /// every message; a template is resolved against the message itself.
    fn target_index(&self, message: &CanonicalMessage) -> anyhow::Result<String> {
        if !self.routed {
            return Ok(self.index.clone());
        }
        let resolved = resolve(&self.index, message).ok_or_else(|| {
            anyhow!(
                "Meilisearch `index` template '{}' resolved to nothing for this message, so there is no index to write it to",
                self.index
            )
        })?;
        if resolved.trim().is_empty() {
            return Err(anyhow!(
                "Meilisearch `index` template '{}' resolved to an empty index name",
                self.index
            ));
        }
        Ok(resolved)
    }

    /// Creates a routed index the first time it is written to. A literal index
    /// was already created at startup, before the first document could reach it.
    async fn ensure_index(&self, index: &str) -> Result<(), MeiliError> {
        if !self.create_index || !self.routed {
            return Ok(());
        }
        if self.known_indexes.lock().await.contains(index) {
            return Ok(());
        }
        prepare_index(
            &self.client,
            index,
            self.primary_key.as_deref(),
            self.settings.as_ref(),
        )
        .await?;
        self.known_indexes.lock().await.insert(index.to_owned());
        Ok(())
    }

    /// Issues one upsert run, as several requests when the NDJSON body would
    /// exceed `max_request_bytes`. The chunks go out in source order, so an
    /// earlier document can never land after a later one.
    async fn send_documents(
        &self,
        index: &str,
        messages: &[CanonicalMessage],
    ) -> Result<Vec<u64>, RunFailure> {
        let bodies =
            chunk_documents(messages, self.max_request_bytes).map_err(RunFailure::permanent)?;
        let mut tasks = Vec::with_capacity(bodies.len());
        for body in bodies {
            tasks.push(
                self.client
                    .add_documents(index, self.primary_key.as_deref(), self.method, body)
                    .await?,
            );
        }
        Ok(tasks)
    }

    /// Splits the batch into contiguous runs sharing an operation and a target
    /// index, stopping at the first message that cannot be placed.
    fn plan(&self, messages: &[CanonicalMessage]) -> (Vec<Run>, Option<(usize, anyhow::Error)>) {
        let mut runs: Vec<Run> = Vec::new();
        for (position, message) in messages.iter().enumerate() {
            let placed = self
                .classify(message)
                .and_then(|op| self.target_index(message).map(|index| (op, index)));
            let (op, index) = match placed {
                Ok(placed) => placed,
                Err(error) => return (runs, Some((position, error))),
            };
            match runs.last_mut() {
                Some(run) if run.op == op && run.index == index => run.end = position + 1,
                _ => runs.push(Run {
                    op,
                    index,
                    start: position,
                    end: position + 1,
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

    async fn send_run(&self, run: &Run, messages: &[CanonicalMessage]) -> Result<(), RunFailure> {
        self.ensure_index(&run.index).await?;
        let tasks = match run.op {
            Op::Upsert => self.send_documents(&run.index, messages).await?,
            Op::Delete => {
                let ids = self.delete_ids(messages).map_err(RunFailure::permanent)?;
                vec![self.client.delete_documents(&run.index, &ids).await?]
            }
        };
        if self.wait_for_task {
            for task in tasks {
                self.client.wait_for_task(task).await?;
            }
        }
        Ok(())
    }
}

/// Creates an index and applies the settings it was configured with. The
/// settings are merged after creation rather than sent with it, because
/// `POST /indexes` accepts only `uid` and `primaryKey`, and because an index
/// that already existed still has to end up with the settings the route asked
/// for.
async fn prepare_index(
    client: &MeiliClient,
    index: &str,
    primary_key: Option<&str>,
    settings: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<(), MeiliError> {
    client.create_index(index, primary_key).await?;
    match settings {
        Some(settings) if !settings.is_empty() => client.update_settings(index, settings).await,
        _ => Ok(()),
    }
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessagePublisher>> {
    let (settings, index) = config::resolve_for_publisher(route_name, value)?;
    let routed = config::is_template(&index);
    let client = MeiliClient::new(&settings)
        .map_err(|error| anyhow::Error::new(PublisherError::NonRetryable(error)))?;
    // A routed index is not known until a message names it, so its creation is
    // deferred to the first write instead.
    if settings.create_index && !routed {
        if let Err(error) = prepare_index(
            &client,
            &index,
            settings.primary_key.as_deref(),
            settings.settings.as_ref(),
        )
        .await
        {
            let reported = anyhow!("failed to prepare Meilisearch index '{index}': {error}");
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
        routed,
        primary_key: settings.primary_key,
        method: settings.method,
        operation: settings.operation,
        delete_values: settings.delete_values,
        wait_for_task: settings.wait_for_task,
        max_request_bytes: settings.max_request_bytes as usize,
        create_index: settings.create_index,
        settings: settings.settings,
        known_indexes: Mutex::default(),
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
            if let Err(failure) = self.send_run(run, &messages[run.start..run.end]).await {
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
        routing_publisher(operation, "movies")
    }

    fn routing_publisher(operation: Option<&str>, index: &str) -> MeilisearchPublisher {
        MeilisearchPublisher {
            client: MeiliClient::new(
                &serde_json::from_value(serde_json::json!({"url": "http://localhost:7700"}))
                    .unwrap(),
            )
            .unwrap(),
            index: index.to_owned(),
            routed: config::is_template(index),
            primary_key: Some("id".to_owned()),
            method: WriteMethod::Replace,
            operation: operation.map(str::to_owned),
            delete_values: vec!["delete".to_owned()],
            wait_for_task: true,
            max_request_bytes: usize::MAX,
            create_index: true,
            settings: None,
            known_indexes: Mutex::default(),
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
    fn a_token_can_sit_inside_surrounding_text() {
        let mut message = message(7, None);
        message
            .metadata
            .insert("postgres.table".to_owned(), "movies".to_owned());

        assert_eq!(
            resolve("app_${metadata:postgres.table}", &message).as_deref(),
            Some("app_movies")
        );
        assert_eq!(
            resolve("${metadata:postgres.table}_${payload:id}_v2", &message).as_deref(),
            Some("movies_7_v2")
        );
        assert_eq!(resolve("${metadata:absent}_suffix", &message), None);
    }

    /// A `${` that names no source is text, not a silently empty substitution.
    #[test]
    fn an_unterminated_or_unknown_token_stays_as_written() {
        let message = message(7, None);
        assert_eq!(resolve("a${b", &message).as_deref(), Some("a${b"));
        assert_eq!(
            resolve("${nosuch:x}", &message).as_deref(),
            Some("${nosuch:x}")
        );
        assert_eq!(resolve("${plain}", &message).as_deref(), Some("${plain}"));
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
                index: "movies".to_owned(),
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
                    index: "movies".to_owned(),
                    start: 0,
                    end: 1
                },
                Run {
                    op: Op::Delete,
                    index: "movies".to_owned(),
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

    fn routed_message(id: u64, table: &str, operation: Option<&str>) -> CanonicalMessage {
        let mut message = message(id, operation);
        message
            .metadata
            .insert("postgres.table".to_owned(), table.to_owned());
        message
    }

    #[test]
    fn a_templated_index_routes_each_message_by_its_own_metadata() {
        let publisher = routing_publisher(None, "${metadata:postgres.table}");
        let messages = vec![
            routed_message(1, "movies", None),
            routed_message(2, "movies", None),
            routed_message(3, "directors", None),
        ];

        let (runs, stopped) = publisher.plan(&messages);

        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![
                Run {
                    op: Op::Upsert,
                    index: "movies".to_owned(),
                    start: 0,
                    end: 2
                },
                Run {
                    op: Op::Upsert,
                    index: "directors".to_owned(),
                    start: 2,
                    end: 3
                }
            ]
        );
    }

    /// Two indexes never share a document, but the runs still go out in source
    /// order, so a message cannot overtake an earlier one from its own table.
    #[test]
    fn switching_back_to_an_earlier_index_opens_a_new_run() {
        let publisher = routing_publisher(None, "${metadata:postgres.table}");
        let messages = vec![
            routed_message(1, "movies", None),
            routed_message(2, "directors", None),
            routed_message(3, "movies", None),
        ];

        let (runs, _) = publisher.plan(&messages);

        let targets: Vec<_> = runs.iter().map(|run| run.index.as_str()).collect();
        assert_eq!(targets, vec!["movies", "directors", "movies"]);
    }

    #[test]
    fn an_operation_and_an_index_both_split_a_run() {
        let publisher = routing_publisher(
            Some("${metadata:postgres.operation}"),
            "${metadata:postgres.table}",
        );
        let messages = vec![
            routed_message(1, "movies", Some("insert")),
            routed_message(2, "movies", Some("delete")),
            routed_message(3, "directors", Some("delete")),
        ];

        let (runs, _) = publisher.plan(&messages);

        assert_eq!(runs.len(), 3);
        assert_eq!(runs[1].op, Op::Delete);
        assert_eq!(runs[1].index, "movies");
        assert_eq!(runs[2].index, "directors");
    }

    #[test]
    fn a_message_the_index_template_cannot_resolve_stops_the_plan() {
        let publisher = routing_publisher(None, "${metadata:postgres.table}");
        let messages = vec![routed_message(1, "movies", None), message(2, None)];

        let (runs, stopped) = publisher.plan(&messages);

        assert_eq!(runs.len(), 1);
        let (position, error) = stopped.expect("an unroutable message should stop the plan");
        assert_eq!(position, 1);
        assert!(error.to_string().contains("resolved to nothing"));
    }

    #[test]
    fn an_index_template_resolving_to_blank_is_rejected() {
        let publisher = routing_publisher(None, "${metadata:postgres.table}");
        assert!(publisher
            .target_index(&routed_message(1, "   ", None))
            .is_err());
    }

    #[test]
    fn a_literal_index_is_used_for_every_message_unchanged() {
        let publisher = publisher(None);
        assert_eq!(
            publisher
                .target_index(&routed_message(1, "other", None))
                .unwrap(),
            "movies"
        );
    }

    #[test]
    fn documents_are_framed_into_one_request_when_they_fit() {
        let messages: Vec<_> = (1..=3).map(|id| message(id, None)).collect();
        let bodies = chunk_documents(&messages, usize::MAX).unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].iter().filter(|byte| **byte == b'\n').count(), 3);
    }

    #[test]
    fn a_run_too_large_for_one_request_is_split_in_source_order() {
        let messages: Vec<_> = (1..=5).map(|id| message(id, None)).collect();
        let one_line = chunk_documents(&messages[..1], usize::MAX).unwrap()[0].len();

        let bodies = chunk_documents(&messages, one_line * 2).unwrap();

        assert_eq!(bodies.len(), 3);
        let rejoined: Vec<u8> = bodies.concat();
        assert_eq!(rejoined, chunk_documents(&messages, usize::MAX).unwrap()[0]);
        assert!(bodies
            .iter()
            .all(|body| body.len() <= one_line * 2 && !body.is_empty()));
    }

    /// Splitting one document is impossible, so it is sent alone and
    /// Meilisearch is left to name the payload limit it broke.
    #[test]
    fn a_single_oversized_document_is_still_sent_on_its_own() {
        let messages: Vec<_> = (1..=2).map(|id| message(id, None)).collect();
        let bodies = chunk_documents(&messages, 1).unwrap();
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn an_empty_run_produces_no_request() {
        assert!(chunk_documents(&[], usize::MAX).unwrap().is_empty());
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

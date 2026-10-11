use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicBool, Ordering},
};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mq_bridge::{
    errors::{InvalidConfig, PublisherError},
    support::interpolation::CompiledTemplate,
    traits::{disconnect_outcome, BoxFuture, DisconnectOutcome, EndpointStatus, MessagePublisher},
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Op {
    Upsert,
    Delete,
}

impl Op {
    fn other(self) -> Self {
        match self {
            Op::Upsert => Op::Delete,
            Op::Delete => Op::Upsert,
        }
    }
}

/// The messages of one segment sharing an operation and a target index, in
/// source order — the unit one HTTP request can carry. Only writes to the same
/// document must keep their order, so a segment holds at most one run per
/// operation and index, and a new segment opens only when a document already in
/// it switches operation: issuing `insert(id=7); delete(id=7)` out of order
/// would leave document 7 in the index for good. Segments are issued in order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Run {
    op: Op,
    index: String,
    segment: usize,
    positions: Vec<usize>,
}

/// A batch grouped for sending: the runs, the messages set aside because
/// Meilisearch would refuse them, and where planning stopped, if it did.
#[derive(Debug, Default)]
struct Plan {
    runs: Vec<Run>,
    /// Each message's document id, read once here for the steps that need it.
    ids: Vec<Option<serde_json::Value>>,
    rejected: Vec<(usize, anyhow::Error)>,
    stopped: Option<(usize, anyhow::Error)>,
}

/// Meilisearch's rule for a document id: an integer, or a non-empty string of
/// at most 511 bytes of ASCII letters, digits, `-` and `_`.
fn is_valid_document_id(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Number(number) => number.is_i64() || number.is_u64(),
        serde_json::Value::String(text) => {
            !text.is_empty()
                && text.len() <= 511
                && text
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        }
        _ => false,
    }
}

/// Why a run could not be applied, already classified for the route.
#[derive(Clone, Debug)]
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
            retryable: error.is_retryable(),
            reason: describe(&error),
        }
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

/// A template's value for one message, or `None` when a token it names is absent:
/// there is no sensible value to put in its place.
fn resolve(template: &CompiledTemplate, message: &CanonicalMessage) -> Option<String> {
    String::from_utf8(template.render_resolved(Some(message))?).ok()
}

/// Reads one top-level field of a JSON object and skips every other value
/// unparsed, so a document carrying a large vector costs no allocation per
/// number. Fails when the payload is not one valid JSON object.
fn top_level_field(payload: &[u8], field: &str) -> serde_json::Result<Option<serde_json::Value>> {
    struct FieldOf<'a>(&'a str);

    impl<'de> serde::de::Visitor<'de> for FieldOf<'_> {
        type Value = Option<serde_json::Value>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a JSON object")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut found = None;
            while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                if key == self.0 {
                    found = Some(map.next_value()?);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(found)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let found = serde::Deserializer::deserialize_map(&mut deserializer, FieldOf(field))?;
    deserializer.end()?;
    Ok(found)
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

/// Frames messages as NDJSON request bodies one at a time, so a batch is never
/// held a second time as bodies waiting to be sent. A new body starts whenever
/// the current one would grow past `max_bytes`. A single document larger than
/// the limit is still sent on its own: splitting it is impossible, and
/// Meilisearch naming the payload limit beats this endpoint guessing at the cause.
struct DocumentChunks<I> {
    messages: I,
    carried: Vec<u8>,
    max_bytes: usize,
}

impl<'a, I: Iterator<Item = &'a CanonicalMessage>> DocumentChunks<I> {
    fn new(messages: impl IntoIterator<IntoIter = I>, max_bytes: usize) -> Self {
        Self {
            messages: messages.into_iter(),
            carried: Vec::new(),
            max_bytes,
        }
    }

    fn next_body(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        let mut body = std::mem::take(&mut self.carried);
        for message in self.messages.by_ref() {
            let mark = body.len();
            append_document(&mut body, message)?;
            if mark > 0 && body.len() > self.max_bytes {
                self.carried = body.split_off(mark);
                return Ok(Some(body));
            }
        }
        Ok(Some(body).filter(|body| !body.is_empty()))
    }
}

/// Decides, per message, what goes back to the route. `outcomes` covers a
/// prefix of `runs`; the runs after it were never sent or never confirmed.
/// A failed run is handed back with its own failure; every run of a later
/// segment and every unconfirmed run goes back with the first failure, since a
/// later write to the same document must not land ahead of it. Resending a
/// write that did land is harmless: upsert and delete by key are idempotent.
fn settle(
    runs: &[Run],
    outcomes: &[Result<(), RunFailure>],
    stopped: Option<(usize, RunFailure)>,
    len: usize,
) -> Vec<Option<RunFailure>> {
    let mut failures = vec![None; len];
    let failed = runs.iter().zip(outcomes).find_map(|(run, outcome)| {
        outcome
            .as_ref()
            .err()
            .map(|failure| (run.segment, failure.clone()))
    });
    if let Some((cut, first)) = failed {
        for (i, run) in runs.iter().enumerate() {
            let failure = match outcomes.get(i) {
                Some(Err(failure)) => failure.clone(),
                Some(Ok(())) if run.segment <= cut => continue,
                _ => first.clone(),
            };
            for &position in &run.positions {
                failures[position] = Some(failure.clone());
            }
        }
    }
    if let Some((from, failure)) = stopped {
        for slot in &mut failures[from..] {
            slot.get_or_insert_with(|| failure.clone());
        }
    }
    failures
}

/// How many ids one existence check asks about; keeps the request small.
const EXISTENCE_CHECK_CHUNK: usize = 1_000;

/// Hands the failed messages back in source order, so a retry replays them in
/// the order they were written.
fn report(messages: Vec<CanonicalMessage>, failures: Vec<Option<RunFailure>>) -> SentBatch {
    let failed = messages
        .into_iter()
        .zip(failures)
        .filter_map(|(message, failure)| {
            let failure = failure?;
            let error = anyhow!("{}", failure.reason);
            let classified = if failure.retryable {
                PublisherError::Retryable(error)
            } else {
                PublisherError::NonRetryable(error)
            };
            Some((message, classified))
        })
        .collect();
    SentBatch::from_failures(failed)
}

/// `update_where`, compiled: each message patches the documents whose
/// `attribute` equals `value` by writing itself to their `into` field.
struct UpdateWhere {
    attribute: String,
    value: CompiledTemplate,
    into: String,
}

/// What this publisher's route pass has done so far, which decides whether the
/// index it built may replace the live one.
#[derive(Default)]
struct Pass {
    written: AtomicBool,
    refused: AtomicBool,
    /// Documents handed back to be retried and not written since.
    unwritten: std::sync::Mutex<HashSet<String>>,
}

impl Pass {
    /// A retried document that then succeeds no longer counts against the
    /// swap; one refused for good, or failing without an id to follow, does.
    fn record(&self, ids: &[Option<serde_json::Value>], failures: &[Option<RunFailure>]) {
        self.written.store(true, Ordering::Relaxed);
        let mut unwritten = self
            .unwritten
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        for (position, failure) in failures.iter().enumerate() {
            let id = ids
                .get(position)
                .and_then(Option::as_ref)
                .map(|id| id.to_string());
            match (failure, id) {
                (None, Some(id)) => {
                    unwritten.remove(&id);
                }
                (None, None) => {}
                (Some(failure), Some(id)) if failure.retryable => {
                    unwritten.insert(id);
                }
                (Some(_), _) => self.refused.store(true, Ordering::Relaxed),
            }
        }
    }

    /// Why the built index must not go live, or `None` when it may. Only a
    /// route that drained its input to the end has built the whole index, and
    /// a host older than mq-bridge 0.4.18 reports no outcome at all.
    fn objection(&self, outcome: Option<DisconnectOutcome>) -> Option<&'static str> {
        match outcome {
            Some(DisconnectOutcome::Completed) => {}
            Some(DisconnectOutcome::Failed) => return Some("the route ended with an error"),
            Some(DisconnectOutcome::Stopped) => {
                return Some("the route was stopped before its input was drained")
            }
            None => {
                return Some(
                    "the host does not report how the route ended (mq-bridge 0.4.18+ does)",
                )
            }
        }
        let unwritten = self
            .unwritten
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.refused.load(Ordering::Relaxed) || !unwritten.is_empty() {
            return Some("at least one document could not be written");
        }
        if !self.written.load(Ordering::Relaxed) {
            return Some("nothing was written, and an empty index would replace the live one");
        }
        None
    }
}

/// How many matching document ids one page of an `update_where` lookup reads.
const PATCH_PAGE: usize = 1_000;

struct MeilisearchPublisher {
    client: MeiliClient,
    /// The configured index, as written.
    index: String,
    /// Set when `index` is a `${...}` template, resolved per message.
    index_template: Option<CompiledTemplate>,
    primary_key: Option<String>,
    method: WriteMethod,
    operation: Option<CompiledTemplate>,
    delete_values: Vec<String>,
    wait_for_task: bool,
    max_request_bytes: usize,
    create_index: bool,
    settings: Option<serde_json::Map<String, serde_json::Value>>,
    update_where: Option<UpdateWhere>,
    swap_into: Option<String>,
    pass: Pass,
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
        let Some(template) = &self.index_template else {
            return Ok(self.index.clone());
        };
        let resolved = resolve(template, message).ok_or_else(|| {
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
        if !self.create_index || self.index_template.is_none() {
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
    async fn send_documents<'a, I>(&self, index: &str, messages: I) -> Result<Vec<u64>, RunFailure>
    where
        I: IntoIterator<Item = &'a CanonicalMessage>,
        I::IntoIter: Send,
    {
        let mut chunks = DocumentChunks::new(messages, self.max_request_bytes);
        let mut tasks = Vec::new();
        while let Some(body) = chunks.next_body().map_err(RunFailure::permanent)? {
            tasks.push(
                self.client
                    .add_documents(index, self.primary_key.as_deref(), self.method, body)
                    .await?,
            );
        }
        Ok(tasks)
    }

    /// The id of the document a message writes, when it can be told: without a
    /// configured `primary_key` Meilisearch infers the key, so the plan must
    /// assume any two messages may touch the same document. A document
    /// Meilisearch would refuse is rejected here, so it cannot fail the task of
    /// the others in its run.
    fn document_id(&self, message: &CanonicalMessage) -> anyhow::Result<Option<serde_json::Value>> {
        let key = self.primary_key.as_deref();
        let value = top_level_field(&message.payload, key.unwrap_or_default()).map_err(|_| {
            anyhow!("Meilisearch stores JSON objects; this message's payload is not one. Use a `transform` middleware to shape it before the sink.")
        })?;
        let Some(key) = key else {
            return Ok(None);
        };
        let value = value.ok_or_else(|| {
            anyhow!("the document carries no `{key}` field, so Meilisearch cannot tell which document it is")
        })?;
        if !is_valid_document_id(&value) {
            return Err(anyhow!(
                "`{key}` {value} is not a valid Meilisearch document id: use an integer, or a string of at most 511 bytes of a-z A-Z 0-9 - _"
            ));
        }
        Ok(Some(value))
    }

    /// Groups the batch into segments of runs (see [`Run`]), setting aside
    /// documents Meilisearch would refuse and stopping at the first message
    /// that cannot be placed.
    fn plan(&self, messages: &[CanonicalMessage]) -> Plan {
        let mut plan = Plan {
            ids: vec![None; messages.len()],
            ..Plan::default()
        };
        let runs = &mut plan.runs;
        let mut segment = 0;
        let mut segment_start = 0;
        let mut keyed: HashMap<(String, String), Op> = HashMap::new();
        let mut unkeyed: HashSet<(String, Op)> = HashSet::new();
        for (position, message) in messages.iter().enumerate() {
            let placed = self
                .classify(message)
                .and_then(|op| self.target_index(message).map(|index| (op, index)));
            let (op, index) = match placed {
                Ok(placed) => placed,
                Err(error) => {
                    plan.stopped = Some((position, error));
                    return plan;
                }
            };
            let key = match self.document_id(message) {
                Ok(id) => {
                    let key = id.clone().map(stringify);
                    plan.ids[position] = id;
                    key
                }
                Err(error) => {
                    plan.rejected.push((position, error));
                    continue;
                }
            };
            let conflicts = match &key {
                Some(key) => {
                    unkeyed.contains(&(index.clone(), op.other()))
                        || keyed
                            .get(&(index.clone(), key.clone()))
                            .is_some_and(|seen| *seen != op)
                }
                None => runs[segment_start..]
                    .iter()
                    .any(|run| run.index == index && run.op != op),
            };
            if conflicts {
                segment += 1;
                segment_start = runs.len();
                keyed.clear();
                unkeyed.clear();
            }
            match key {
                Some(key) => keyed.insert((index.clone(), key), op),
                None => {
                    unkeyed.insert((index.clone(), op));
                    None
                }
            };
            match runs[segment_start..]
                .iter_mut()
                .find(|run| run.op == op && run.index == index)
            {
                Some(run) => run.positions.push(position),
                None => runs.push(Run {
                    op,
                    index,
                    segment,
                    positions: vec![position],
                }),
            }
        }
        plan
    }

    /// The ids a delete run removes, as read by the plan.
    fn delete_ids(
        &self,
        positions: &[usize],
        ids: &[Option<serde_json::Value>],
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        if self.primary_key.is_none() {
            return Err(anyhow!(
                "Meilisearch `primary_key` is required to delete a document by id"
            ));
        }
        positions
            .iter()
            .map(|&position| {
                ids[position].clone().ok_or_else(|| {
                    anyhow!("a delete carries no id, so the document to remove is unknown")
                })
            })
            .collect()
    }

    /// Keeps only the positions whose document already exists in `index`, for
    /// `update_existing`. Messages dropped here are acknowledged unwritten.
    /// Waits for the index's queued writes first, so a parent another route
    /// just created or deleted is seen.
    async fn keep_existing(
        &self,
        index: &str,
        positions: &[usize],
        ids: &[Option<serde_json::Value>],
    ) -> Result<Vec<usize>, RunFailure> {
        let Some(primary_key) = self.primary_key.as_deref() else {
            return Ok(positions.to_vec());
        };
        self.await_queued_writes(index).await?;
        let keys: Vec<String> = positions
            .iter()
            .map(|&position| ids[position].clone().map(stringify).unwrap_or_default())
            .collect();
        let mut wanted: Vec<String> = keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        wanted.sort_unstable();
        let mut existing = HashSet::with_capacity(wanted.len());
        for chunk in wanted.chunks(EXISTENCE_CHECK_CHUNK) {
            let found = self.client.existing_ids(index, primary_key, chunk).await?;
            existing.extend(found.into_iter().map(stringify));
        }
        let kept: Vec<_> = positions
            .iter()
            .zip(keys)
            .filter_map(|(&position, key)| existing.contains(&key).then_some(position))
            .collect();
        let skipped = wanted.len() - existing.len();
        if skipped > 0 {
            tracing::debug!(
                index,
                skipped,
                "Meilisearch update_existing skipped documents that do not exist"
            );
        }
        Ok(kept)
    }

    /// Ends a rebuild: swaps the built index with the live one and deletes
    /// what is left under the build name, which is then the old documents.
    async fn swap_in(&self, live: &str) -> Result<(), MeiliError> {
        if let Some(objection) = self.pass.objection(disconnect_outcome()) {
            tracing::warn!(
                "Meilisearch index '{}' was NOT swapped into '{live}': {objection}. '{live}' is unchanged",
                self.index
            );
            return Ok(());
        }
        // Meilisearch refuses to swap with an index that does not exist.
        self.client
            .create_index(live, self.primary_key.as_deref())
            .await?;
        self.client.swap_indexes(&self.index, live).await?;
        tracing::info!("Meilisearch index '{}' is now live as '{live}'", self.index);
        self.client.delete_index(&self.index).await?;
        Ok(())
    }

    fn reported(
        &self,
        messages: Vec<CanonicalMessage>,
        ids: &[Option<serde_json::Value>],
        failures: Vec<Option<RunFailure>>,
    ) -> SentBatch {
        if self.swap_into.is_some() {
            self.pass.record(ids, &failures);
        }
        report(messages, failures)
    }

    /// Waits for the writes already queued on `index`, so a read that follows
    /// sees them. A queued task that fails is its own route's to report.
    async fn await_queued_writes(&self, index: &str) -> Result<(), MeiliError> {
        if let Some(task) = self.client.latest_pending_task(index).await? {
            match self.client.wait_for_task(task).await {
                Ok(()) | Err(MeiliError::Task { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// What one `update_where` message asks for: the index, the filter naming
    /// the documents to patch, and the message as one line of JSON.
    fn patch_of(
        &self,
        patch: &UpdateWhere,
        message: &CanonicalMessage,
    ) -> anyhow::Result<(String, String, Vec<u8>)> {
        let index = self.target_index(message)?;
        let value = resolve(&patch.value, message)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "Meilisearch `update_where` resolved to no value for this message, so there is nothing to match `{}` against",
                    patch.attribute
                )
            })?;
        let filter = format!("{} = {}", patch.attribute, serde_json::Value::String(value));
        let mut document = Vec::with_capacity(message.payload.len() + 1);
        append_document(&mut document, message)?;
        document.pop();
        top_level_field(&document, "").context("message payload is not valid JSON")?;
        Ok((index, filter, document))
    }

    /// Enqueues the updates one `update_where` message causes: its payload
    /// written to `into` on every document `filter` matches. Only the ids are
    /// held; the bodies are sent as they fill.
    async fn issue_patch(
        &self,
        into: &str,
        (index, filter, document): (String, String, Vec<u8>),
        settled: &mut HashSet<String>,
    ) -> Result<Vec<u64>, RunFailure> {
        let key = self.primary_key.as_deref().unwrap_or_default();
        self.ensure_index(&index).await?;
        // Documents another route has just enqueued must be matched too.
        if settled.insert(index.clone()) {
            self.await_queued_writes(&index).await?;
        }
        let (key_json, into_json) = (
            serde_json::Value::from(key).to_string(),
            serde_json::Value::from(into).to_string(),
        );
        // Every id is read before the first write: an update enqueued while
        // paging could reorder the pages and hide a document.
        let mut ids = Vec::new();
        loop {
            let page = self
                .client
                .matching_ids(&index, key, &filter, ids.len(), PATCH_PAGE)
                .await?;
            let read = page.len();
            ids.extend(page);
            if read < PATCH_PAGE {
                break;
            }
        }
        let mut tasks = Vec::new();
        let mut body = Vec::new();
        for id in ids {
            let mark = body.len();
            body.extend_from_slice(format!("{{{key_json}:{id},{into_json}:").as_bytes());
            body.extend_from_slice(&document);
            body.extend_from_slice(b"}\n");
            if mark > 0 && body.len() > self.max_request_bytes {
                let carried = body.split_off(mark);
                let full = std::mem::replace(&mut body, carried);
                tasks.push(self.send_patch(&index, full).await?);
            }
        }
        if !body.is_empty() {
            tasks.push(self.send_patch(&index, body).await?);
        }
        Ok(tasks)
    }

    async fn send_patch(&self, index: &str, body: Vec<u8>) -> Result<u64, MeiliError> {
        self.client
            .add_documents(
                index,
                self.primary_key.as_deref(),
                WriteMethod::Update,
                body,
            )
            .await
    }

    /// Applies a batch of `update_where` messages in source order. A message
    /// that cannot be turned into a patch fails alone; once Meilisearch refuses
    /// one, it and every later message go back, so two changes to the same
    /// parent are never applied out of order.
    async fn patch_batch(
        &self,
        patch: &UpdateWhere,
        messages: &[CanonicalMessage],
    ) -> Vec<Option<RunFailure>> {
        let mut failures = vec![None; messages.len()];
        let mut issued = Vec::with_capacity(messages.len());
        let mut settled = HashSet::new();
        for (position, message) in messages.iter().enumerate() {
            let request = match self.patch_of(patch, message) {
                Ok(request) => request,
                Err(error) => {
                    failures[position] = Some(RunFailure::permanent(error));
                    continue;
                }
            };
            match self.issue_patch(&patch.into, request, &mut settled).await {
                Ok(tasks) => issued.push((position, tasks)),
                Err(failure) => {
                    issued.push((position, Vec::new()));
                    failures[position] = Some(failure);
                    break;
                }
            }
        }
        let mut first = None;
        for (position, tasks) in &issued {
            let outcome = match (&first, &failures[*position]) {
                (Some(first), _) | (None, Some(first)) => Err(RunFailure::clone(first)),
                (None, None) => self.confirm(tasks).await,
            };
            if let Err(failure) = outcome {
                failures[*position].get_or_insert_with(|| failure.clone());
                first.get_or_insert(failure);
            }
        }
        if let (Some(first), Some((last, _))) = (first, issued.last()) {
            for slot in &mut failures[last + 1..] {
                slot.get_or_insert_with(|| first.clone());
            }
        }
        failures
    }

    /// Enqueues one run and returns its tasks without waiting for them.
    async fn issue_run(
        &self,
        run: &Run,
        messages: &[CanonicalMessage],
        ids: &[Option<serde_json::Value>],
    ) -> Result<Vec<u64>, RunFailure> {
        self.ensure_index(&run.index).await?;
        match run.op {
            Op::Upsert if self.method == WriteMethod::UpdateExisting => {
                let kept = self.keep_existing(&run.index, &run.positions, ids).await?;
                let selected = kept.into_iter().map(|position| &messages[position]);
                self.send_documents(&run.index, selected).await
            }
            Op::Upsert => {
                let positions = run.positions.iter().copied();
                let selected = positions.map(|position| &messages[position]);
                self.send_documents(&run.index, selected).await
            }
            Op::Delete => {
                let ids = self
                    .delete_ids(&run.positions, ids)
                    .map_err(RunFailure::permanent)?;
                Ok(vec![self.client.delete_documents(&run.index, &ids).await?])
            }
        }
    }

    /// Waits for a run's tasks, unless the route opted out of waiting.
    async fn confirm(&self, tasks: &[u64]) -> Result<(), RunFailure> {
        if self.wait_for_task {
            self.client.wait_for_tasks(tasks).await?;
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
    let (settings, index) = config::resolve(route_name, value).map_err(InvalidConfig)?;
    let client = MeiliClient::new(&settings).map_err(InvalidConfig)?;
    let compile = |template: &str| CompiledTemplate::compile(template, None).map_err(InvalidConfig);
    let index_template = Some(compile(&index)?).filter(CompiledTemplate::is_dynamic);
    let routed = index_template.is_some();
    let operation = settings.operation.as_deref().map(compile).transpose()?;
    let update_where = match (&settings.update_where, &settings.update_into) {
        (Some(condition), Some(into)) => {
            let (attribute, value) =
                config::split_update_where(condition).map_err(InvalidConfig)?;
            Some(UpdateWhere {
                attribute: attribute.to_owned(),
                value: compile(value)?,
                into: into.clone(),
            })
        }
        _ => None,
    };
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
            return Err(if error.is_retryable() {
                reported
            } else {
                anyhow::Error::new(PublisherError::NonRetryable(reported))
            });
        }
    }
    Ok(Box::new(MeilisearchPublisher {
        client,
        index,
        index_template,
        primary_key: settings.primary_key,
        method: settings.method,
        operation,
        delete_values: settings.delete_values,
        wait_for_task: settings.wait_for_task,
        max_request_bytes: settings.max_request_bytes as usize,
        create_index: settings.create_index,
        settings: settings.settings,
        update_where,
        swap_into: settings.swap_into,
        pass: Pass::default(),
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
        if let Some(patch) = &self.update_where {
            let failures = self.patch_batch(patch, &messages).await;
            return Ok(self.reported(messages, &[], failures));
        }

        // Every run is enqueued before any is awaited: Meilisearch applies an
        // index's tasks in enqueue order and can batch queued tasks together,
        // whereas awaiting each run in turn costs a full indexing round trip per run.
        let Plan {
            runs,
            ids,
            rejected,
            stopped,
        } = self.plan(&messages);
        let mut issued = Vec::with_capacity(runs.len());
        let mut unissued = None;
        for run in &runs {
            match self.issue_run(run, &messages, &ids).await {
                Ok(tasks) => issued.push(tasks),
                Err(failure) => {
                    unissued = Some(failure);
                    break;
                }
            }
        }
        let mut outcomes = Vec::with_capacity(runs.len());
        let mut cut = None;
        for (run, tasks) in runs.iter().zip(&issued) {
            if cut.is_some_and(|cut| run.segment > cut) {
                break;
            }
            let outcome = self.confirm(tasks).await;
            if outcome.is_err() {
                cut.get_or_insert(run.segment);
            }
            outcomes.push(outcome);
        }
        if let Some(failure) = unissued {
            if outcomes.len() == issued.len() {
                outcomes.push(Err(failure));
            }
        }
        let stopped = stopped.map(|(at, error)| (at, RunFailure::permanent(error)));
        let mut failures = settle(&runs, &outcomes, stopped, messages.len());
        for (position, error) in rejected {
            failures[position] = Some(RunFailure::permanent(error));
        }
        Ok(self.reported(messages, &ids, failures))
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        let live = self.swap_into.as_deref()?;
        Some(Box::pin(async move {
            self.swap_in(live).await.map_err(|error| {
                let index = &self.index;
                tracing::error!(
                    "Meilisearch could not finish swapping '{index}' into '{live}': {error}"
                );
                anyhow!("swapping '{index}' into '{live}' failed: {error}")
            })
        }))
    }

    /// Meilisearch keys documents by primary key, so two batches touching one
    /// document must reach it in source order. A plugin host reads this through
    /// the ABI 1.1 ordering slot.
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

    fn template(text: &str) -> CompiledTemplate {
        CompiledTemplate::compile(text, None).unwrap()
    }

    fn render(text: &str, message: &CanonicalMessage) -> Option<String> {
        resolve(&template(text), message)
    }

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
            index_template: Some(template(index)).filter(CompiledTemplate::is_dynamic),
            primary_key: Some("id".to_owned()),
            method: WriteMethod::Replace,
            operation: operation.map(template),
            delete_values: vec!["delete".to_owned()],
            wait_for_task: true,
            max_request_bytes: usize::MAX,
            create_index: true,
            settings: None,
            update_where: None,
            swap_into: None,
            pass: Pass::default(),
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

    /// A Meilisearch whose `movies` index holds two documents by author 5, and
    /// the request lines it was sent.
    async fn meilisearch_with_two_matches(
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = requests.clone();
        let url = crate::client::stub::serve(move |request| {
            seen.lock().unwrap().push(request.to_owned());
            if request.starts_with("POST /indexes/movies/documents/fetch") {
                (
                    200,
                    r#"{"results":[{"id":1},{"id":2}],"total":2}"#.to_owned(),
                )
            } else if request.starts_with("PUT /indexes/movies/documents") {
                (202, r#"{"taskUid":7}"#.to_owned())
            } else if request.starts_with("GET /tasks?") {
                (200, r#"{"results":[]}"#.to_owned())
            } else {
                (200, r#"{"uid":7,"status":"succeeded"}"#.to_owned())
            }
        })
        .await;
        (url, requests)
    }

    async fn patching_publisher(url: &str) -> Box<dyn MessagePublisher> {
        create(
            "movies",
            &serde_json::json!({
                "url": url,
                "index": "movies",
                "primary_key": "id",
                "create_index": false,
                "update_where": "author.id = ${payload:id}",
                "update_into": "author",
            }),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn update_where_merges_the_message_into_the_documents_it_matches() {
        let (url, requests) = meilisearch_with_two_matches().await;
        let publisher = patching_publisher(&url).await;

        let author = CanonicalMessage::from(r#"{"id":5,"name":"Lem"}"#.to_owned());
        let sent = publisher.send_batch(vec![author]).await.unwrap();

        assert!(matches!(sent, SentBatch::Ack));
        let requests = requests.lock().unwrap();
        let writes: Vec<_> = requests
            .iter()
            .filter(|request| !request.starts_with("GET") && !request.contains("/fetch"))
            .collect();
        assert_eq!(
            writes.len(),
            1,
            "both matches travel in one body: {requests:?}"
        );
        assert!(writes[0].starts_with("PUT /indexes/movies/documents"));
    }

    #[tokio::test]
    async fn an_update_where_message_without_the_matched_value_fails_alone() {
        let (url, _) = meilisearch_with_two_matches().await;
        let publisher = patching_publisher(&url).await;

        let nameless = CanonicalMessage::from(r#"{"name":"Lem"}"#.to_owned());
        let author = CanonicalMessage::from(r#"{"id":5,"name":"Lem"}"#.to_owned());
        let sent = publisher.send_batch(vec![nameless, author]).await.unwrap();

        let SentBatch::Partial { failed, .. } = sent else {
            panic!("the message naming no author must be handed back");
        };
        assert_eq!(failed.len(), 1);
        assert!(matches!(failed[0].1, PublisherError::NonRetryable(_)));
    }

    #[test]
    fn the_update_where_value_is_quoted_so_it_cannot_widen_the_filter() {
        let publisher = publisher(None);
        let patch = UpdateWhere {
            attribute: "author.id".to_owned(),
            value: template("${payload:id}"),
            into: "author".to_owned(),
        };
        let hostile = CanonicalMessage::from(
            serde_json::json!({"id": "5\" OR id EXISTS OR x = \"", "name": "Lem"}).to_string(),
        );

        let (_, filter, document) = publisher.patch_of(&patch, &hostile).unwrap();

        assert_eq!(filter, r#"author.id = "5\" OR id EXISTS OR x = \"""#);
        assert!(!document.contains(&b'\n'));
    }

    #[test]
    fn only_a_drained_route_that_wrote_without_failures_may_swap() {
        use DisconnectOutcome::{Completed, Failed, Stopped};
        let failure = || Some(RunFailure::permanent(anyhow!("refused")));

        let retry = || {
            let retryable = true;
            let reason = "busy".to_owned();
            Some(RunFailure { retryable, reason })
        };
        let ids = [Some(serde_json::json!(1)), Some(serde_json::json!(2))];

        let pass = Pass::default();
        assert!(pass.objection(Some(Completed)).is_some(), "nothing written");
        pass.record(&ids, &[None, None]);
        assert!(pass.objection(Some(Completed)).is_none());
        assert!(pass.objection(Some(Stopped)).is_some());
        assert!(pass.objection(Some(Failed)).is_some());
        assert!(
            pass.objection(None).is_some(),
            "a host that reports nothing"
        );

        pass.record(&ids, &[None, retry()]);
        assert!(
            pass.objection(Some(Completed)).is_some(),
            "document 2 is missing"
        );
        pass.record(&ids[1..], &[None]);
        assert!(
            pass.objection(Some(Completed)).is_none(),
            "the retry wrote it"
        );

        pass.record(&ids, &[None, failure()]);
        pass.record(&ids, &[None, None]);
        assert!(
            pass.objection(Some(Completed)).is_some(),
            "a refusal sticks"
        );
    }

    #[test]
    fn tokens_resolve_from_metadata_payload_or_literally() {
        let message = message(7, Some("update"));
        assert_eq!(
            render("${metadata:postgres.operation}", &message).as_deref(),
            Some("update")
        );
        assert_eq!(render("${payload:id}", &message).as_deref(), Some("7"));
        assert_eq!(render("${payload:title}", &message).as_deref(), Some("x"));
        assert_eq!(render("insert", &message).as_deref(), Some("insert"));
        assert_eq!(render("${metadata:absent}", &message), None);
    }

    #[test]
    fn a_token_can_sit_inside_surrounding_text() {
        let mut message = message(7, None);
        message
            .metadata
            .insert("postgres.table".to_owned(), "movies".to_owned());

        assert_eq!(
            render("app_${metadata:postgres.table}", &message).as_deref(),
            Some("app_movies")
        );
        assert_eq!(
            render("${metadata:postgres.table}_${payload:id}_v2", &message).as_deref(),
            Some("movies_7_v2")
        );
        assert_eq!(render("${metadata:absent}_suffix", &message), None);
    }

    /// A `${` that names no source is text, not a silently empty substitution.
    #[test]
    fn an_unterminated_or_unknown_token_stays_as_written() {
        let message = message(7, None);
        assert_eq!(render("a${b", &message).as_deref(), Some("a${b"));
        assert_eq!(
            render("${nosuch:x}", &message).as_deref(),
            Some("${nosuch:x}")
        );
        assert_eq!(render("${plain}", &message).as_deref(), Some("${plain}"));
    }

    #[test]
    fn without_an_operation_mapping_every_message_is_an_upsert() {
        let publisher = publisher(None);
        let messages = vec![message(1, Some("delete")), message(2, None)];
        let Plan { runs, stopped, .. } = publisher.plan(&messages);
        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![Run {
                op: Op::Upsert,
                index: "movies".to_owned(),
                segment: 0,
                positions: vec![0, 1]
            }]
        );
    }

    /// A `capture_all` backfill row comes from a table scan and carries no
    /// operation; it must still be indexed, not stopped or deleted.
    #[test]
    fn a_message_without_the_mapped_operation_is_an_upsert() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages = vec![message(1, None), message(2, Some("insert"))];
        let Plan { runs, stopped, .. } = publisher.plan(&messages);
        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![Run {
                op: Op::Upsert,
                index: "movies".to_owned(),
                segment: 0,
                positions: vec![0, 1]
            }]
        );
    }

    /// The case that makes run order load-bearing: replaying these two in the
    /// wrong order leaves document 7 in the index for good.
    #[test]
    fn an_insert_and_a_delete_of_one_key_stay_in_separate_ordered_runs() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages = vec![message(7, Some("insert")), message(7, Some("delete"))];

        let Plan { runs, stopped, .. } = publisher.plan(&messages);

        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![
                Run {
                    op: Op::Upsert,
                    index: "movies".to_owned(),
                    segment: 0,
                    positions: vec![0]
                },
                Run {
                    op: Op::Delete,
                    index: "movies".to_owned(),
                    segment: 1,
                    positions: vec![1]
                }
            ]
        );
    }

    #[test]
    fn a_cdc_batch_of_one_operation_becomes_a_single_request() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages: Vec<_> = (1..=100).map(|id| message(id, Some("update"))).collect();
        let Plan { runs, .. } = publisher.plan(&messages);
        assert_eq!(runs.len(), 1);
    }

    #[test]
    fn truncate_stops_the_plan_rather_than_indexing_an_empty_row() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages = vec![message(1, Some("insert")), message(2, Some("truncate"))];

        let Plan { runs, stopped, .. } = publisher.plan(&messages);

        assert_eq!(runs.len(), 1);
        let (index, error) = stopped.expect("truncate should stop the plan");
        assert_eq!(index, 1);
        assert!(error.to_string().contains("truncate"));
    }

    #[test]
    fn a_delete_names_the_document_by_its_primary_key() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let plan = publisher.plan(&[message(7, Some("delete"))]);
        let ids = publisher.delete_ids(&[0], &plan.ids).unwrap();
        assert_eq!(ids, vec![serde_json::json!(7)]);

        let without_key = CanonicalMessage::from(r#"{"title":"x"}"#);
        let plan = publisher.plan(&[without_key]);
        assert_eq!(plan.rejected.len(), 1);
        assert!(publisher.delete_ids(&[0], &plan.ids).is_err());
    }

    #[test]
    fn the_id_is_read_without_parsing_the_rest_of_the_document() {
        let document = br#"{"title":"x","_vectors":{"default":[0.1,0.2]},"id":7,"tags":["a"]}"#;
        assert_eq!(
            top_level_field(document, "id").unwrap(),
            Some(serde_json::json!(7))
        );
        assert_eq!(top_level_field(document, "default").unwrap(), None);
        assert_eq!(
            top_level_field(br#"{"id":1,"id":2}"#, "id").unwrap(),
            Some(serde_json::json!(2))
        );
        for broken in [&b"[1]"[..], b"plain", br#"{"id":1"#, br#"{"id":1} x"#] {
            assert!(top_level_field(broken, "id").is_err());
        }
    }

    fn chunk_documents<'a>(
        messages: impl IntoIterator<Item = &'a CanonicalMessage>,
        max_bytes: usize,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut chunks = DocumentChunks::new(messages, max_bytes);
        let mut bodies = Vec::new();
        while let Some(body) = chunks.next_body()? {
            bodies.push(body);
        }
        Ok(bodies)
    }

    #[test]
    fn a_body_is_built_only_when_it_is_asked_for() {
        let messages: Vec<_> = (1..=3).map(|id| message(id, None)).collect();
        let mut read = 0;
        let mut chunks = DocumentChunks::new(messages.iter().inspect(|_| read += 1), 1);
        assert!(chunks.next_body().unwrap().is_some());
        drop(chunks);
        assert_eq!(read, 2, "only the first body and the line carried over");
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

        let Plan { runs, stopped, .. } = publisher.plan(&messages);

        assert!(stopped.is_none());
        assert_eq!(
            runs,
            vec![
                Run {
                    op: Op::Upsert,
                    index: "movies".to_owned(),
                    segment: 0,
                    positions: vec![0, 1]
                },
                Run {
                    op: Op::Upsert,
                    index: "directors".to_owned(),
                    segment: 0,
                    positions: vec![2]
                }
            ]
        );
    }

    /// Two indexes never share a document, so a message rejoins its index's
    /// run; within the run it stays behind the earlier ones from its table.
    #[test]
    fn switching_back_to_an_earlier_index_rejoins_its_run() {
        let publisher = routing_publisher(None, "${metadata:postgres.table}");
        let messages = vec![
            routed_message(1, "movies", None),
            routed_message(2, "directors", None),
            routed_message(3, "movies", None),
        ];

        let Plan { runs, .. } = publisher.plan(&messages);

        let targets: Vec<_> = runs.iter().map(|run| run.index.as_str()).collect();
        assert_eq!(targets, vec!["movies", "directors"]);
        assert_eq!(runs[0].positions, vec![0, 2]);
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

        let Plan { runs, .. } = publisher.plan(&messages);

        assert_eq!(runs.len(), 3);
        assert_eq!(runs[1].op, Op::Delete);
        assert_eq!(runs[1].index, "movies");
        assert_eq!(runs[2].index, "directors");
    }

    #[test]
    fn a_message_the_index_template_cannot_resolve_stops_the_plan() {
        let publisher = routing_publisher(None, "${metadata:postgres.table}");
        let messages = vec![routed_message(1, "movies", None), message(2, None)];

        let Plan { runs, stopped, .. } = publisher.plan(&messages);

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
        assert!(MeiliError::Transport(anyhow!("offline")).is_retryable());
        assert!(status(429).is_retryable());
        assert!(status(503).is_retryable());
        assert!(!status(401).is_retryable());
        assert!(!status(413).is_retryable());
        assert!(!MeiliError::Task {
            code: "missing_document_id".to_owned(),
            message: String::new(),
        }
        .is_retryable());
    }

    /// A stand-in Meilisearch whose one task stays `processing` for `slow`,
    /// counting the document requests it is sent.
    async fn slow_meilisearch(
        slow: std::time::Duration,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::Ordering;

        let writes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = writes.clone();
        let mut enqueued = None;
        let url = crate::client::stub::serve(move |request| {
            if request.starts_with("POST /indexes/movies/documents") {
                counted.fetch_add(1, Ordering::SeqCst);
                enqueued.get_or_insert_with(std::time::Instant::now);
                (202, r#"{"taskUid":7}"#.to_owned())
            } else if enqueued.is_some_and(|at| at.elapsed() >= slow) {
                (200, r#"{"status":"succeeded"}"#.to_owned())
            } else {
                (200, r#"{"status":"processing"}"#.to_owned())
            }
        })
        .await;
        (url, writes)
    }

    async fn stub_publisher(url: &str) -> Box<dyn MessagePublisher> {
        create(
            "movies",
            &serde_json::json!({
                "url": url,
                "index": "movies",
                "primary_key": "id",
                "create_index": false,
                "task_timeout_ms": 50,
            }),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_task_slower_than_the_timeout_is_awaited_rather_than_resent() {
        let slow = std::time::Duration::from_millis(400);
        let (url, writes) = slow_meilisearch(slow).await;
        let publisher = stub_publisher(&url).await;

        let started = std::time::Instant::now();
        let sent = publisher.send_batch(vec![message(1, None)]).await.unwrap();

        assert!(matches!(sent, SentBatch::Ack), "the batch must not fail");
        assert!(started.elapsed() >= slow);
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_task_meilisearch_no_longer_knows_is_sent_again() {
        let url = crate::client::stub::serve(|request| {
            if request.starts_with("POST") {
                (202, r#"{"taskUid":7}"#.to_owned())
            } else {
                (
                    404,
                    r#"{"message":"Task `7` not found.","code":"task_not_found"}"#.to_owned(),
                )
            }
        })
        .await;
        let publisher = stub_publisher(&url).await;

        let sent = publisher.send_batch(vec![message(1, None)]).await.unwrap();

        let SentBatch::Partial { failed, .. } = sent else {
            panic!("a lost task must hand its batch back");
        };
        assert!(matches!(failed[0].1, PublisherError::Retryable(_)));
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

    fn boom(retryable: bool) -> RunFailure {
        RunFailure {
            retryable,
            reason: "boom".to_owned(),
        }
    }

    /// Upserts and deletes of different documents commute, so a mixed CDC
    /// batch costs one request per operation instead of one per switch.
    #[test]
    fn interleaved_writes_to_distinct_documents_share_one_segment() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages: Vec<_> = (1..=100)
            .flat_map(|id| {
                [
                    message(id, Some("update")),
                    message(1000 + id, Some("delete")),
                ]
            })
            .collect();

        let Plan { runs, stopped, .. } = publisher.plan(&messages);

        assert!(stopped.is_none());
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().all(|run| run.segment == 0));
        assert_eq!(runs[0].positions, (0..200).step_by(2).collect::<Vec<_>>());
    }

    #[test]
    fn a_document_switching_operation_opens_a_new_segment() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let messages = vec![
            message(1, Some("insert")),
            message(2, Some("delete")),
            message(1, Some("delete")),
            message(3, Some("insert")),
        ];

        let Plan { runs, .. } = publisher.plan(&messages);

        let shape: Vec<_> = runs
            .iter()
            .map(|run| (run.segment, run.op, run.positions.clone()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (0, Op::Upsert, vec![0]),
                (0, Op::Delete, vec![1]),
                (1, Op::Delete, vec![2]),
                (1, Op::Upsert, vec![3]),
            ]
        );
    }

    /// Without `primary_key` the documents cannot be told apart, so every
    /// operation switch keeps its order.
    #[test]
    fn without_a_primary_key_every_operation_switch_opens_a_segment() {
        let mut publisher = publisher(Some("${metadata:postgres.operation}"));
        publisher.primary_key = None;
        let messages = vec![
            message(1, Some("insert")),
            message(2, Some("delete")),
            message(3, Some("insert")),
        ];

        let Plan { runs, .. } = publisher.plan(&messages);

        let segments: Vec<_> = runs.iter().map(|run| run.segment).collect();
        assert_eq!(segments, vec![0, 1, 2]);
    }

    /// A failed run fails the later segments and anything unconfirmed with
    /// it, while the other runs of its own segment, confirmed, are kept.
    #[test]
    fn a_failed_run_fails_its_own_messages_and_every_later_segment() {
        let run = |op, segment, positions: Vec<usize>| Run {
            op,
            index: "movies".to_owned(),
            segment,
            positions,
        };
        let runs = vec![
            run(Op::Upsert, 0, vec![0, 2]),
            run(Op::Delete, 0, vec![1]),
            run(Op::Delete, 1, vec![3]),
            run(Op::Upsert, 1, vec![4]),
        ];
        let outcomes = vec![Err(boom(false)), Ok(()), Ok(())];

        let failures = settle(&runs, &outcomes, Some((5, boom(true))), 6);

        let failed: Vec<_> = failures
            .iter()
            .map(|failure| failure.as_ref().map(|failure| failure.retryable))
            .collect();
        assert_eq!(
            failed,
            vec![
                Some(false),
                None,
                Some(false),
                Some(false),
                Some(false),
                Some(true)
            ]
        );
    }

    /// A document Meilisearch would refuse is set aside on its own instead of
    /// failing the task that carries its whole run.
    #[test]
    fn a_document_meilisearch_would_refuse_is_rejected_alone() {
        let publisher = publisher(Some("${metadata:postgres.operation}"));
        let mut invalid = CanonicalMessage::from(r#"{"id":"not valid!"}"#);
        invalid
            .metadata
            .insert("postgres.operation".to_owned(), "insert".to_owned());
        let messages = vec![
            message(1, Some("insert")),
            invalid,
            CanonicalMessage::from(r#"{"title":"no key"}"#),
            CanonicalMessage::from("[1]"),
            message(2, Some("insert")),
        ];

        let Plan {
            runs,
            rejected,
            stopped,
            ..
        } = publisher.plan(&messages);

        assert!(stopped.is_none());
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].positions, vec![0, 4]);
        let positions: Vec<_> = rejected.iter().map(|(position, _)| *position).collect();
        assert_eq!(positions, vec![1, 2, 3]);
    }

    #[test]
    fn document_ids_follow_meilisearchs_rule() {
        use serde_json::json;
        for valid in [
            json!(7),
            json!(-1),
            json!("abc-DEF_09"),
            json!("x".repeat(511)),
        ] {
            assert!(is_valid_document_id(&valid), "{valid} should be valid");
        }
        for invalid in [
            json!(""),
            json!("a b"),
            json!("a.b"),
            json!("x".repeat(512)),
            json!(1.5),
            json!(null),
            json!(true),
            json!([1]),
        ] {
            assert!(
                !is_valid_document_id(&invalid),
                "{invalid} should be invalid"
            );
        }
    }

    #[test]
    fn a_batch_without_failures_is_acknowledged_whole() {
        let messages: Vec<_> = (1..=3).map(|id| message(id, None)).collect();
        assert!(matches!(report(messages, vec![None; 3]), SentBatch::Ack));
    }

    #[test]
    fn failed_messages_are_reported_in_source_order() {
        let messages: Vec<_> = (1..=4).map(|id| message(id, None)).collect();
        let ids: Vec<_> = messages.iter().map(|message| message.message_id).collect();

        let SentBatch::Partial { failed, .. } = report(
            messages,
            vec![None, Some(boom(true)), None, Some(boom(false))],
        ) else {
            panic!("a failed message must report a partial batch");
        };

        let reported: Vec<_> = failed
            .iter()
            .map(|(message, _)| message.message_id)
            .collect();
        assert_eq!(reported, vec![ids[1], ids[3]]);
        assert!(matches!(failed[0].1, PublisherError::Retryable(_)));
        assert!(matches!(failed[1].1, PublisherError::NonRetryable(_)));
    }
}

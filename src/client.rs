use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use serde::Deserialize;

use crate::config::{Compression, MeilisearchConfig, WriteMethod};

/// The level mq-bridge's own HTTP endpoint compresses at.
const GZIP_LEVEL: u32 = 4;

fn gzip(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(data.len() / 4),
        flate2::Compression::new(GZIP_LEVEL),
    );
    encoder.write_all(data)?;
    encoder.finish()
}

/// Meilisearch's own name for "this index is already there", which
/// [`MeiliClient::create_index`] treats as success.
const INDEX_ALREADY_EXISTS: &str = "index_already_exists";

/// What a poll answers once the task it asks about no longer exists, as after
/// a restore from an older snapshot.
const TASK_NOT_FOUND: &str = "task_not_found";

/// How a Meilisearch call failed. Kept apart from `anyhow` so the publisher and
/// the consumer classify retryable and permanent failures from one place
/// instead of matching on message text.
#[derive(Debug)]
pub(crate) enum MeiliError {
    /// No usable response — DNS, connect, TLS, timeout, or a body that is not
    /// what Meilisearch sends. Worth retrying: every write this endpoint issues
    /// is keyed by primary key and so is idempotent.
    Transport(anyhow::Error),
    /// Meilisearch answered, with a status outside 2xx.
    Status {
        status: u16,
        code: String,
        message: String,
    },
    /// The request was accepted but the task it enqueued finished as `failed`.
    Task { code: String, message: String },
}

impl std::fmt::Display for MeiliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeiliError::Transport(error) => write!(f, "{error:#}"),
            MeiliError::Status {
                status,
                code,
                message,
            } => write!(f, "Meilisearch returned {status} ({code}): {message}"),
            MeiliError::Task { code, message } => {
                write!(f, "Meilisearch task failed ({code}): {message}")
            }
        }
    }
}

impl MeiliError {
    fn transport(error: impl Into<anyhow::Error>) -> Self {
        MeiliError::Transport(error.into())
    }

    /// The Meilisearch error code, for callers that treat one specially.
    pub(crate) fn code(&self) -> Option<&str> {
        match self {
            MeiliError::Transport(_) => None,
            MeiliError::Status { code, .. } | MeiliError::Task { code, .. } => Some(code),
        }
    }

    /// Which failures can heal on their own. A rejected key or a malformed
    /// document cannot, and retrying it would only delay the dead letter.
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            MeiliError::Transport(_) => true,
            MeiliError::Status { status, .. } => matches!(status, 429 | 500 | 502 | 503 | 504),
            MeiliError::Task { .. } => false,
        }
    }
}

/// The shape of every Meilisearch error body.
#[derive(Debug, Default, Deserialize)]
struct ApiError {
    #[serde(default)]
    message: String,
    #[serde(default)]
    code: String,
}

/// Every write answers 202 with the id of the task that will apply it.
#[derive(Debug, Deserialize)]
struct EnqueuedTask {
    #[serde(rename = "taskUid")]
    task_uid: u64,
}

#[derive(Debug, Deserialize)]
struct TaskState {
    #[serde(default)]
    uid: u64,
    status: String,
    #[serde(default)]
    error: Option<ApiError>,
}

impl TaskState {
    /// How the task ended, or `None` while it is still enqueued or processing.
    fn outcome(&self) -> Option<Result<(), MeiliError>> {
        match self.status.as_str() {
            "succeeded" => Some(Ok(())),
            "failed" | "canceled" => {
                let error = self.error.as_ref();
                Some(Err(MeiliError::Task {
                    code: error
                        .map(|error| error.code.clone())
                        .filter(|code| !code.is_empty())
                        .unwrap_or_else(|| self.status.clone()),
                    message: error.map(|error| error.message.clone()).unwrap_or_default(),
                }))
            }
            _ => None,
        }
    }
}

/// One page of `GET /indexes/{uid}/documents`.
#[derive(Debug, Deserialize)]
pub(crate) struct DocumentsPage {
    pub results: Vec<serde_json::Value>,
    #[serde(default)]
    pub total: u64,
}

pub(crate) struct MeiliClient {
    http: reqwest::Client,
    base: String,
    api_key: Option<String>,
    task_timeout: Duration,
    compression: Compression,
}

impl MeiliClient {
    pub(crate) fn new(config: &MeilisearchConfig) -> anyhow::Result<Self> {
        let base = config.url.trim().trim_end_matches('/').to_owned();
        if !base.starts_with("http://") && !base.starts_with("https://") {
            return Err(anyhow!(
                "Meilisearch `url` must be an absolute http(s) URL, e.g. 'http://localhost:7700'"
            ));
        }
        let mut builder = reqwest::Client::builder().connect_timeout(Duration::from_millis(
            config.connect_timeout_ms.unwrap_or(10_000),
        ));
        if let Some(ms) = config.request_timeout_ms {
            builder = builder.timeout(Duration::from_millis(ms));
        }
        Ok(Self {
            http: builder
                .build()
                .context("failed to build the Meilisearch HTTP client")?,
            base,
            api_key: config.api_key.clone().filter(|key| !key.trim().is_empty()),
            task_timeout: Duration::from_millis(config.task_timeout_ms),
            compression: config.compression,
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.http.request(method, format!("{}{path}", self.base));
        match &self.api_key {
            Some(key) => request.header("Authorization", format!("Bearer {key}")),
            None => request,
        }
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<String, MeiliError> {
        let response = request.send().await.map_err(MeiliError::transport)?;
        let status = response.status();
        let body = response.text().await.map_err(MeiliError::transport)?;
        if status.is_success() {
            return Ok(body);
        }
        let error: ApiError = serde_json::from_str(&body).unwrap_or_else(|_| ApiError {
            message: body.trim().to_owned(),
            code: String::new(),
        });
        Err(MeiliError::Status {
            status: status.as_u16(),
            code: error.code,
            message: error.message,
        })
    }

    async fn send_for_task(&self, request: reqwest::RequestBuilder) -> Result<u64, MeiliError> {
        let body = self.send(request).await?;
        let enqueued: EnqueuedTask = serde_json::from_str(&body).map_err(|error| {
            MeiliError::transport(anyhow!("unexpected Meilisearch response '{body}': {error}"))
        })?;
        Ok(enqueued.task_uid)
    }

    /// Creates the index so `primary_key` is applied before the first document
    /// arrives. An index that already exists is the normal case, not an error:
    /// Meilisearch reports it either as a status or as a failed task depending
    /// on the version, so both are accepted here.
    pub(crate) async fn create_index(
        &self,
        index: &str,
        primary_key: Option<&str>,
    ) -> Result<(), MeiliError> {
        let mut body = serde_json::json!({ "uid": index });
        if let Some(key) = primary_key {
            body["primaryKey"] = serde_json::Value::String(key.to_owned());
        }
        let request = self
            .request(reqwest::Method::POST, "/indexes")
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body).unwrap_or_default());
        let task = match self.send_for_task(request).await {
            Ok(task) => task,
            Err(error) if error.code() == Some(INDEX_ALREADY_EXISTS) => return Ok(()),
            Err(error) => return Err(error),
        };
        match self.wait_for_task(task).await {
            Err(error) if error.code() == Some(INDEX_ALREADY_EXISTS) => Ok(()),
            outcome => outcome,
        }
    }

    /// Forwards a settings document to `PATCH /indexes/{uid}/settings`, which
    /// merges only the keys it is given. The body is passed through verbatim,
    /// so a setting Meilisearch adds later needs no change here — and an
    /// unknown one is rejected by Meilisearch rather than silently dropped.
    pub(crate) async fn update_settings(
        &self,
        index: &str,
        settings: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), MeiliError> {
        let request = self
            .request(
                reqwest::Method::PATCH,
                &format!("/indexes/{index}/settings"),
            )
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(settings).unwrap_or_default());
        let task = self.send_for_task(request).await?;
        self.wait_for_task(task).await
    }

    /// Sends one NDJSON body of documents, returning the task it enqueued.
    pub(crate) async fn add_documents(
        &self,
        index: &str,
        primary_key: Option<&str>,
        method: WriteMethod,
        ndjson: Vec<u8>,
    ) -> Result<u64, MeiliError> {
        let request = self
            .documents_request(index, primary_key, method, ndjson)
            .await?;
        self.send_for_task(request).await
    }

    async fn documents_request(
        &self,
        index: &str,
        primary_key: Option<&str>,
        method: WriteMethod,
        ndjson: Vec<u8>,
    ) -> Result<reqwest::RequestBuilder, MeiliError> {
        let mut request = self
            .request(method.http_method(), &format!("/indexes/{index}/documents"))
            .header("Content-Type", "application/x-ndjson");
        if let Some(key) = primary_key {
            request = request.query(&[("primaryKey", key)]);
        }
        Ok(match self.compression {
            Compression::None => request.body(ndjson),
            Compression::Gzip => {
                // A body can be tens of MB, too long to hold an async worker.
                let body = tokio::task::spawn_blocking(move || gzip(&ndjson))
                    .await
                    .map_err(MeiliError::transport)?
                    .map_err(MeiliError::transport)?;
                request.header("Content-Encoding", "gzip").body(body)
            }
        })
    }

    /// Exchanges two indexes atomically and waits for the swap to be applied.
    pub(crate) async fn swap_indexes(&self, first: &str, second: &str) -> Result<(), MeiliError> {
        let body = serde_json::json!([{ "indexes": [first, second] }]);
        let request = self
            .request(reqwest::Method::POST, "/swap-indexes")
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body).unwrap_or_default());
        let task = self.send_for_task(request).await?;
        self.wait_for_task(task).await
    }

    pub(crate) async fn delete_index(&self, index: &str) -> Result<(), MeiliError> {
        let request = self.request(reqwest::Method::DELETE, &format!("/indexes/{index}"));
        let task = self.send_for_task(request).await?;
        self.wait_for_task(task).await
    }

    pub(crate) async fn delete_documents(
        &self,
        index: &str,
        ids: &[serde_json::Value],
    ) -> Result<u64, MeiliError> {
        let request = self
            .request(
                reqwest::Method::POST,
                &format!("/indexes/{index}/documents/delete-batch"),
            )
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(ids).unwrap_or_default());
        self.send_for_task(request).await
    }

    /// Polls a task to a finished state. Meilisearch answers a write with 202
    /// and applies it afterwards, so a task that is still `enqueued` says
    /// nothing about whether the documents were accepted.
    ///
    /// It never gives up on a task that is still alive: Meilisearch does not
    /// lose an enqueued task, so reporting a slow one as failed would have the
    /// route resend documents that are about to be indexed anyway. Each
    /// `task_timeout_ms` that passes is logged instead, and a poll that fails
    /// transiently is repeated rather than failing the write it asks about.
    pub(crate) async fn wait_for_task(&self, task: u64) -> Result<(), MeiliError> {
        const FIRST_POLL: Duration = Duration::from_millis(10);
        const MAX_POLL: Duration = Duration::from_millis(50);
        const MAX_SLOW_POLL: Duration = Duration::from_millis(250);
        const MAX_FAILED_POLL: Duration = Duration::from_secs(1);

        let started = Instant::now();
        let mut warn_at = started + self.task_timeout;
        let mut delay = FIRST_POLL;
        loop {
            let (waiting_on, limit) = match self.task_state(task).await {
                Ok(state) => match state.outcome() {
                    Some(outcome) => return outcome,
                    // A task that has already run for seconds is polled less often.
                    None => (
                        format!("still '{}'", state.status),
                        (started.elapsed() / 20).clamp(MAX_POLL, MAX_SLOW_POLL),
                    ),
                },
                // The task is gone, and with it the write: only a resend restores it.
                Err(error) if error.code() == Some(TASK_NOT_FOUND) => {
                    return Err(MeiliError::transport(anyhow!(
                        "Meilisearch no longer knows task {task}, so its write is lost and has to be sent again: {error}"
                    )));
                }
                Err(error) if error.is_retryable() => {
                    (format!("cannot be polled: {error}"), MAX_FAILED_POLL)
                }
                Err(error) => return Err(error),
            };
            if Instant::now() >= warn_at {
                tracing::warn!(
                    task,
                    waited = ?started.elapsed(),
                    "Meilisearch task {waiting_on}; still waiting, since resending its documents would index them twice"
                );
                warn_at += self.task_timeout;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(limit);
        }
    }

    async fn task_state(&self, task: u64) -> Result<TaskState, MeiliError> {
        let body = self
            .send(self.request(reqwest::Method::GET, &format!("/tasks/{task}")))
            .await?;
        serde_json::from_str(&body).map_err(|error| {
            MeiliError::transport(anyhow!("unexpected Meilisearch task '{body}': {error}"))
        })
    }

    /// Waits for the tasks of one run, which target one index. Meilisearch
    /// finishes an index's tasks in uid order, so the last one is waited for
    /// and the others are then read in a single request, instead of polling
    /// each in turn. Reports the first failure in uid order.
    pub(crate) async fn wait_for_tasks(&self, tasks: &[u64]) -> Result<(), MeiliError> {
        let Some((last, earlier)) = tasks.split_last() else {
            return Ok(());
        };
        let last = self.wait_for_task(*last).await;
        if earlier.is_empty() {
            return last;
        }
        let states = self.task_states(earlier).await;
        for task in earlier {
            let known = states
                .as_ref()
                .ok()
                .and_then(|states| states.iter().find(|state| state.uid == *task))
                .and_then(TaskState::outcome);
            match known {
                Some(outcome) => outcome?,
                None => self.wait_for_task(*task).await?,
            }
        }
        last
    }

    async fn task_states(&self, tasks: &[u64]) -> Result<Vec<TaskState>, MeiliError> {
        #[derive(Deserialize)]
        struct Tasks {
            results: Vec<TaskState>,
        }
        let uids = tasks
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let body = self
            .send(
                self.request(reqwest::Method::GET, "/tasks")
                    .query(&[("uids", uids), ("limit", tasks.len().to_string())]),
            )
            .await?;
        let tasks: Tasks = serde_json::from_str(&body).map_err(|error| {
            MeiliError::transport(anyhow!(
                "unexpected Meilisearch task list '{body}': {error}"
            ))
        })?;
        Ok(tasks.results)
    }

    /// Reads one page of an index's documents. `/documents` rather than
    /// `/search`: search pagination stops at `maxTotalHits` (1000 by default),
    /// so it cannot export a whole index, while this endpoint is uncapped.
    pub(crate) async fn get_documents(
        &self,
        index: &str,
        offset: u64,
        limit: usize,
        fields: Option<&str>,
    ) -> Result<DocumentsPage, MeiliError> {
        let mut query = vec![
            ("offset".to_owned(), offset.to_string()),
            ("limit".to_owned(), limit.to_string()),
        ];
        if let Some(fields) = fields {
            query.push(("fields".to_owned(), fields.to_owned()));
        }
        let body = self
            .send(
                self.request(reqwest::Method::GET, &format!("/indexes/{index}/documents"))
                    .query(&query),
            )
            .await?;
        serde_json::from_str(&body).map_err(|error| {
            MeiliError::transport(anyhow!(
                "unexpected Meilisearch documents page '{body}': {error}"
            ))
        })
    }

    /// The newest task on `index` that has not finished yet. Meilisearch applies
    /// an index's tasks in uid order, so once it is done every earlier write is.
    pub(crate) async fn latest_pending_task(&self, index: &str) -> Result<Option<u64>, MeiliError> {
        #[derive(Deserialize)]
        struct Task {
            uid: u64,
        }
        #[derive(Deserialize)]
        struct Tasks {
            results: Vec<Task>,
        }
        let body = self
            .send(self.request(reqwest::Method::GET, "/tasks").query(&[
                ("indexUids", index),
                ("statuses", "enqueued,processing"),
                ("limit", "1"),
            ]))
            .await?;
        let tasks: Tasks = serde_json::from_str(&body).map_err(|error| {
            MeiliError::transport(anyhow!(
                "unexpected Meilisearch task list '{body}': {error}"
            ))
        })?;
        Ok(tasks.results.first().map(|task| task.uid))
    }

    /// Which of `ids` name a document in `index`, read through
    /// `POST /documents/fetch` (Meilisearch 1.14+). Unknown ids are skipped.
    pub(crate) async fn existing_ids(
        &self,
        index: &str,
        primary_key: &str,
        ids: &[String],
    ) -> Result<Vec<serde_json::Value>, MeiliError> {
        self.fetch_ids(
            index,
            primary_key,
            serde_json::json!({
                "ids": ids,
                "fields": [primary_key],
                "limit": ids.len(),
            }),
        )
        .await
    }

    async fn fetch_ids(
        &self,
        index: &str,
        primary_key: &str,
        body: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, MeiliError> {
        let request = self
            .request(
                reqwest::Method::POST,
                &format!("/indexes/{index}/documents/fetch"),
            )
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body).unwrap_or_default());
        let body = self.send(request).await?;
        let page: DocumentsPage = serde_json::from_str(&body).map_err(|error| {
            MeiliError::transport(anyhow!(
                "unexpected Meilisearch documents page '{body}': {error}"
            ))
        })?;
        Ok(page
            .results
            .into_iter()
            .filter_map(|mut document| document.get_mut(primary_key).map(serde_json::Value::take))
            .collect())
    }

    /// One page of the ids of the documents `filter` matches, in the order
    /// `POST /documents/fetch` returns them.
    pub(crate) async fn matching_ids(
        &self,
        index: &str,
        primary_key: &str,
        filter: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, MeiliError> {
        self.fetch_ids(
            index,
            primary_key,
            serde_json::json!({
                "filter": filter,
                "fields": [primary_key],
                "offset": offset,
                "limit": limit,
            }),
        )
        .await
    }

    pub(crate) async fn health(&self) -> Result<(), MeiliError> {
        self.send(self.request(reqwest::Method::GET, "/health"))
            .await
            .map(|_| ())
    }
}

/// A scripted stand-in for Meilisearch: answers each request with whatever
/// `respond` returns for its request line (`GET /tasks/7`).
#[cfg(test)]
pub(crate) mod stub {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub(crate) async fn serve(
        mut respond: impl FnMut(&str) -> (u16, String) + Send + 'static,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                let head = loop {
                    let read = socket.read(&mut buffer).await.unwrap();
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break String::from_utf8_lossy(&request[..end]).into_owned();
                    }
                };
                let length = head
                    .lines()
                    .find_map(|line| {
                        line.to_lowercase()
                            .strip_prefix("content-length:")?
                            .trim()
                            .parse()
                            .ok()
                    })
                    .unwrap_or(0usize);
                while request.len() < head.len() + 4 + length {
                    let read = socket.read(&mut buffer).await.unwrap();
                    request.extend_from_slice(&buffer[..read]);
                }
                let (status, body) = respond(head.lines().next().unwrap_or_default());
                let response = format!(
                    "HTTP/1.1 {status} Stub\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> MeilisearchConfig {
        serde_json::from_value(serde_json::json!({ "url": url })).unwrap()
    }

    #[test]
    fn a_url_without_a_scheme_is_rejected() {
        assert!(MeiliClient::new(&config("localhost:7700")).is_err());
        assert!(MeiliClient::new(&config("http://localhost:7700")).is_ok());
        assert!(MeiliClient::new(&config("https://search.example.com")).is_ok());
    }

    #[test]
    fn a_trailing_slash_does_not_double_up_in_paths() {
        let client = MeiliClient::new(&config("http://localhost:7700/")).unwrap();
        assert_eq!(client.base, "http://localhost:7700");
    }

    #[test]
    fn an_empty_api_key_is_treated_as_absent() {
        let mut settings = config("http://localhost:7700");
        settings.api_key = Some("  ".to_owned());
        assert!(MeiliClient::new(&settings).unwrap().api_key.is_none());

        settings.api_key = Some("secret".to_owned());
        assert_eq!(
            MeiliClient::new(&settings).unwrap().api_key.as_deref(),
            Some("secret")
        );
    }

    const NDJSON: &[u8] = b"{\"id\":1,\"title\":\"one\"}\n{\"id\":2,\"title\":\"two\"}\n";

    async fn documents_request(compression: &str) -> reqwest::Request {
        let mut settings = config("http://localhost:7700");
        settings.compression = serde_json::from_value(serde_json::json!(compression)).unwrap();
        MeiliClient::new(&settings)
            .unwrap()
            .documents_request("movies", Some("id"), WriteMethod::Replace, NDJSON.to_vec())
            .await
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn without_compression_the_ndjson_is_sent_as_it_is() {
        let request = documents_request("none").await;
        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://localhost:7700/indexes/movies/documents?primaryKey=id"
        );
        let headers: Vec<_> = request.headers().iter().collect();
        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(request.headers()["content-type"], "application/x-ndjson");
        assert_eq!(request.body().unwrap().as_bytes(), Some(NDJSON));
    }

    #[tokio::test]
    async fn gzip_sends_the_same_ndjson_compressed_and_says_so() {
        let request = documents_request("gzip").await;
        assert_eq!(request.headers().len(), 2);
        assert_eq!(request.headers()["content-type"], "application/x-ndjson");
        assert_eq!(request.headers()["content-encoding"], "gzip");

        let plain = documents_request("none").await;
        assert_eq!(request.url(), plain.url());
        let sent = request.body().unwrap().as_bytes().unwrap();
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(sent), &mut decoded).unwrap();
        assert_eq!(Some(decoded.as_slice()), plain.body().unwrap().as_bytes());
    }

    #[tokio::test]
    async fn the_tasks_of_a_run_are_read_together_once_the_last_is_done() {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = requests.clone();
        let url = stub::serve(move |request| {
            seen.lock().unwrap().push(request.to_owned());
            if request.starts_with("GET /tasks?") {
                let failed = r#"{"uid":2,"status":"failed","error":{"code":"invalid_document_geo_field","message":"bad"}}"#;
                (
                    200,
                    format!(r#"{{"results":[{failed},{{"uid":1,"status":"succeeded"}}]}}"#),
                )
            } else {
                (200, r#"{"uid":3,"status":"succeeded"}"#.to_owned())
            }
        })
        .await;
        let client = MeiliClient::new(&config(&url)).unwrap();

        let outcome = client.wait_for_tasks(&[1, 2, 3]).await;

        assert_eq!(
            outcome.unwrap_err().code(),
            Some("invalid_document_geo_field")
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2, "{requests:?}");
        assert!(requests[0].starts_with("GET /tasks/3 "), "{requests:?}");
        assert!(requests[1].contains("uids=1%2C2"), "{requests:?}");
    }

    #[test]
    fn errors_carry_the_meilisearch_code_they_name() {
        let status = MeiliError::Status {
            status: 413,
            code: "payload_too_large".to_owned(),
            message: "too big".to_owned(),
        };
        assert_eq!(status.code(), Some("payload_too_large"));
        assert!(status.to_string().contains("413"));
        assert!(MeiliError::Transport(anyhow!("offline")).code().is_none());
    }
}

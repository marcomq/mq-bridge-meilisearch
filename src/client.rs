use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use serde::Deserialize;

use crate::config::{MeilisearchConfig, WriteMethod};

/// Meilisearch's own name for "this index is already there", which
/// [`MeiliClient::create_index`] treats as success.
const INDEX_ALREADY_EXISTS: &str = "index_already_exists";

/// How a Meilisearch call failed. Kept apart from `anyhow` so the publisher and
/// the consumer classify retryable and permanent failures from one place
/// instead of matching on message text.
#[derive(Debug)]
pub(crate) enum MeiliError {
    /// No response at all — DNS, connect, TLS, timeout — or a wait for an
    /// enqueued task that gave up. Both are worth retrying: every write this
    /// endpoint issues is keyed by primary key and so is idempotent.
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
    status: String,
    #[serde(default)]
    error: Option<ApiError>,
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
        let mut request = self
            .request(method.http_method(), &format!("/indexes/{index}/documents"))
            .header("Content-Type", "application/x-ndjson");
        if let Some(key) = primary_key {
            request = request.query(&[("primaryKey", key)]);
        }
        self.send_for_task(request.body(ndjson)).await
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
    pub(crate) async fn wait_for_task(&self, task: u64) -> Result<(), MeiliError> {
        const FIRST_POLL: Duration = Duration::from_millis(10);
        const MAX_POLL: Duration = Duration::from_millis(50);

        let deadline = Instant::now() + self.task_timeout;
        let mut delay = FIRST_POLL;
        loop {
            let body = self
                .send(self.request(reqwest::Method::GET, &format!("/tasks/{task}")))
                .await?;
            let state: TaskState = serde_json::from_str(&body).map_err(|error| {
                MeiliError::transport(anyhow!("unexpected Meilisearch task '{body}': {error}"))
            })?;
            match state.status.as_str() {
                "succeeded" => return Ok(()),
                "failed" | "canceled" => {
                    let error = state.error.unwrap_or_default();
                    return Err(MeiliError::Task {
                        code: if error.code.is_empty() {
                            state.status
                        } else {
                            error.code
                        },
                        message: error.message,
                    });
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(MeiliError::transport(anyhow!(
                    "Meilisearch task {task} was still '{}' after {:?}; raise `task_timeout_ms` if indexing is simply slow",
                    state.status,
                    self.task_timeout
                )));
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_POLL);
        }
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

    pub(crate) async fn health(&self) -> Result<(), MeiliError> {
        self.send(self.request(reqwest::Method::GET, "/health"))
            .await
            .map(|_| ())
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

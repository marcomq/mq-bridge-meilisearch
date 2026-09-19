use anyhow::{anyhow, Context};
use mq_bridge::errors::{ConsumerError, PublisherError};
use serde::Deserialize;

/// How an upsert reaches Meilisearch.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WriteMethod {
    /// `POST /documents`: a document with the same primary key is replaced
    /// whole, so fields absent from the payload are dropped.
    #[default]
    Replace,
    /// `PUT /documents`: top-level fields are merged into the existing
    /// document, leaving every field the payload omits untouched. This is what
    /// lets several source tables write into one shared document.
    Update,
}

impl WriteMethod {
    pub(crate) fn http_method(self) -> reqwest::Method {
        match self {
            WriteMethod::Replace => reqwest::Method::POST,
            WriteMethod::Update => reqwest::Method::PUT,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_delete_values() -> Vec<String> {
    vec!["delete".to_owned()]
}

fn default_task_timeout_ms() -> u64 {
    60_000
}

fn default_polling_interval_ms() -> u64 {
    1_000
}

/// Configuration accepted by an endpoint named `meilisearch`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MeilisearchConfig {
    /// Base URL of the Meilisearch instance, e.g. `http://localhost:7700`.
    pub url: String,
    /// A master key or an API key with access to the index. Omit for an
    /// instance started without a master key.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Index UID; defaults to the route name.
    #[serde(default)]
    pub index: Option<String>,
    /// The document field Meilisearch keys documents by. Strongly recommended:
    /// left unset, Meilisearch infers it from the first batch by picking a
    /// field whose name contains `id`, which can silently pick the wrong one.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Output only: whether an upsert replaces or merges. See [`WriteMethod`].
    #[serde(default)]
    pub method: WriteMethod,
    /// Output only: a template resolving to the change operation of a message,
    /// typically `${metadata:postgres.operation}`. Unset means every message is
    /// an upsert, which is all a plain table copy needs.
    #[serde(default)]
    pub operation: Option<String>,
    /// Output only: operation values that mean "remove this document".
    #[serde(default = "default_delete_values")]
    pub delete_values: Vec<String>,
    /// Create the index at startup if it does not exist, so `primary_key` is
    /// applied before the first document rather than inferred from it.
    #[serde(default = "default_true")]
    pub create_index: bool,
    /// Output only: wait for the asynchronous task each write enqueues to
    /// finish before acknowledging. Turning this off acknowledges on enqueue,
    /// which commits the source cursor for writes that may still fail.
    #[serde(default = "default_true")]
    pub wait_for_task: bool,
    /// How long to wait for one enqueued task, in milliseconds.
    #[serde(default = "default_task_timeout_ms")]
    pub task_timeout_ms: u64,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    /// Input only: comma-separated document fields to return; all by default.
    #[serde(default)]
    pub fields: Option<String>,
    /// Input only: names this reader's position in `checkpoint_store`. Without
    /// it the scan restarts from the first document on every route start.
    #[serde(default)]
    pub cursor_id: Option<String>,
    /// Input only: where the resume position is persisted, as a
    /// `file://`/`postgres://`/`mongodb://`/`s3://` spec.
    #[serde(default)]
    pub checkpoint_store: Option<String>,
    /// Input only: delay between polls once the scan has reached the end.
    #[serde(default = "default_polling_interval_ms")]
    pub polling_interval_ms: u64,
    /// Input only: upper bound the idle delay backs off to; no backoff by
    /// default, so the reader keeps polling at `polling_interval_ms`.
    #[serde(default)]
    pub max_polling_interval_ms: Option<u64>,
}

/// A rejected configuration cannot heal by reconnecting, so both constructors
/// below hand the route an error classified as permanent. An unclassified
/// `anyhow::Error` reaches the route as a connection failure, which it retries
/// on its reconnect interval forever.
pub(crate) fn resolve_for_consumer(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(MeilisearchConfig, String)> {
    resolve(route_name, value).map_err(|error| anyhow::Error::new(ConsumerError::Permanent(error)))
}

pub(crate) fn resolve_for_publisher(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(MeilisearchConfig, String)> {
    resolve(route_name, value)
        .map_err(|error| anyhow::Error::new(PublisherError::NonRetryable(error)))
}

fn resolve(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(MeilisearchConfig, String)> {
    let config: MeilisearchConfig = serde_json::from_value(value.clone())
        .context("invalid Meilisearch endpoint configuration")?;
    if config.url.trim().is_empty() {
        return Err(anyhow!("Meilisearch `url` must not be empty"));
    }
    let index = config
        .index
        .clone()
        .unwrap_or_else(|| route_name.to_owned());
    if index.trim().is_empty() {
        return Err(anyhow!("Meilisearch `index` must not be empty"));
    }
    // A delete names the document to remove by its primary key, read out of the
    // payload, so mapping `operation` without one cannot express a delete.
    if config.operation.is_some() && config.primary_key.is_none() {
        return Err(anyhow!(
            "Meilisearch `primary_key` is required when `operation` is set, so a delete can name the document to remove"
        ));
    }
    if config.task_timeout_ms == 0 {
        return Err(anyhow!(
            "Meilisearch `task_timeout_ms` must be greater than 0"
        ));
    }
    Ok((config, index))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(extra: serde_json::Value) -> serde_json::Value {
        let mut base = serde_json::json!({"url": "http://localhost:7700"});
        let object = base.as_object_mut().unwrap();
        for (key, item) in extra.as_object().unwrap() {
            object.insert(key.clone(), item.clone());
        }
        base
    }

    #[test]
    fn index_defaults_to_the_route_name() {
        let (config, index) = resolve("movies", &value(serde_json::json!({}))).unwrap();
        assert_eq!(index, "movies");
        assert_eq!(config.method, WriteMethod::Replace);
        assert_eq!(config.delete_values, vec!["delete".to_owned()]);
        assert!(config.create_index);
        assert!(config.wait_for_task);
    }

    #[test]
    fn an_explicit_index_wins() {
        let (_, index) = resolve("route", &value(serde_json::json!({"index": "books"}))).unwrap();
        assert_eq!(index, "books");
    }

    #[test]
    fn update_selects_meilisearchs_partial_merge() {
        let (config, _) =
            resolve("route", &value(serde_json::json!({"method": "update"}))).unwrap();
        assert_eq!(config.method, WriteMethod::Update);
        assert_eq!(config.method.http_method(), reqwest::Method::PUT);
        assert_eq!(WriteMethod::Replace.http_method(), reqwest::Method::POST);
        assert!(resolve("route", &value(serde_json::json!({"method": "merge"}))).is_err());
    }

    /// Without a primary key a delete has no document id to send, and the route
    /// would drop rows silently rather than fail.
    #[test]
    fn mapping_an_operation_requires_a_primary_key() {
        let mapped = serde_json::json!({"operation": "${metadata:postgres.operation}"});
        assert!(resolve("route", &value(mapped.clone())).is_err());

        let mut with_key = mapped;
        with_key["primary_key"] = serde_json::json!("id");
        assert!(resolve("route", &value(with_key)).is_ok());
    }

    #[test]
    fn invalid_configuration_is_rejected_before_connecting() {
        assert!(resolve("route", &serde_json::json!({})).is_err());
        assert!(resolve("route", &serde_json::json!({"url": ""})).is_err());
        assert!(resolve("route", &value(serde_json::json!({"extra": true}))).is_err());
        assert!(resolve("route", &value(serde_json::json!({"task_timeout_ms": 0}))).is_err());
    }

    #[test]
    fn a_rejected_configuration_is_permanent_so_the_route_stops_reconnecting() {
        let rejected = value(serde_json::json!({"extra": true}));

        let consumer_error = resolve_for_consumer("route", &rejected).unwrap_err();
        assert!(matches!(
            consumer_error.downcast_ref::<ConsumerError>(),
            Some(ConsumerError::Permanent(_))
        ));

        let publisher_error = resolve_for_publisher("route", &rejected).unwrap_err();
        assert!(matches!(
            publisher_error.downcast_ref::<PublisherError>(),
            Some(PublisherError::NonRetryable(_))
        ));
    }
}

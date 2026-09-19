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

/// Every value in a CLI endpoint URI arrives as a string — a query string
/// carries no types — so the scalar options accept both their real JSON form
/// and its spelling. A YAML route is unaffected; it already has types.
mod flexible {
    use serde::{de::Error, Deserialize, Deserializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrText {
        Bool(bool),
        Text(String),
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum IntOrText {
        Int(u64),
        Text(String),
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ListOrText {
        List(Vec<String>),
        Text(String),
    }

    fn parse_bool<E: Error>(text: &str) -> Result<bool, E> {
        match text.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(E::custom(format!("expected true or false, found '{text}'"))),
        }
    }

    fn parse_int<E: Error>(text: &str) -> Result<u64, E> {
        text.trim()
            .parse()
            .map_err(|_| E::custom(format!("expected a whole number, found '{text}'")))
    }

    pub(super) fn boolean<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
        match BoolOrText::deserialize(deserializer)? {
            BoolOrText::Bool(value) => Ok(value),
            BoolOrText::Text(text) => parse_bool(&text),
        }
    }

    pub(super) fn integer<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        match IntOrText::deserialize(deserializer)? {
            IntOrText::Int(value) => Ok(value),
            IntOrText::Text(text) => parse_int(&text),
        }
    }

    pub(super) fn optional_integer<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        match Option::<IntOrText>::deserialize(deserializer)? {
            None => Ok(None),
            Some(IntOrText::Int(value)) => Ok(Some(value)),
            Some(IntOrText::Text(text)) => parse_int(&text).map(Some),
        }
    }

    /// A list, or the comma-separated spelling a URI can carry.
    pub(super) fn string_list<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<String>, D::Error> {
        Ok(match ListOrText::deserialize(deserializer)? {
            ListOrText::List(values) => values,
            ListOrText::Text(text) => text
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect(),
        })
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

/// Meilisearch refuses a request body over `MEILI_HTTP_PAYLOAD_SIZE_LIMIT`,
/// 100 MB out of the box. Splitting below that keeps the headroom a proxy or a
/// hosted instance with a lower cap usually needs.
fn default_max_request_bytes() -> u64 {
    90_000_000
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
    /// Index UID; defaults to the route name. Output only, this may be a
    /// template such as `${metadata:postgres.table}`, which routes each message
    /// to the index its own metadata names.
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
    /// Output only: index settings forwarded verbatim to
    /// `PATCH /indexes/{uid}/settings` when the index is created, e.g.
    /// `searchableAttributes` or `filterableAttributes`. The call merges only
    /// the keys it is given, so settings this endpoint does not set are left
    /// as they are.
    #[serde(default)]
    pub settings: Option<serde_json::Map<String, serde_json::Value>>,
    /// Output only: a template resolving to the change operation of a message,
    /// typically `${metadata:postgres.operation}`. Unset means every message is
    /// an upsert, which is all a plain table copy needs.
    #[serde(default)]
    pub operation: Option<String>,
    /// Output only: operation values that mean "remove this document".
    #[serde(
        default = "default_delete_values",
        deserialize_with = "flexible::string_list"
    )]
    pub delete_values: Vec<String>,
    /// Create the index at startup if it does not exist, so `primary_key` is
    /// applied before the first document rather than inferred from it.
    #[serde(default = "default_true", deserialize_with = "flexible::boolean")]
    pub create_index: bool,
    /// Output only: wait for the asynchronous task each write enqueues to
    /// finish before acknowledging. Turning this off acknowledges on enqueue,
    /// which commits the source cursor for writes that may still fail.
    #[serde(default = "default_true", deserialize_with = "flexible::boolean")]
    pub wait_for_task: bool,
    /// How long to wait for one enqueued task, in milliseconds.
    #[serde(
        default = "default_task_timeout_ms",
        deserialize_with = "flexible::integer"
    )]
    pub task_timeout_ms: u64,
    /// Output only: the largest document request this endpoint will send. A run
    /// bigger than this is split into several requests, still in order, rather
    /// than rejected whole as `payload_too_large`.
    #[serde(
        default = "default_max_request_bytes",
        deserialize_with = "flexible::integer"
    )]
    pub max_request_bytes: u64,
    #[serde(default, deserialize_with = "flexible::optional_integer")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, deserialize_with = "flexible::optional_integer")]
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
    #[serde(
        default = "default_polling_interval_ms",
        deserialize_with = "flexible::integer"
    )]
    pub polling_interval_ms: u64,
    /// Input only: upper bound the idle delay backs off to; no backoff by
    /// default, so the reader keeps polling at `polling_interval_ms`.
    #[serde(default, deserialize_with = "flexible::optional_integer")]
    pub max_polling_interval_ms: Option<u64>,
}

/// Rewrites a `meilisearch://` URL to the HTTP one Meilisearch actually speaks.
///
/// The CLI builds a plugin endpoint's `url` from the URI up to the query, so a
/// route written as `meilisearch://host:7700` arrives with that scheme rather
/// than an HTTP one. Anything else is passed through untouched, so an explicit
/// `http(s)://` URL — or a `?url=` override — still wins.
fn normalize_url(url: &str) -> String {
    let url = url.trim();
    for (scheme, http) in [
        ("meilisearchs://", "https://"),
        ("meilisearch://", "http://"),
    ] {
        if let Some(rest) = url.strip_prefix(scheme) {
            return format!("{http}{rest}");
        }
    }
    url.to_owned()
}

/// Whether a value carries a `${...}` token, which makes it per-message. Only
/// the publisher can honour that; a reader pages through one concrete index.
pub(crate) fn is_template(value: &str) -> bool {
    value.contains("${")
}

/// A rejected configuration cannot heal by reconnecting, so both constructors
/// below hand the route an error classified as permanent. An unclassified
/// `anyhow::Error` reaches the route as a connection failure, which it retries
/// on its reconnect interval forever.
pub(crate) fn resolve_for_consumer(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(MeilisearchConfig, String)> {
    resolve(route_name, value)
        .and_then(|(config, index)| {
            if is_template(&index) {
                Err(anyhow!(
                    "Meilisearch `index` cannot be a template when reading: a reader pages through one index, so '{index}' has nothing to resolve against"
                ))
            } else {
                Ok((config, index))
            }
        })
        .map_err(|error| anyhow::Error::new(ConsumerError::Permanent(error)))
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
    let mut config: MeilisearchConfig = serde_json::from_value(value.clone())
        .context("invalid Meilisearch endpoint configuration")?;
    config.url = normalize_url(&config.url);
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
    if config.max_request_bytes == 0 {
        return Err(anyhow!(
            "Meilisearch `max_request_bytes` must be greater than 0"
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

    /// `mqb copy … meilisearch://host:7700?index=movies` derives `url` from the
    /// URI up to the query, so the endpoint has to accept its own scheme.
    #[test]
    fn a_meilisearch_scheme_url_becomes_the_http_one() {
        let (config, _) = resolve(
            "route",
            &serde_json::json!({"url": "meilisearch://localhost:7700"}),
        )
        .unwrap();
        assert_eq!(config.url, "http://localhost:7700");

        let (secure, _) = resolve(
            "route",
            &serde_json::json!({"url": "meilisearchs://search.example.com"}),
        )
        .unwrap();
        assert_eq!(secure.url, "https://search.example.com");
    }

    #[test]
    fn an_http_url_is_left_alone() {
        let (config, _) = resolve("route", &value(serde_json::json!({}))).unwrap();
        assert_eq!(config.url, "http://localhost:7700");
    }

    /// A URI query carries no types, so every option arrives as a string.
    #[test]
    fn scalar_options_accept_the_string_spelling_a_uri_carries() {
        let (config, _) = resolve(
            "route",
            &value(serde_json::json!({
                "create_index": "false",
                "wait_for_task": "TRUE",
                "task_timeout_ms": "30000",
                "connect_timeout_ms": "250",
                "delete_values": "delete, remove",
            })),
        )
        .unwrap();

        assert!(!config.create_index);
        assert!(config.wait_for_task);
        assert_eq!(config.task_timeout_ms, 30_000);
        assert_eq!(config.connect_timeout_ms, Some(250));
        assert_eq!(
            config.delete_values,
            vec!["delete".to_owned(), "remove".to_owned()]
        );
    }

    #[test]
    fn the_typed_forms_still_work_and_nonsense_is_still_rejected() {
        let (config, _) = resolve(
            "route",
            &value(serde_json::json!({
                "create_index": false,
                "task_timeout_ms": 30000,
                "delete_values": ["delete"],
            })),
        )
        .unwrap();
        assert!(!config.create_index);
        assert_eq!(config.task_timeout_ms, 30_000);
        assert_eq!(config.delete_values, vec!["delete".to_owned()]);

        assert!(resolve("route", &value(serde_json::json!({"create_index": "yes"}))).is_err());
        assert!(resolve(
            "route",
            &value(serde_json::json!({"task_timeout_ms": "soon"}))
        )
        .is_err());
    }

    #[test]
    fn settings_are_carried_through_untouched() {
        let (config, _) = resolve(
            "route",
            &value(serde_json::json!({
                "settings": {
                    "searchableAttributes": ["title", "overview"],
                    "filterableAttributes": ["genre"],
                    "pagination": {"maxTotalHits": 10000},
                }
            })),
        )
        .unwrap();

        let settings = config.settings.expect("settings should be kept");
        assert_eq!(settings.len(), 3);
        assert_eq!(
            settings["searchableAttributes"],
            serde_json::json!(["title", "overview"])
        );
        assert_eq!(settings["pagination"]["maxTotalHits"], 10_000);
    }

    #[test]
    fn a_request_size_limit_defaults_below_meilisearchs_own() {
        let (config, _) = resolve("route", &value(serde_json::json!({}))).unwrap();
        assert_eq!(config.max_request_bytes, 90_000_000);

        let (tuned, _) = resolve(
            "route",
            &value(serde_json::json!({"max_request_bytes": "1048576"})),
        )
        .unwrap();
        assert_eq!(tuned.max_request_bytes, 1_048_576);

        assert!(resolve("route", &value(serde_json::json!({"max_request_bytes": 0}))).is_err());
    }

    /// A sink can fan one stream across indexes; a reader pages through exactly
    /// one, so the same template has nothing to resolve against there.
    #[test]
    fn a_templated_index_is_a_sink_only_feature() {
        let routed = value(serde_json::json!({"index": "${metadata:postgres.table}"}));

        let (_, index) = resolve_for_publisher("route", &routed).unwrap();
        assert_eq!(index, "${metadata:postgres.table}");

        let error = resolve_for_consumer("route", &routed).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ConsumerError>(),
            Some(ConsumerError::Permanent(_))
        ));
        assert!(format!("{error:#}").contains("cannot be a template"));
    }

    #[test]
    fn a_token_anywhere_in_the_value_makes_it_a_template() {
        assert!(is_template("${metadata:postgres.table}"));
        assert!(is_template("app_${metadata:postgres.table}"));
        assert!(!is_template("movies"));
        assert!(!is_template(""));
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

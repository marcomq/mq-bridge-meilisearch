use anyhow::{anyhow, Context};
use schemars::JsonSchema;
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
    const ALL: [Self; 2] = [Self::Replace, Self::Update];

    /// The spelling `rename_all = "lowercase"` gives each variant. Exhaustive,
    /// so a new variant cannot reach the schema unnamed.
    fn as_str(self) -> &'static str {
        match self {
            WriteMethod::Replace => "replace",
            WriteMethod::Update => "update",
        }
    }

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
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MeilisearchConfig {
    /// Base URL of the Meilisearch instance, e.g. `http://localhost:7700`.
    ///
    /// Takes the address of a `meilisearch://host:7700/prefix?index=…` URI —
    /// everything before the query — so a path prefix survives and `index` stays
    /// a query parameter rather than competing with it.
    #[schemars(extend("x-mqb-uri" = "url"))]
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
    #[schemars(schema_with = "write_method_schema")]
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
    /// Output only: the largest document request this endpoint will send. A run
    /// bigger than this is split into several requests, still in order, rather
    /// than rejected whole as `payload_too_large`.
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: u64,
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

/// `method` as a flat `enum`, rather than the `$ref` to a `oneOf` of `const`s
/// a derived schema spells a documented enum as. A host resolves neither, so
/// the derived form leaves the field unchecked.
fn write_method_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "enum": WriteMethod::ALL.map(WriteMethod::as_str),
    })
}

/// What this endpoint accepts, as the JSON Schema a host reads to render a form
/// for it and to turn a URI's query string into typed configuration.
///
/// Derived rather than written out, so it cannot drift from the struct.
pub(crate) fn config_schema() -> Option<serde_json::Value> {
    serde_json::to_value(schemars::schema_for!(MeilisearchConfig)).ok()
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

pub(crate) fn resolve_for_consumer(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(MeilisearchConfig, String)> {
    let (config, index) = resolve(route_name, value)?;
    if is_template(&index) {
        return Err(anyhow!(
            "Meilisearch `index` cannot be a template when reading: a reader pages through one index, so '{index}' has nothing to resolve against"
        ));
    }
    Ok((config, index))
}

pub(crate) fn resolve(
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

    /// The declared schema is the endpoint's only contract: a host maps a URI
    /// against it and hands over typed values, so the endpoint takes those and
    /// nothing else.
    #[test]
    fn the_typed_forms_are_what_the_endpoint_takes() {
        let (config, _) = resolve(
            "route",
            &value(serde_json::json!({
                "create_index": false,
                "task_timeout_ms": 30000,
                "connect_timeout_ms": 250,
                "delete_values": ["delete", "remove"],
            })),
        )
        .unwrap();
        assert!(!config.create_index);
        assert_eq!(config.task_timeout_ms, 30_000);
        assert_eq!(config.connect_timeout_ms, Some(250));
        assert_eq!(
            config.delete_values,
            vec!["delete".to_owned(), "remove".to_owned()]
        );

        for spelled_as_text in [
            serde_json::json!({"create_index": "false"}),
            serde_json::json!({"task_timeout_ms": "30000"}),
            serde_json::json!({"delete_values": "delete,remove"}),
            serde_json::json!({"create_index": "yes"}),
        ] {
            assert!(resolve("route", &value(spelled_as_text)).is_err());
        }
    }

    /// A `$ref` to a `oneOf` of `const`s is not something a host checks, so the
    /// schema names the variants inline — and names all of them.
    #[test]
    fn the_declared_method_enum_names_every_variant() {
        let schema = config_schema().expect("a schema");
        let method = &schema["properties"]["method"];
        assert_eq!(method["type"], serde_json::json!("string"));
        assert!(method.get("$ref").is_none());

        let declared = method["enum"].as_array().expect("an enum");
        assert_eq!(declared.len(), WriteMethod::ALL.len());
        for (value, variant) in declared.iter().zip(WriteMethod::ALL) {
            assert_eq!(
                serde_json::from_value::<WriteMethod>(value.clone()).unwrap(),
                variant
            );
        }
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
            &value(serde_json::json!({"max_request_bytes": 1048576})),
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

        let (_, index) = resolve("route", &routed).unwrap();
        assert_eq!(index, "${metadata:postgres.table}");

        let error = resolve_for_consumer("route", &routed).unwrap_err();
        assert!(format!("{error:#}").contains("cannot be a template"));
    }

    #[test]
    fn a_token_anywhere_in_the_value_makes_it_a_template() {
        assert!(is_template("${metadata:postgres.table}"));
        assert!(is_template("app_${metadata:postgres.table}"));
        assert!(!is_template("movies"));
        assert!(!is_template(""));
    }
}

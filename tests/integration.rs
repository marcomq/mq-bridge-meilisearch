//! Docker-backed checks of the directly linked endpoint.
//!
//! Start the instance the tests expect with:
//!
//! ```console
//! cargo test --test integration -- --ignored --nocapture
//! ```

use mq_bridge::{
    errors::PublisherError,
    test_utils::run_test_with_docker,
    traits::{MessageConsumer, MessageDisposition, MessagePublisher},
    CanonicalMessage, SentBatch,
};

const URL: &str = "http://localhost:7700";
const API_KEY: &str = "mq-bridge-test-key";

/// Both tests share one process, and a second `register()` is an error.
fn register_once() {
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED
        .call_once(|| mq_bridge_meilisearch::register().expect("register Meilisearch endpoint"));
}

fn factory() -> std::sync::Arc<dyn mq_bridge::traits::CustomEndpointFactory> {
    register_once();
    mq_bridge::extensions::get_endpoint_factory("meilisearch")
        .expect("meilisearch endpoint factory should be registered")
}

fn index_name(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

fn config(index: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut config = serde_json::json!({
        "url": URL,
        "api_key": API_KEY,
        "index": index,
        "primary_key": "id",
    });
    let object = config.as_object_mut().unwrap();
    for (key, value) in extra.as_object().unwrap() {
        object.insert(key.clone(), value.clone());
    }
    config
}

fn document(id: u64, title: &str, operation: Option<&str>) -> CanonicalMessage {
    let mut message =
        CanonicalMessage::from(serde_json::json!({"id": id, "title": title}).to_string());
    if let Some(operation) = operation {
        message
            .metadata
            .insert("postgres.operation".to_owned(), operation.to_owned());
    }
    message
}

/// Reads the whole index through the endpoint's own consumer, acknowledging as
/// it goes, and returns the documents it produced.
async fn read_all(consumer: &mut dyn MessageConsumer) -> Vec<serde_json::Value> {
    let mut documents = Vec::new();
    loop {
        let batch = consumer.receive_batch(100).await.expect("receive a batch");
        if batch.messages.is_empty() {
            return documents;
        }
        let count = batch.messages.len();
        documents.extend(
            batch
                .messages
                .iter()
                .map(|message| serde_json::from_slice(&message.payload).expect("a JSON document")),
        );
        (batch.commit)(vec![MessageDisposition::Ack; count])
            .await
            .expect("acknowledge the batch");
    }
}

fn ids(documents: &[serde_json::Value]) -> Vec<u64> {
    let mut ids: Vec<u64> = documents
        .iter()
        .map(|document| document["id"].as_u64().expect("an id"))
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn upserts_are_indexed_and_deletes_remove_them_again() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("round-trip");
        let config = config(
            &index,
            serde_json::json!({"operation": "${metadata:postgres.operation}"}),
        );

        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");
        publisher
            .send_batch(vec![
                document(1, "one", Some("insert")),
                document(2, "two", Some("insert")),
                document(3, "three", Some("insert")),
            ])
            .await
            .expect("publish three documents");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("create Meilisearch consumer");
        assert_eq!(ids(&read_all(&mut *consumer).await), vec![1, 2, 3]);

        publisher
            .send_batch(vec![document(2, "two", Some("delete"))])
            .await
            .expect("publish a delete");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("re-create Meilisearch consumer");
        assert_eq!(ids(&read_all(&mut *consumer).await), vec![1, 3]);
    })
    .await;
}

/// An insert and a delete of one key in a single batch: the endpoint must issue
/// them as two requests in source order, or the document survives its delete.
#[tokio::test]
#[ignore = "requires Docker"]
async fn an_insert_and_a_delete_of_one_key_in_one_batch_end_with_the_document_gone() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("ordering");
        let config = config(
            &index,
            serde_json::json!({"operation": "${metadata:postgres.operation}"}),
        );

        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");
        publisher
            .send_batch(vec![
                document(7, "seven", Some("insert")),
                document(8, "eight", Some("insert")),
                document(7, "seven", Some("delete")),
            ])
            .await
            .expect("publish a mixed batch");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("create Meilisearch consumer");
        assert_eq!(ids(&read_all(&mut *consumer).await), vec![8]);
    })
    .await;
}

/// The headline recipe from the README: two source tables writing into one
/// document, with Meilisearch performing the join through a partial merge.
#[tokio::test]
#[ignore = "requires Docker"]
async fn update_merges_rows_from_different_tables_into_one_document() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("merge");
        let config = config(&index, serde_json::json!({"method": "update"}));

        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");
        publisher
            .send_batch(vec![CanonicalMessage::from(
                serde_json::json!({"id": 1, "title": "Stalker"}).to_string(),
            )])
            .await
            .expect("publish the first table's row");
        publisher
            .send_batch(vec![CanonicalMessage::from(
                serde_json::json!({"id": 1, "director": "Tarkovsky"}).to_string(),
            )])
            .await
            .expect("publish the second table's row");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("create Meilisearch consumer");
        let documents = read_all(&mut *consumer).await;

        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0]["title"], "Stalker");
        assert_eq!(documents[0]["director"], "Tarkovsky");
    })
    .await;
}

/// Meilisearch answers a write with 202 and applies it afterwards. Acking on
/// that 202 would commit the source cursor for a write that then failed, so the
/// publisher waits for the task and reports the failure back to the route.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_task_that_fails_after_being_accepted_is_reported_not_acknowledged() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("task-failure");
        let config = config(&index, serde_json::json!({}));

        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");
        let without_primary_key =
            CanonicalMessage::from(serde_json::json!({"title": "no id here"}).to_string());

        let sent = publisher
            .send_batch(vec![without_primary_key])
            .await
            .expect("the request itself is accepted");

        let SentBatch::Partial { failed, .. } = sent else {
            panic!("a failed indexing task must not be acknowledged");
        };
        assert_eq!(failed.len(), 1);
        assert!(
            matches!(failed[0].1, PublisherError::NonRetryable(_)),
            "a document Meilisearch rejects cannot heal by retrying: {:?}",
            failed[0].1
        );
    })
    .await;
}

/// What `mqb copy … meilisearch://…` actually hands the endpoint.
///
/// The CLI derives a plugin endpoint's `url` from the URI up to the query and
/// makes every query parameter a string, so this config is built the same way
/// rather than in its typed form. If this breaks, the CLI is broken.
#[tokio::test]
#[ignore = "requires Docker"]
async fn the_config_the_cli_builds_from_a_uri_works() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("cli-uri");
        // meilisearch://localhost:7700?index=…&primary_key=id&api_key=…&create_index=true
        let config = serde_json::json!({
            "url": "meilisearch://localhost:7700",
            "index": index,
            "primary_key": "id",
            "api_key": API_KEY,
            "create_index": "true",
            "wait_for_task": "true",
            "task_timeout_ms": "30000",
        });

        factory
            .create_publisher(&index, &config)
            .await
            .expect("the CLI's URI-derived config should build a publisher")
            .send_batch(vec![document(1, "one", None)])
            .await
            .expect("publish through the CLI-shaped config");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("create Meilisearch consumer");
        assert_eq!(ids(&read_all(&mut *consumer).await), vec![1]);
    })
    .await;
}

/// A nacked batch rolls the scan position back, so its documents are read
/// again rather than skipped.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_nacked_batch_is_read_again() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("nack");
        let config = config(&index, serde_json::json!({}));

        factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher")
            .send_batch(vec![document(1, "one", None), document(2, "two", None)])
            .await
            .expect("publish two documents");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("create Meilisearch consumer");

        let first = consumer.receive_batch(2).await.expect("receive a batch");
        assert_eq!(first.messages.len(), 2);
        (first.commit)(vec![MessageDisposition::Nack; 2])
            .await
            .expect("nack the batch");

        let second = consumer.receive_batch(2).await.expect("receive again");
        assert_eq!(second.messages.len(), 2, "a nacked batch must come back");
    })
    .await;
}

/// The scan position survives a restart when a checkpoint file is configured.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_checkpointed_scan_resumes_where_it_stopped() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("resume");
        let directory = std::env::temp_dir().join(index_name("checkpoint"));
        let checkpoint = directory.join("cursors.json");
        let config = config(
            &index,
            serde_json::json!({
                "cursor_id": "scan",
                "checkpoint_store": format!("file://{}", checkpoint.display()),
            }),
        );

        factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher")
            .send_batch((1..=4).map(|id| document(id, "x", None)).collect())
            .await
            .expect("publish four documents");

        let mut consumer = factory
            .create_consumer(&index, &config)
            .await
            .expect("create Meilisearch consumer");
        let first = consumer.receive_batch(2).await.expect("receive a batch");
        let count = first.messages.len();
        assert_eq!(count, 2);
        (first.commit)(vec![MessageDisposition::Ack; count])
            .await
            .expect("acknowledge the first half");

        let mut resumed = factory
            .create_consumer(&index, &config)
            .await
            .expect("re-create the consumer from the checkpoint");
        assert_eq!(ids(&read_all(&mut *resumed).await), vec![3, 4]);

        tokio::fs::remove_dir_all(&directory).await.ok();
    })
    .await;
}

/// Reads a Meilisearch endpoint directly, for checks the endpoint's own
/// consumer does not cover — index settings are not documents.
async fn get(path: &str) -> serde_json::Value {
    let response = reqwest::Client::new()
        .get(format!("{URL}{path}"))
        .header("Authorization", format!("Bearer {API_KEY}"))
        .send()
        .await
        .expect("reach Meilisearch");
    assert!(response.status().is_success(), "GET {path} failed");
    let body = response.text().await.expect("a response body");
    serde_json::from_str(&body).expect("a JSON response")
}

fn routed_document(id: u64, table: &str) -> CanonicalMessage {
    let mut message = document(id, "x", None);
    message
        .metadata
        .insert("postgres.table".to_owned(), table.to_owned());
    message
}

/// The multi-table case: one CDC stream, one sink, an index per source table,
/// each created on first write because none of them is known at startup.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_templated_index_fans_one_batch_out_across_indexes() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let prefix = index_name("routed");
        let movies = format!("{prefix}-movies");
        let directors = format!("{prefix}-directors");

        let publisher = factory()
            .create_publisher(
                "routed",
                &config(
                    &prefix,
                    serde_json::json!({"index": "${metadata:postgres.table}"}),
                ),
            )
            .await
            .expect("create Meilisearch publisher");

        let sent = publisher
            .send_batch(vec![
                routed_document(1, &movies),
                routed_document(2, &movies),
                routed_document(3, &directors),
            ])
            .await
            .expect("send the routed batch");
        assert!(matches!(sent, SentBatch::Ack), "{sent:?}");

        for (index, expected) in [(&movies, vec![1, 2]), (&directors, vec![3])] {
            let mut consumer = factory()
                .create_consumer(index, &config(index, serde_json::json!({})))
                .await
                .expect("create Meilisearch consumer");
            assert_eq!(ids(&read_all(consumer.as_mut()).await), expected);
            assert_eq!(
                get(&format!("/indexes/{index}")).await["primaryKey"],
                serde_json::json!("id"),
                "a routed index should be created with the configured primary key"
            );
        }
    })
    .await;
}

/// `mqb copy` can stand an index up from nothing: the settings a search needs
/// are part of the endpoint's configuration, not a separate provisioning step.
#[tokio::test]
#[ignore = "requires Docker"]
async fn settings_are_applied_to_the_index_before_documents_arrive() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let index = index_name("settings");
        let publisher = factory()
            .create_publisher(
                &index,
                &config(
                    &index,
                    serde_json::json!({
                        "settings": {
                            "searchableAttributes": ["title"],
                            "filterableAttributes": ["title"],
                        }
                    }),
                ),
            )
            .await
            .expect("create Meilisearch publisher");

        let settings = get(&format!("/indexes/{index}/settings")).await;
        assert_eq!(
            settings["searchableAttributes"],
            serde_json::json!(["title"])
        );
        assert_eq!(
            settings["filterableAttributes"],
            serde_json::json!(["title"])
        );
        // A key the route never set keeps Meilisearch's own default, because
        // `PATCH /settings` merges rather than replaces.
        assert_eq!(settings["displayedAttributes"], serde_json::json!(["*"]));

        publisher
            .send_batch(vec![document(1, "one", None)])
            .await
            .expect("send a document");

        // A setting Meilisearch does not know must stop the route, not be
        // dropped in silence: the index would otherwise not match the config.
        let rejected = index_name("bad-settings");
        let outcome = factory()
            .create_publisher(
                &rejected,
                &config(
                    &rejected,
                    serde_json::json!({"settings": {"notASetting": ["title"]}}),
                ),
            )
            .await;
        let Err(error) = outcome else {
            panic!("an unknown setting should fail the route");
        };
        assert!(
            format!("{error:#}").contains("prepare Meilisearch index"),
            "{error:#}"
        );
    })
    .await;
}

/// A batch larger than one request must still arrive whole, in order, rather
/// than being dead-lettered as `payload_too_large`.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_batch_over_the_request_limit_is_split_and_still_lands_complete() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let index = index_name("chunked");
        let publisher = factory()
            .create_publisher(
                &index,
                &config(&index, serde_json::json!({"max_request_bytes": 64})),
            )
            .await
            .expect("create Meilisearch publisher");

        let documents: Vec<_> = (1..=10).map(|id| document(id, "title", None)).collect();
        let sent = publisher
            .send_batch(documents)
            .await
            .expect("send the oversized batch");
        assert!(matches!(sent, SentBatch::Ack), "{sent:?}");

        let mut consumer = factory()
            .create_consumer(&index, &config(&index, serde_json::json!({})))
            .await
            .expect("create Meilisearch consumer");
        assert_eq!(
            ids(&read_all(consumer.as_mut()).await),
            (1..=10).collect::<Vec<_>>()
        );
    })
    .await;
}

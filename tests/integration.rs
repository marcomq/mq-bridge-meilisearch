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

/// `mqb copy` with no `--batch-size` asks the endpoint for one.
#[test]
fn the_host_reads_the_batch_size_the_endpoint_asks_for() {
    register_once();
    let asked = mq_bridge::extensions::endpoint_default_batch_size("meilisearch");
    assert_eq!(asked, Some(50_000));
}

fn index_name(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

/// Makes Meilisearch validate `_geo`, which it only does at indexing time: a
/// document failing that check is accepted by the request and fails its task.
fn geo_filterable() -> serde_json::Value {
    serde_json::json!({"settings": {"filterableAttributes": ["_geo"]}})
}

fn bad_geo(id: u64) -> CanonicalMessage {
    CanonicalMessage::from(serde_json::json!({"id": id, "_geo": "nowhere"}).to_string())
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

/// The README's `_vectors` recipe, run as written: a vector arriving as a JSON
/// string is stored as a vector, and a row carrying none is still indexed.
#[tokio::test]
#[ignore = "requires Docker"]
async fn the_readme_vector_recipe_stores_a_user_provided_vector() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let readme = include_str!("../README.md");
        let recipe = readme
            .split("```yaml\n")
            .find(|block| block.contains("_vectors.default"))
            .and_then(|block| block.split("```").next())
            .expect("the README shows a `_vectors` recipe");
        let recipe: serde_json::Value = serde_yaml::from_str(recipe).expect("valid YAML");
        let output = &recipe["output"];
        let endpoint: mq_bridge::models::Endpoint =
            serde_json::from_value(output.clone()).expect("the README's output is an endpoint");
        let settings = &output["custom"]["config"]["settings"];
        let dimensions = settings["embedders"]["default"]["dimensions"]
            .as_u64()
            .expect("the embedder's dimensions");

        let index = index_name("vectors");
        let sink = factory()
            .create_publisher(
                &index,
                &config(&index, serde_json::json!({"settings": settings})),
            )
            .await
            .expect("create Meilisearch publisher");
        let publisher =
            mq_bridge::middleware::apply_middlewares_to_publisher(sink, &endpoint, &index)
                .await
                .expect("the README's transform compiles");

        let vector = vec![0.5; dimensions as usize];
        let embedded = serde_json::json!({
            "id": 1,
            "title": "Solaris",
            "embedding": serde_json::to_string(&vector).unwrap(),
        });
        let bare = serde_json::json!({"id": 2, "title": null, "embedding": null});
        let sent = publisher
            .send_batch(vec![
                CanonicalMessage::from(embedded.to_string()),
                CanonicalMessage::from(bare.to_string()),
            ])
            .await
            .expect("publish through the transform");
        assert!(matches!(sent, SentBatch::Ack), "{sent:?}");

        let stored = get(&format!(
            "/indexes/{index}/documents/1?retrieveVectors=true"
        ))
        .await;
        assert_eq!(stored["title"], "Solaris");
        assert_eq!(
            stored["_vectors"]["default"]["embeddings"][0],
            serde_json::json!(vector)
        );
        assert!(
            stored.get("embedding").is_none(),
            "only mapped fields are indexed"
        );
        get(&format!("/indexes/{index}/documents/2")).await;
    })
    .await;
}

/// Rebuilds `live` from the example file through a real route, the only place
/// a publisher is told how the route ended, and checks the swap happened.
async fn a_drained_rebuild_goes_live(run: &str) -> anyhow::Result<()> {
    use anyhow::Context as _;
    let (build, live) = (format!("build-{run}"), format!("live-{run}"));
    let endpoint = |index: &str, extra: serde_json::Value| {
        let mut config = serde_json::json!({
            "url": URL, "api_key": API_KEY, "index": index, "primary_key": "id",
        });
        config
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        config
    };
    let factory = mq_bridge::extensions::get_endpoint_factory("meilisearch")
        .expect("the meilisearch endpoint is registered");
    let stale =
        CanonicalMessage::from(serde_json::json!({"id": 999, "title": "stale"}).to_string());
    let old = factory
        .create_publisher(&live, &endpoint(&live, serde_json::json!({})))
        .await?;
    old.send_batch(vec![stale]).await?;

    let route: mq_bridge::Route = serde_json::from_value(serde_json::json!({
        "exit_on_empty": true,
        "input": {"file": {"path": "examples/movies.jsonl", "format": "raw"}},
        "output": {"custom": {
            "name": "meilisearch",
            "config": endpoint(&build, serde_json::json!({"swap_into": live})),
        }},
    }))?;
    route.run(&build).await?.join().await?;

    let fetch = |index: String| async move {
        reqwest::Client::new()
            .get(format!("{URL}/indexes/{index}/documents?fields=id"))
            .header("Authorization", format!("Bearer {API_KEY}"))
            .send()
            .await
    };
    let documents: serde_json::Value = serde_json::from_str(&fetch(live).await?.text().await?)?;
    let stale_left = documents["results"]
        .as_array()
        .context("a documents page")?
        .iter()
        .any(|document| document["id"] == 999);
    if documents["total"] != 3 || stale_left {
        anyhow::bail!("the live index was not replaced by the rebuilt one: {documents}");
    }
    let leftover = fetch(build).await?.status();
    if leftover != reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("the build index should be gone after the swap, got {leftover}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker"]
async fn swap_into_puts_a_drained_rebuild_live_and_removes_the_build_index() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        register_once();
        let run = uuid::Uuid::new_v4().simple().to_string();
        a_drained_rebuild_goes_live(&run)
            .await
            .expect("a linked rebuild is swapped in");
    })
    .await;
}

/// A parent row that changes must reach the copy of it embedded in every child
/// document, and leave the documents of other parents alone.
#[tokio::test]
#[ignore = "requires Docker"]
async fn update_where_refreshes_the_field_joined_into_other_documents() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("patch");
        let filterable = serde_json::json!({"settings": {"filterableAttributes": ["author.id"]}});
        let books = factory
            .create_publisher(&index, &config(&index, filterable))
            .await
            .expect("create the publisher of the child documents");
        let book = |id: u64, author: u64| {
            let author = serde_json::json!({"id": author, "name": "old"});
            let book =
                serde_json::json!({"id": id, "title": format!("book {id}"), "author": author});
            CanonicalMessage::from(book.to_string())
        };
        books
            .send_batch(vec![book(1, 5), book(2, 5), book(3, 6)])
            .await
            .expect("index the child documents");

        let authors = factory
            .create_publisher(
                &index,
                &config(
                    &index,
                    serde_json::json!({
                        "update_where": "author.id = ${payload:id}",
                        "update_into": "author",
                        "max_request_bytes": 40,
                    }),
                ),
            )
            .await
            .expect("create the publisher of the parent rows");
        let author = |id: u64, name: &str| {
            CanonicalMessage::from(serde_json::json!({"id": id, "name": name}).to_string())
        };
        let sent = authors
            .send_batch(vec![
                author(5, "first"),
                author(5, "new \"name\""),
                author(7, "nobody"),
            ])
            .await
            .expect("publish the parent changes");
        assert!(matches!(sent, SentBatch::Ack));

        let mut consumer = factory
            .create_consumer(&index, &config(&index, serde_json::json!({})))
            .await
            .expect("create Meilisearch consumer");
        let mut documents = read_all(&mut *consumer).await;
        documents.sort_by_key(|document| document["id"].as_u64());

        assert_eq!(documents.len(), 3, "a parent without children adds nothing");
        for document in &documents[..2] {
            assert_eq!(
                document["author"],
                serde_json::json!({"id": 5, "name": "new \"name\""})
            );
            assert!(
                document["title"].is_string(),
                "the rest of the document stays"
            );
        }
        assert_eq!(documents[2]["author"]["name"], "old");
    })
    .await;
}

/// A child row whose parent document is gone must not recreate it as a stub,
/// while one whose parent is still queued in Meilisearch must wait for it.
#[tokio::test]
#[ignore = "requires Docker"]
async fn update_existing_skips_documents_that_do_not_exist() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("existing");
        let owner = factory
            .create_publisher(&index, &config(&index, serde_json::json!({})))
            .await
            .expect("create the owning publisher");
        owner
            .send_batch(vec![document(1, "Stalker", None)])
            .await
            .expect("publish the parent row");

        let child_config = config(&index, serde_json::json!({"method": "update_existing"}));
        let child = factory
            .create_publisher(&index, &child_config)
            .await
            .expect("create the update_existing publisher");
        let queued = reqwest::Client::new()
            .post(format!("{URL}/indexes/{index}/documents"))
            .header("Authorization", format!("Bearer {API_KEY}"))
            .header("Content-Type", "application/json")
            .body(serde_json::json!([{"id": 2, "title": "Solaris"}]).to_string())
            .send()
            .await
            .expect("enqueue a parent without waiting for it");
        assert!(queued.status().is_success());
        let sent = child
            .send_batch(vec![
                CanonicalMessage::from(serde_json::json!({"id": 1, "rating": 8}).to_string()),
                CanonicalMessage::from(serde_json::json!({"id": 2, "rating": 7}).to_string()),
                CanonicalMessage::from(serde_json::json!({"id": 3, "rating": 5}).to_string()),
            ])
            .await
            .expect("publish three child rows");
        assert!(
            matches!(sent, SentBatch::Ack),
            "a skipped row is acknowledged"
        );

        let mut consumer = factory
            .create_consumer(&index, &child_config)
            .await
            .expect("create Meilisearch consumer");
        let documents = read_all(&mut *consumer).await;
        assert_eq!(ids(&documents), vec![1, 2]);
        let by_id = |id: u64| {
            documents
                .iter()
                .find(|document| document["id"] == id)
                .unwrap()
        };
        assert_eq!(by_id(1)["title"], "Stalker");
        assert_eq!(by_id(1)["rating"], 8);
        assert_eq!(by_id(2)["title"], "Solaris");
        assert_eq!(by_id(2)["rating"], 7);
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
        let config = config(&index, geo_filterable());

        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");
        let sent = publisher
            .send_batch(vec![bad_geo(1)])
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

/// A failed run hands back its own messages and every later segment, even
/// though those were already enqueued when the failure showed up; the other
/// run of its segment, touching other documents, is acknowledged.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_failed_run_mid_batch_reports_it_and_every_later_segment() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("mid-failure");
        let mut config = config(
            &index,
            serde_json::json!({"operation": "${metadata:postgres.operation}"}),
        );
        config["settings"] = geo_filterable()["settings"].clone();
        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");

        let mut invalid = bad_geo(5);
        invalid
            .metadata
            .insert("postgres.operation".to_owned(), "insert".to_owned());
        let batch = vec![
            document(1, "one", Some("insert")),
            invalid,
            document(2, "two", Some("delete")),
            document(1, "one", Some("delete")),
        ];
        let expected: Vec<_> = [0, 1, 3].iter().map(|&i| batch[i].message_id).collect();

        let SentBatch::Partial { failed, .. } =
            publisher.send_batch(batch).await.expect("send a batch")
        else {
            panic!("a batch with a failed run must not be acknowledged");
        };
        let failed: Vec<_> = failed.iter().map(|(m, _)| m.message_id).collect();
        assert_eq!(failed, expected);
    })
    .await;
}

/// A CDC batch whose operations alternate between different documents costs
/// one upsert and one delete task, not one task per switch.
#[tokio::test]
#[ignore = "requires Docker"]
async fn alternating_operations_on_distinct_documents_cost_two_tasks() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("pipelined");
        let config = config(
            &index,
            serde_json::json!({"operation": "${metadata:postgres.operation}"}),
        );
        let publisher = factory
            .create_publisher(&index, &config)
            .await
            .expect("create Meilisearch publisher");

        let batch = (0..100)
            .flat_map(|id| {
                [
                    document(id, "kept", Some("insert")),
                    document(1000 + id, "gone", Some("delete")),
                ]
            })
            .collect();
        assert!(matches!(
            publisher.send_batch(batch).await.expect("send a batch"),
            SentBatch::Ack
        ));

        let tasks = get(&format!("/tasks?indexUids={index}&limit=1000")).await;
        let tasks = tasks["results"].as_array().expect("a task list");
        let writes: Vec<_> = tasks
            .iter()
            .filter(|task| task["type"] != "indexCreation")
            .collect();
        assert_eq!(writes.len(), 2, "one task per operation switch: {writes:?}");
    })
    .await;
}

/// What `mqb copy … meilisearch://…` actually hands the endpoint.
///
/// A host builds a plugin endpoint's configuration by mapping the URI against
/// the schema the endpoint declares, so this runs that mapping rather than a
/// hand-built imitation of it. If this breaks, the CLI is broken.
#[tokio::test]
#[ignore = "requires Docker"]
async fn the_config_the_cli_builds_from_a_uri_works() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let factory = factory();
        let index = index_name("cli-uri");
        let config = serde_json::Value::Object(
            mq_bridge::plugin::endpoint_uri_schema("meilisearch")
                .config_from_uri(&format!(
                    "meilisearch://localhost:7700?index={index}&primary_key=id\
                     &api_key={API_KEY}&create_index=true&wait_for_task=true\
                     &task_timeout_ms=30000"
                ))
                .expect("the URI should map onto the declared schema"),
        );

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

#[tokio::test]
#[ignore = "requires Docker"]
async fn gzip_compressed_documents_are_indexed_and_searchable() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let index = index_name("gzip");
        let publisher = factory()
            .create_publisher(
                &index,
                &config(&index, serde_json::json!({"compression": "gzip"})),
            )
            .await
            .expect("create Meilisearch publisher");

        let sent = publisher
            .send_batch(vec![
                document(1, "compressed", None),
                document(2, "plain", None),
            ])
            .await
            .expect("send the compressed batch");
        assert!(matches!(sent, SentBatch::Ack), "{sent:?}");

        let found = get(&format!("/indexes/{index}/search?q=compressed")).await;
        assert_eq!(ids(found["hits"].as_array().expect("hits")), vec![1]);
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

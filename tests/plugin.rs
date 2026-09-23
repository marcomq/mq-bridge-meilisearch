//! The same Meilisearch implementation, reached two ways.
//!
//! One suite runs against the factory linked directly into this test binary,
//! and again against the factory the host builds from the compiled plugin
//! library. Identical results mean the ABI round trip preserved the endpoint's
//! behaviour — and that the Python and npm packages, which load that very
//! library, get the same endpoint.
//!
//! mq-bridge's own `plugin::conformance` suite is not used here. Its checks
//! publish plain-string payloads and compare them to what comes back byte for
//! byte, while Meilisearch stores JSON documents and returns them re-serialized
//! from its own store. The checks below make the same direct-versus-plugin
//! comparison over documents, which is what this endpoint actually carries.

use anyhow::{bail, Context};
use mq_bridge::{
    plugin::{load_endpoint_plugin, test_support::build_plugin_cdylib},
    test_utils::run_test_with_docker,
    traits::{CustomEndpointFactory, MessageConsumer, MessageDisposition},
    CanonicalMessage,
};
use mq_bridge_meilisearch::{MeilisearchConfig, MeilisearchFactory};
use serde_json::json;

const URL: &str = "http://localhost:7700";
const API_KEY: &str = "mq-bridge-test-key";

fn config(index: &str) -> serde_json::Value {
    serde_json::json!({
        "url": URL,
        "api_key": API_KEY,
        "index": index,
        "primary_key": "id",
        "operation": "${metadata:postgres.operation}",
    })
}

fn document(id: u64, operation: &str) -> CanonicalMessage {
    let mut message =
        CanonicalMessage::from(serde_json::json!({"id": id, "title": "doc"}).to_string());
    message
        .metadata
        .insert("postgres.operation".to_owned(), operation.to_owned());
    message
}

async fn read_ids(consumer: &mut dyn MessageConsumer) -> anyhow::Result<Vec<u64>> {
    let mut ids = Vec::new();
    loop {
        let batch = consumer.receive_batch(100).await?;
        if batch.messages.is_empty() {
            ids.sort_unstable();
            return Ok(ids);
        }
        let count = batch.messages.len();
        for message in &batch.messages {
            let document: serde_json::Value = serde_json::from_slice(&message.payload)?;
            ids.push(
                document["id"]
                    .as_u64()
                    .context("a document without an id")?,
            );
        }
        (batch.commit)(vec![MessageDisposition::Ack; count]).await?;
    }
}

async fn publish(
    factory: &dyn CustomEndpointFactory,
    index: &str,
    messages: Vec<CanonicalMessage>,
) -> anyhow::Result<()> {
    let publisher = factory.create_publisher(index, &config(index)).await?;
    match publisher.send_batch(messages).await? {
        mq_bridge::SentBatch::Ack => Ok(()),
        mq_bridge::SentBatch::Partial { failed, .. } => {
            bail!("{} message(s) were not indexed: {:?}", failed.len(), failed)
        }
    }
}

async fn round_trip(factory: &dyn CustomEndpointFactory, index: &str) -> anyhow::Result<()> {
    publish(
        factory,
        index,
        (1..=4).map(|id| document(id, "insert")).collect(),
    )
    .await?;
    let mut consumer = factory.create_consumer(index, &config(index)).await?;
    let ids = read_ids(&mut *consumer).await?;
    if ids != vec![1, 2, 3, 4] {
        bail!("published 1..=4 but read back {ids:?}");
    }
    Ok(())
}

async fn delete_removes_the_document(
    factory: &dyn CustomEndpointFactory,
    index: &str,
) -> anyhow::Result<()> {
    publish(
        factory,
        index,
        vec![document(1, "insert"), document(2, "insert")],
    )
    .await?;
    publish(factory, index, vec![document(1, "delete")]).await?;
    let mut consumer = factory.create_consumer(index, &config(index)).await?;
    let ids = read_ids(&mut *consumer).await?;
    if ids != vec![2] {
        bail!("expected only document 2 to survive the delete, found {ids:?}");
    }
    Ok(())
}

async fn a_nacked_batch_is_read_again(
    factory: &dyn CustomEndpointFactory,
    index: &str,
) -> anyhow::Result<()> {
    publish(factory, index, vec![document(1, "insert")]).await?;
    let mut consumer = factory.create_consumer(index, &config(index)).await?;

    let first = consumer.receive_batch(1).await?;
    if first.messages.len() != 1 {
        bail!("expected one document, got {}", first.messages.len());
    }
    (first.commit)(vec![MessageDisposition::Nack]).await?;

    let second = consumer.receive_batch(1).await?;
    if second.messages.len() != 1 {
        bail!("a nacked batch was not read again");
    }
    Ok(())
}

async fn documents_carry_locating_metadata(
    factory: &dyn CustomEndpointFactory,
    index: &str,
) -> anyhow::Result<()> {
    publish(factory, index, vec![document(9, "insert")]).await?;
    let mut consumer = factory.create_consumer(index, &config(index)).await?;
    let batch = consumer.receive_batch(1).await?;
    let message = batch.messages.first().context("no document was read")?;

    if message
        .metadata
        .get("meilisearch.index")
        .map(String::as_str)
        != Some(index)
    {
        bail!(
            "`meilisearch.index` did not survive: {:?}",
            message.metadata
        );
    }
    if message
        .metadata
        .get("meilisearch.document_id")
        .map(String::as_str)
        != Some("9")
    {
        bail!(
            "`meilisearch.document_id` did not survive: {:?}",
            message.metadata
        );
    }
    Ok(())
}

/// What ABI 1.1 carries across the boundary: the ordering flag, and a failure
/// reported for the messages it concerns rather than for the whole batch.
async fn sends_are_ordered_and_fail_per_message(
    factory: &dyn CustomEndpointFactory,
    index: &str,
) -> anyhow::Result<()> {
    let publisher = factory.create_publisher(index, &config(index)).await?;
    if !publisher.requires_ordered_publish() {
        bail!("the publisher does not ask the route to keep its sends in order");
    }

    let mut invalid =
        CanonicalMessage::from(json!({"id": "not a valid id!", "title": "doc"}).to_string());
    invalid
        .metadata
        .insert("postgres.operation".to_owned(), "insert".to_owned());
    let invalid_id = invalid.message_id;
    let batch = vec![document(1, "insert"), document(2, "delete"), invalid];
    match publisher.send_batch(batch).await? {
        mq_bridge::SentBatch::Partial { failed, .. }
            if failed.len() == 1 && failed[0].0.message_id == invalid_id => {}
        other => bail!("expected only the invalid document to fail, got {other:?}"),
    }

    let mut consumer = factory.create_consumer(index, &config(index)).await?;
    let ids = read_ids(&mut *consumer).await?;
    if ids != vec![1] {
        bail!("the runs before the failure should be indexed, found {ids:?}");
    }
    Ok(())
}

/// Runs every check against one factory, returning the names that passed. Each
/// check gets its own index, so none can see another's documents.
async fn suite(
    factory: &dyn CustomEndpointFactory,
    prefix: &str,
) -> anyhow::Result<Vec<&'static str>> {
    round_trip(factory, &format!("{prefix}-round-trip"))
        .await
        .context("check `round_trip` failed")?;
    delete_removes_the_document(factory, &format!("{prefix}-delete"))
        .await
        .context("check `delete_removes_the_document` failed")?;
    a_nacked_batch_is_read_again(factory, &format!("{prefix}-nack"))
        .await
        .context("check `a_nacked_batch_is_read_again` failed")?;
    documents_carry_locating_metadata(factory, &format!("{prefix}-metadata"))
        .await
        .context("check `documents_carry_locating_metadata` failed")?;
    sends_are_ordered_and_fail_per_message(factory, &format!("{prefix}-partial"))
        .await
        .context("check `sends_are_ordered_and_fail_per_message` failed")?;
    Ok(vec![
        "round_trip",
        "delete_removes_the_document",
        "a_nacked_batch_is_read_again",
        "documents_carry_locating_metadata",
        "sends_are_ordered_and_fail_per_message",
    ])
}

/// What the declared schema buys: a query string carries no types, so without
/// one `wait_for_task=false` would reach the endpoint as the string `"false"`.
/// The host coerces against the schema, so the endpoint's own types are what
/// arrive — which is why the endpoint takes those and nothing else.
#[test]
fn a_uri_maps_onto_the_declared_schema() {
    mq_bridge_meilisearch::register().expect("register the endpoint");

    let config = mq_bridge::plugin::endpoint_uri_schema("meilisearch")
        .config_from_uri(
            "meilisearch://localhost:7700?index=movies&wait_for_task=false\
             &task_timeout_ms=30000&delete_values=delete,remove&method=update",
        )
        .expect("map the uri");

    assert_eq!(config["url"], json!("meilisearch://localhost:7700"));
    assert_eq!(config["index"], json!("movies"));
    assert_eq!(config["wait_for_task"], json!(false));
    assert_eq!(config["task_timeout_ms"], json!(30000));
    assert_eq!(config["delete_values"], json!(["delete", "remove"]));
    assert_eq!(config["method"], json!("update"));

    // And the endpoint accepts what the mapping produced: a schema that maps a
    // URI the endpoint then rejects would be worth nothing, and this config
    // denies unknown fields.
    serde_json::from_value::<MeilisearchConfig>(serde_json::Value::Object(config))
        .expect("the mapped configuration deserializes");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker"]
async fn the_endpoint_behaves_the_same_linked_directly_and_loaded_as_a_plugin() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let run = uuid::Uuid::new_v4().simple().to_string();

        let direct = suite(&MeilisearchFactory, &format!("direct-{run}"))
            .await
            .expect("the directly linked endpoint should pass the suite");

        let library = build_plugin_cdylib(".", "mq-bridge-meilisearch").expect("build the plugin");
        let info = load_endpoint_plugin(&library).expect("load the plugin");
        assert_eq!(info.name, "meilisearch");
        assert_eq!((info.abi_major, info.abi_minor), (1, 1));
        assert!(info.supports_consumer && info.supports_publisher);
        let factory = mq_bridge::extensions::get_endpoint_factory(&info.name)
            .expect("loading a plugin registers its endpoint");

        let loaded = suite(factory.as_ref(), &format!("plugin-{run}"))
            .await
            .expect("the plugin-loaded endpoint should pass the same suite");

        assert_eq!(direct, loaded);
    })
    .await;
}

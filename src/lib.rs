//! Meilisearch input/output endpoint extension for `mq-bridge`.
//!
//! The output is a document sink: a batch becomes one NDJSON request per
//! contiguous run of upserts or deletes, and the asynchronous task Meilisearch
//! answers with is awaited before the batch is acknowledged. The input is a
//! non-destructive scan of an index's documents, resumable through the shared
//! [`mq_bridge::checkpoint`] store.
//!
//! The same implementation is used three ways:
//!
//! * linked directly by a Rust program, which calls [`register`];
//! * loaded from the compiled `cdylib` by any mq-bridge host through
//!   `mq_bridge::plugin::load_endpoint_plugin`;
//! * from Python or Node.js, whose `mq-bridge-meilisearch` packages ship that
//!   same library and call the host's generic loader.

mod checkpoint;
mod client;
mod config;
mod consumer;
mod publisher;

use std::sync::Arc;

use async_trait::async_trait;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

pub use config::{MeilisearchConfig, WriteMethod};

#[derive(Debug, Default)]
pub struct MeilisearchFactory;

// Exports the same factory as a loadable plugin. `register()` below covers the
// directly linked case; this covers every host that loads the compiled library,
// including the Python and Node.js packages.
#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "meilisearch",
    factory: MeilisearchFactory,
}

/// Registers this crate's factory under `meilisearch`. Call once, before
/// starting routes that use it. Only needed when linking this crate directly; a
/// host that loads the compiled plugin registers the endpoint as part of
/// loading it.
pub fn register() -> anyhow::Result<()> {
    mq_bridge::extensions::register_endpoint_factory("meilisearch", Arc::new(MeilisearchFactory))
}

#[async_trait]
impl CustomEndpointFactory for MeilisearchFactory {
    fn config_schema(&self) -> Option<serde_json::Value> {
        config::config_schema()
    }

    async fn create_consumer(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        consumer::create(route_name, value).await
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        publisher::create(route_name, value).await
    }
}

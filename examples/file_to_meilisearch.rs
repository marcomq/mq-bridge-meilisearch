#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const ROUTE_NAME: &str = "file_to_meilisearch";
    mq_bridge_meilisearch::register()?;
    let document: serde_yaml::Value = serde_yaml::from_str(&std::fs::read_to_string(
        "examples/file_to_meilisearch.yaml",
    )?)?;
    let route = document
        .get("routes")
        .and_then(|routes| routes.get(ROUTE_NAME))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("route '{ROUTE_NAME}' is missing"))?;
    let handle = serde_yaml::from_value::<mq_bridge::Route>(route)?
        .run(ROUTE_NAME)
        .await?;
    handle.join().await?;
    Ok(())
}

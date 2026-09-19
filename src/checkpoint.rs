use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context};
use tokio::sync::Mutex;

/// A file-backed scan position: one JSON object mapping `<index>:<cursor_id>`
/// to a value, so several routes can share one file without colliding.
///
/// mq-bridge's own [`checkpoint`](mq_bridge::checkpoint) store is not reused
/// here because it is compiled only when one of the datastore features is on,
/// and a plugin `cdylib` links its own copy of mq-bridge with default features
/// off — a SQL or Mongo backend could never be reached from inside the plugin.
pub(crate) struct FileCheckpoint {
    path: PathBuf,
    key: String,
    write_lock: Mutex<()>,
}

impl FileCheckpoint {
    pub(crate) fn new(path: impl Into<PathBuf>, index: &str, cursor_id: &str) -> Self {
        Self {
            path: path.into(),
            key: format!("{index}:{cursor_id}"),
            write_lock: Mutex::new(()),
        }
    }

    pub(crate) async fn load(&self) -> anyhow::Result<Option<String>> {
        Ok(self.read().await?.remove(&self.key))
    }

    pub(crate) async fn save(&self, value: &str) -> anyhow::Result<()> {
        let _guard = self.write_lock.lock().await;
        let mut entries = self.read().await?;
        entries.insert(self.key.clone(), value.to_owned());
        let encoded = serde_json::to_vec_pretty(&entries)
            .context("failed to encode the Meilisearch checkpoint file")?;

        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create '{}'", parent.display()))?;
        }
        // Written beside the target and renamed over it, so a crash mid-write
        // leaves the previous position rather than a truncated file.
        let temporary = self
            .path
            .with_extension(format!("{}.tmp", std::process::id()));
        tokio::fs::write(&temporary, &encoded)
            .await
            .with_context(|| format!("failed to write '{}'", temporary.display()))?;
        tokio::fs::rename(&temporary, &self.path)
            .await
            .with_context(|| format!("failed to update '{}'", self.path.display()))
    }

    async fn read(&self) -> anyhow::Result<BTreeMap<String, String>> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("'{}' is not a checkpoint file", self.path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(error) => Err(anyhow::Error::new(error)
                .context(format!("failed to read '{}'", self.path.display()))),
        }
    }
}

/// Resolves a `checkpoint_store` spec to the file that holds the position.
/// `file:///abs/path`, `file://relative/path` and a bare path are accepted.
pub(crate) fn parse_spec(spec: &str) -> anyhow::Result<PathBuf> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(anyhow!("Meilisearch `checkpoint_store` is empty"));
    }
    if let Some(rest) = spec.strip_prefix("file://") {
        if rest.is_empty() {
            return Err(anyhow!(
                "Meilisearch `checkpoint_store` '{spec}' names no path"
            ));
        }
        return Ok(PathBuf::from(rest));
    }
    match spec.split_once("://") {
        Some((scheme, _)) => Err(anyhow!(
            "Meilisearch `checkpoint_store` does not support '{scheme}://'; use a `file://` spec or a plain path. \
             Meilisearch stores documents, not cursors, so the position is kept outside it."
        )),
        None => Ok(PathBuf::from(spec)),
    }
}

/// Whether a path is usable before the first save, so a misconfigured route
/// fails at startup instead of on its first commit.
pub(crate) fn check_writable(path: &Path) -> anyhow::Result<()> {
    if path.is_dir() {
        return Err(anyhow!(
            "Meilisearch `checkpoint_store` '{}' is a directory; name the file to write",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_url_or_a_plain_path_both_name_the_same_file() {
        assert_eq!(
            parse_spec("file:///var/lib/mqb/cursors.json").unwrap(),
            PathBuf::from("/var/lib/mqb/cursors.json")
        );
        assert_eq!(
            parse_spec("cursors.json").unwrap(),
            PathBuf::from("cursors.json")
        );
        assert_eq!(
            parse_spec("file://cursors.json").unwrap(),
            PathBuf::from("cursors.json")
        );
    }

    #[test]
    fn an_unsupported_backend_says_so_instead_of_being_read_as_a_path() {
        let error = parse_spec("postgres://localhost/mqb")
            .unwrap_err()
            .to_string();
        assert!(error.contains("postgres://"), "{error}");
        assert!(parse_spec("").is_err());
    }

    #[tokio::test]
    async fn a_position_survives_a_reopen() {
        let directory = std::env::temp_dir().join(format!("mqb-meili-{}", uuid::Uuid::new_v4()));
        let path = directory.join("cursors.json");

        let store = FileCheckpoint::new(&path, "movies", "scan");
        assert_eq!(store.load().await.unwrap(), None);
        store.save("42").await.unwrap();

        let reopened = FileCheckpoint::new(&path, "movies", "scan");
        assert_eq!(reopened.load().await.unwrap().as_deref(), Some("42"));

        tokio::fs::remove_dir_all(&directory).await.ok();
    }

    /// One file can hold several routes' positions, so sharing it is safe.
    #[tokio::test]
    async fn two_indexes_keep_separate_positions_in_one_file() {
        let directory = std::env::temp_dir().join(format!("mqb-meili-{}", uuid::Uuid::new_v4()));
        let path = directory.join("cursors.json");

        FileCheckpoint::new(&path, "movies", "scan")
            .save("10")
            .await
            .unwrap();
        FileCheckpoint::new(&path, "books", "scan")
            .save("20")
            .await
            .unwrap();

        assert_eq!(
            FileCheckpoint::new(&path, "movies", "scan")
                .load()
                .await
                .unwrap()
                .as_deref(),
            Some("10")
        );
        assert_eq!(
            FileCheckpoint::new(&path, "books", "scan")
                .load()
                .await
                .unwrap()
                .as_deref(),
            Some("20")
        );

        tokio::fs::remove_dir_all(&directory).await.ok();
    }
}

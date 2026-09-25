use std::path::{Path, PathBuf};

use anyhow::anyhow;
use mq_bridge::checkpoint::FileCheckpointStore;

/// A file-backed scan position keyed `<index>:<cursor_id>`, so several routes
/// can share one file. Only the file backend: Meilisearch stores documents, not cursors.
pub(crate) fn file_store(
    path: impl Into<PathBuf>,
    index: &str,
    cursor_id: &str,
) -> FileCheckpointStore {
    FileCheckpointStore::new(path, format!("{index}:{cursor_id}"))
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
    use mq_bridge::checkpoint::CheckpointStore;

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

        let store = file_store(&path, "movies", "scan");
        assert_eq!(store.load().await.unwrap(), None);
        store.save("42").await.unwrap();

        let reopened = file_store(&path, "movies", "scan");
        assert_eq!(reopened.load().await.unwrap().as_deref(), Some("42"));

        tokio::fs::remove_dir_all(&directory).await.ok();
    }

    /// One file can hold several routes' positions, so sharing it is safe.
    #[tokio::test]
    async fn two_indexes_keep_separate_positions_in_one_file() {
        let directory = std::env::temp_dir().join(format!("mqb-meili-{}", uuid::Uuid::new_v4()));
        let path = directory.join("cursors.json");

        file_store(&path, "movies", "scan")
            .save("10")
            .await
            .unwrap();
        file_store(&path, "books", "scan").save("20").await.unwrap();

        assert_eq!(
            file_store(&path, "movies", "scan")
                .load()
                .await
                .unwrap()
                .as_deref(),
            Some("10")
        );
        assert_eq!(
            file_store(&path, "books", "scan")
                .load()
                .await
                .unwrap()
                .as_deref(),
            Some("20")
        );

        tokio::fs::remove_dir_all(&directory).await.ok();
    }
}

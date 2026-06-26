//! The engine ties together the content store, the index, and this device's identity.
//!
//! `observe(path)` is the single entry point the watcher calls when a file changes: it
//! stores the content and updates the index, bumping the vector clock only when the content
//! actually changed (so re-scans and sync echoes don't inflate the clock).

use crate::index::{FileRecord, Index};
use crate::store::BlobStore;
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Outcome of observing a path.
#[derive(Debug)]
pub struct Observation {
    pub path: String,
    pub hash: String,
    pub size: u64,
    /// `false` if the content was identical to what we already had indexed.
    pub changed: bool,
}

pub struct Engine {
    /// Synced tree root; index paths are stored relative to it.
    root: PathBuf,
    store: BlobStore,
    index: Index,
    device_id: String,
}

impl Engine {
    /// Open (or create) an engine. Durable state lives under `state_dir`; the synced tree
    /// is `root`. Keep `state_dir` outside (or ignored within) `root` to avoid self-echo.
    pub fn open(root: impl AsRef<Path>, state_dir: impl AsRef<Path>) -> Result<Self> {
        let state_dir = state_dir.as_ref();
        let store = BlobStore::open(state_dir.join("blobs"))?;
        let index = Index::open(state_dir.join("index.sqlite"))?;
        let device_id = index.device_id()?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
            store,
            index,
            device_id,
        })
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn store(&self) -> &BlobStore {
        &self.store
    }

    /// Path relative to the synced root, forward-slashed for cross-OS stability.
    fn rel(&self, abs: &Path) -> String {
        abs.strip_prefix(&self.root)
            .unwrap_or(abs)
            .to_string_lossy()
            .replace('\\', "/")
    }

    /// Store a file's content and update its index entry. Idempotent for unchanged content.
    pub fn observe(&self, abs_path: &Path) -> Result<Observation> {
        let rel = self.rel(abs_path);
        let hash = self.store.put_path(abs_path)?;
        let size = std::fs::metadata(abs_path)?.len();

        let prev = self.index.get(&rel)?;
        let changed = prev.as_ref().map(|p| p.hash != hash).unwrap_or(true);

        if changed {
            let mut vclock = prev.map(|p| p.vclock).unwrap_or_default();
            vclock.increment(&self.device_id);
            self.index.upsert(&FileRecord {
                path: rel.clone(),
                hash: hash.clone(),
                size,
                vclock,
                updated_ms: now_ms(),
            })?;
        }

        Ok(Observation {
            path: rel,
            hash,
            size,
            changed,
        })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

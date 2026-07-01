//! The engine ties together the content store, the index, and this device's identity.
//!
//! `observe(path)` is the single entry point the watcher calls when a file changes: it
//! stores the content and updates the index, bumping the vector clock only when the content
//! actually changed (so re-scans and sync echoes don't inflate the clock).

use crate::index::{FileRecord, Index};
use crate::store::BlobStore;
use crate::vclock::Causality;
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

/// What a peer's record means relative to our local state (drives the sync pull).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SyncAction {
    /// We already have this (same content, or our clock is newer/equal).
    Skip,
    /// The peer's version causally supersedes ours — fetch and apply it.
    Fetch,
    /// Concurrent edits on both sides — a true conflict, resolved by `apply_incoming`.
    Conflict,
}

/// Result of applying an incoming record (for sync stats / logging).
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// We already had it; nothing changed.
    Skipped,
    /// The peer's version superseded ours and was applied (materialized or deleted).
    Applied,
    /// Delete-vs-edit conflict; our edit was kept (the edit wins over a delete).
    ConflictKeptLocal,
    /// Edit-vs-edit conflict; both kept — the loser is at `copy`.
    Conflicted { copy: String },
}

pub struct Engine {
    /// Synced tree root; index paths are stored relative to it.
    root: PathBuf,
    /// Where losing versions of conflicts are preserved (`<state>/conflicts`), mirroring the
    /// original path. Outside the synced tree, so they don't clutter it or propagate.
    conflicts_dir: PathBuf,
    store: BlobStore,
    index: Index,
    device_id: String,
    /// Serializes the evaluate→apply critical section so concurrent applies of the same path
    /// (streams are handled in parallel) can't race into a spurious conflict. `apply_incoming`
    /// is fully synchronous, so this is only ever held briefly and never across network I/O.
    apply_lock: std::sync::Mutex<()>,
}

impl Engine {
    /// Open (or create) an engine. Durable state lives under `state_dir`; the synced tree
    /// is `root`. Keep `state_dir` outside (or ignored within) `root` to avoid self-echo.
    pub fn open(root: impl AsRef<Path>, state_dir: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let state_dir = state_dir.as_ref();
        let store = BlobStore::open(state_dir.join("blobs"))?;
        let index = Index::open(state_dir.join("index.sqlite"))?;
        let device_id = index.device_id()?;

        ignore_state_in_git(&root, state_dir); // best-effort; never blocks open

        Ok(Self {
            root,
            conflicts_dir: state_dir.join("conflicts"),
            store,
            index,
            device_id,
            apply_lock: std::sync::Mutex::new(()),
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
                deleted: false,
            })?;
        }

        Ok(Observation {
            path: rel,
            hash,
            size,
            changed,
        })
    }

    /// Snapshot of every indexed file — what we advertise to a peer.
    pub fn local_records(&self) -> Result<Vec<FileRecord>> {
        self.index.all()
    }

    /// Number of live (non-deleted) files, counted cheaply (no full-row load).
    pub fn file_count(&self) -> Result<usize> {
        self.index.count_live()
    }

    /// Decide what to do with a peer's record, purely from vector clocks (no wall-clock).
    pub fn evaluate(&self, remote: &FileRecord) -> Result<SyncAction> {
        match self.index.get(&remote.path)? {
            None => Ok(SyncAction::Fetch), // never seen this path
            Some(local) if local.hash == remote.hash => Ok(SyncAction::Skip), // identical
            Some(local) => Ok(match remote.vclock.compare(&local.vclock) {
                Causality::After => SyncAction::Fetch,
                Causality::Before | Causality::Equal => SyncAction::Skip,
                Causality::Concurrent => SyncAction::Conflict,
            }),
        }
    }

    /// Record a local deletion as a tombstone (bumping our clock) so it propagates to peers.
    /// Returns the tombstone to push, or `None` if the path wasn't a tracked live file.
    pub fn observe_delete(&self, abs_path: &Path) -> Result<Option<FileRecord>> {
        let rel = self.rel(abs_path);
        match self.index.get(&rel)? {
            Some(prev) if !prev.deleted => {
                let mut vclock = prev.vclock;
                vclock.increment(&self.device_id);
                let tomb = FileRecord {
                    path: rel,
                    hash: String::new(),
                    size: 0,
                    vclock,
                    updated_ms: now_ms(),
                    deleted: true,
                };
                self.index.upsert(&tomb)?;
                Ok(Some(tomb))
            }
            _ => Ok(None), // unknown path or already a tombstone
        }
    }

    /// Apply an incoming peer record with full conflict handling. The single entry point for
    /// both pulls and live pushes. For non-tombstones, the record's content (manifest + chunks)
    /// must already be in the store — the transport fetches any missing chunks first.
    pub fn apply_incoming(&self, remote: &FileRecord) -> Result<ApplyOutcome> {
        // Trust boundary: a peer's path must stay inside the synced tree. Reject absolute paths,
        // `..`, and drive prefixes before any of it reaches the filesystem (materialize/delete).
        ensure_safe_rel(&remote.path)?;
        // Serialize the evaluate→apply sequence against concurrent applies (parallel streams).
        let _apply = self.apply_lock.lock().unwrap();
        match self.evaluate(remote)? {
            SyncAction::Skip => Ok(ApplyOutcome::Skipped),
            SyncAction::Fetch => {
                self.apply_remote(remote)?;
                Ok(ApplyOutcome::Applied)
            }
            SyncAction::Conflict => self.resolve_conflict(remote),
        }
    }

    /// Apply a record that causally supersedes ours: delete (tombstone) or materialize from the
    /// store, then record the merged clock. The watcher's later `observe()` is a no-op
    /// (content-addressed), which suppresses echo loops.
    pub fn apply_remote(&self, remote: &FileRecord) -> Result<()> {
        let mut vclock = remote.vclock.clone();
        if let Some(local) = self.index.get(&remote.path)? {
            vclock.merge(&local.vclock); // dominate both sides
        }
        let abs = self.root.join(&remote.path);

        if remote.deleted {
            if abs.exists() {
                let _ = std::fs::remove_file(&abs);
            }
            self.index.upsert(&FileRecord {
                path: remote.path.clone(),
                hash: String::new(),
                size: 0,
                vclock,
                updated_ms: now_ms(),
                deleted: true,
            })?;
            return Ok(());
        }

        anyhow::ensure!(
            self.store.has(&remote.hash),
            "content for {} ({}) not in store",
            remote.path,
            remote.hash
        );
        self.store.materialize(&remote.hash, &abs)?;
        self.index.upsert(&FileRecord {
            path: remote.path.clone(),
            hash: remote.hash.clone(),
            size: remote.size,
            vclock,
            updated_ms: now_ms(),
            deleted: false,
        })?;
        Ok(())
    }

    /// Resolve a concurrent change. Delete-vs-edit: the edit wins (no data lost). Edit-vs-edit:
    /// keep both — one content wins the path (deterministically, by greater hash), the other is
    /// preserved under `.codrop/conflicts/<path>`. Both peers compute the same outcome, so they
    /// converge without duplicating. Both versions' content must already be in the store.
    fn resolve_conflict(&self, remote: &FileRecord) -> Result<ApplyOutcome> {
        let local = match self.index.get(&remote.path)? {
            Some(l) => l,
            None => {
                self.apply_remote(remote)?;
                return Ok(ApplyOutcome::Applied);
            }
        };
        let mut merged = remote.vclock.clone();
        merged.merge(&local.vclock);

        // delete-vs-edit → the edit wins.
        if local.deleted != remote.deleted {
            if remote.deleted {
                // our edit survives; absorb their clock so it stops re-conflicting.
                self.index.upsert(&FileRecord {
                    vclock: merged,
                    updated_ms: now_ms(),
                    ..local
                })?;
                return Ok(ApplyOutcome::ConflictKeptLocal);
            }
            // their edit resurrects the file over our delete.
            self.apply_remote(remote)?;
            return Ok(ApplyOutcome::Applied);
        }

        // both tombstones (equal hashes would already be Skip) → just merge clocks.
        if local.deleted && remote.deleted {
            self.index.upsert(&FileRecord {
                vclock: merged,
                updated_ms: now_ms(),
                ..local
            })?;
            return Ok(ApplyOutcome::Skipped);
        }

        // both edits, different content → keep both.
        anyhow::ensure!(
            self.store.has(&remote.hash),
            "conflict content for {} not in store",
            remote.path
        );
        let (winner, winner_size, loser) = if remote.hash > local.hash {
            (remote.hash.clone(), remote.size, local.hash.clone())
        } else {
            (local.hash.clone(), local.size, remote.hash.clone())
        };

        // winner takes the canonical path with the merged clock.
        self.store.materialize(&winner, &self.root.join(&remote.path))?;
        self.index.upsert(&FileRecord {
            path: remote.path.clone(),
            hash: winner,
            size: winner_size,
            vclock: merged,
            updated_ms: now_ms(),
            deleted: false,
        })?;

        // loser → .codrop/conflicts/<path> (not indexed), preserving folder structure and name.
        let conflict_path = self.conflicts_dir.join(&remote.path);
        self.store.materialize(&loser, &conflict_path)?;

        Ok(ApplyOutcome::Conflicted {
            copy: conflict_path.to_string_lossy().into_owned(),
        })
    }
}

/// Ensure git ignores `state_dir` when it lives inside the synced `root` — i.e. add e.g.
/// `.codrop/` to `<root>/.gitignore`. Idempotent and best-effort (errors are swallowed). Shared
/// by `Engine::open` and the daemon's `id` command so both keep `.codrop` out of git.
pub fn ignore_state_in_git(root: &Path, state_dir: &Path) {
    if let Ok(rel) = state_dir.strip_prefix(root) {
        let entry = format!("{}/", rel.to_string_lossy().replace('\\', "/"));
        let _ = ensure_gitignore(root, &entry);
    }
}

/// Ensure `<root>/.gitignore` contains `entry` (e.g. `.codrop/`). Idempotent: matches an
/// existing line whether or not it has a trailing slash, and appends with clean newlines.
fn ensure_gitignore(root: &Path, entry: &str) -> Result<()> {
    let path = root.join(".gitignore");
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let needle = entry.trim_end_matches('/');
    if existing
        .lines()
        .any(|l| l.trim().trim_end_matches('/') == needle)
    {
        return Ok(());
    }
    let mut content = existing;
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(entry);
    content.push('\n');
    std::fs::write(&path, content)?;
    Ok(())
}

/// Reject a peer-supplied relative path that would escape the synced root — absolute paths,
/// `..` traversal, or a drive/root prefix. Only plain path segments (and `.`) are allowed.
fn ensure_safe_rel(rel: &str) -> Result<()> {
    use std::path::Component;
    anyhow::ensure!(!rel.is_empty(), "empty path from peer");
    for comp in Path::new(rel).components() {
        match comp {
            Component::Normal(_) | Component::CurDir => {}
            _ => anyhow::bail!("unsafe path from peer: {rel:?}"),
        }
    }
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

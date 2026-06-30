//! SQLite-backed index of the synced tree: `path -> content hash + vector clock`.
//!
//! This is the durable metadata layer. The "hollow tree" the VFS will project (Phase 5)
//! is exactly this index; file *content* lives in the [`crate::store::BlobStore`].

use crate::vclock::VClock;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// One row of the local file index. Also the wire shape exchanged with peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    /// Path relative to the synced root, forward-slashed (stable across OSes).
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub vclock: VClock,
    pub updated_ms: i64,
    /// Tombstone: the file was deleted. The row (with its vclock) is kept so the deletion
    /// propagates to peers and can be ordered against concurrent edits.
    #[serde(default)]
    pub deleted: bool,
}

/// The connection is wrapped in a `Mutex` so `Index` (and therefore `Engine`) is `Sync`,
/// which the async QUIC server needs to share it across tasks. Locks are never held across
/// an `.await`, so this can't deadlock the runtime.
pub struct Index {
    conn: Mutex<Connection>,
}

impl Index {
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                 path       TEXT PRIMARY KEY,
                 hash       TEXT NOT NULL,
                 size       INTEGER NOT NULL,
                 vclock     TEXT NOT NULL,
                 updated_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS meta (
                 key   TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );",
        )?;
        // Migration: add the tombstone column to indexes created before deletes existed.
        // Errors (e.g. "duplicate column") are expected on already-migrated DBs — ignore them.
        let _ = conn.execute(
            "ALTER TABLE files ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0",
            [],
        );
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Stable identifier for this device, generated and persisted on first use.
    pub fn device_id(&self) -> Result<String> {
        if let Some(existing) = self.meta_get("device_id")? {
            return Ok(existing);
        }
        let id = new_device_id();
        self.meta_set("device_id", &id)?;
        Ok(id)
    }

    pub fn get(&self, path: &str) -> Result<Option<FileRecord>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT path,hash,size,vclock,updated_ms,deleted FROM files WHERE path=?1",
                [path],
                row_to_tuple,
            )
            .optional()?;
        row.map(tuple_to_record).transpose()
    }

    pub fn upsert(&self, rec: &FileRecord) -> Result<()> {
        let vclock = serde_json::to_string(&rec.vclock)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO files(path,hash,size,vclock,updated_ms,deleted) VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(path) DO UPDATE SET
                 hash=excluded.hash, size=excluded.size,
                 vclock=excluded.vclock, updated_ms=excluded.updated_ms,
                 deleted=excluded.deleted",
            (
                &rec.path,
                &rec.hash,
                rec.size as i64,
                vclock,
                rec.updated_ms,
                rec.deleted as i64,
            ),
        )?;
        Ok(())
    }

    pub fn all(&self) -> Result<Vec<FileRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT path,hash,size,vclock,updated_ms,deleted FROM files ORDER BY path")?;
        let rows = stmt.query_map([], row_to_tuple)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(tuple_to_record(row?)?);
        }
        Ok(out)
    }

    fn meta_get(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
            .optional()?)
    }

    fn meta_set(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )?;
        Ok(())
    }
}

type Row = (String, String, i64, String, i64, i64);

fn row_to_tuple(r: &rusqlite::Row) -> rusqlite::Result<Row> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
    ))
}

fn tuple_to_record((path, hash, size, vclock, updated_ms, deleted): Row) -> Result<FileRecord> {
    Ok(FileRecord {
        path,
        hash,
        size: size as u64,
        vclock: serde_json::from_str(&vclock)?,
        updated_ms,
        deleted: deleted != 0,
    })
}

/// A stable per-install id derived from process id + nanos, hashed for compactness.
fn new_device_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seed = format!("{nanos}-{}", std::process::id());
    blake3::hash(seed.as_bytes()).to_hex()[..16].to_string()
}

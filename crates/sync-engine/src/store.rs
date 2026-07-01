//! Content-addressed, **chunked** blob store.
//!
//! Content is split into content-defined chunks (FastCDC) that are each stored under their own
//! BLAKE3 digest in `objects/`. A file's full content is represented by a *manifest* — the
//! ordered list of its chunk hashes — stored in `manifests/` under the full-content hash.
//!
//! This gives two wins the sync layer exploits: identical chunks are stored once (dedup across
//! files and across versions of a file), and a peer only needs to transfer the chunks it's
//! missing — so a small edit to a large file moves just the changed chunk(s).
//!
//! The public whole-file API (`put_bytes`/`read`/`materialize`, keyed by full-content hash) is
//! unchanged, so callers that don't care about chunking are unaffected. `has_chunk` /
//! `read_chunk` / `put_chunk` / `get_manifest` / `put_manifest` expose the chunk level to the
//! transport.

use anyhow::{anyhow, Context, Result};
use fastcdc::v2020::FastCDC;
use std::fs;
use std::path::{Path, PathBuf};

// Content-defined chunking targets (bytes): most source files land in a single chunk; large
// files split so an edit only rewrites nearby chunks.
const CHUNK_MIN: u32 = 2 * 1024;
const CHUNK_AVG: u32 = 8 * 1024;
const CHUNK_MAX: u32 = 64 * 1024;

pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))?;
        fs::create_dir_all(root.join("manifests"))?;
        Ok(Self { root })
    }

    // ---- paths ------------------------------------------------------------------------------

    /// `<kind>/<first-2-hex>/<rest>` — sharded to avoid one giant flat directory.
    fn shard_path(&self, kind: &str, hash: &str) -> PathBuf {
        if hash.len() < 2 {
            return self.root.join(kind).join("__invalid__").join(hash);
        }
        let (prefix, rest) = hash.split_at(2);
        self.root.join(kind).join(prefix).join(rest)
    }

    fn object_path(&self, hash: &str) -> PathBuf {
        self.shard_path("objects", hash)
    }

    fn manifest_path(&self, hash: &str) -> PathBuf {
        self.shard_path("manifests", hash)
    }

    // ---- chunk level (used by the transport) ------------------------------------------------

    pub fn has_chunk(&self, hash: &str) -> bool {
        self.object_path(hash).exists()
    }

    pub fn read_chunk(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.object_path(hash);
        Ok(if path.exists() {
            Some(fs::read(path)?)
        } else {
            None
        })
    }

    /// Store a chunk after verifying its hash matches (a peer could send anything).
    pub fn put_chunk(&self, hash: &str, bytes: &[u8]) -> Result<()> {
        let actual = blake3::hash(bytes).to_hex().to_string();
        anyhow::ensure!(actual == hash, "chunk hash mismatch (got {actual}, expected {hash})");
        self.write_object(hash, bytes)
    }

    /// The chunk manifest for a full-content hash, if we have it.
    pub fn get_manifest(&self, full_hash: &str) -> Result<Option<Vec<String>>> {
        let path = self.manifest_path(full_hash);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(path)?;
        Ok(Some(
            text.lines().filter(|l| !l.is_empty()).map(String::from).collect(),
        ))
    }

    /// Store a manifest (list of chunk hashes) for a full-content hash.
    pub fn put_manifest(&self, full_hash: &str, chunks: &[String]) -> Result<()> {
        let path = self.manifest_path(full_hash);
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let body = chunks.join("\n");
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, body)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    // ---- whole-file level (unchanged API, now chunk-backed) ---------------------------------

    /// Do we have the full content (manifest present and every chunk it lists)?
    pub fn has(&self, full_hash: &str) -> bool {
        match self.get_manifest(full_hash) {
            Ok(Some(chunks)) => chunks.iter().all(|c| self.has_chunk(c)),
            _ => false,
        }
    }

    /// Chunk `bytes`, store the chunks + manifest, and return the full-content hash. Idempotent.
    pub fn put_bytes(&self, bytes: &[u8]) -> Result<String> {
        let full = blake3::hash(bytes).to_hex().to_string();
        if self.has(&full) {
            return Ok(full);
        }
        let mut chunk_hashes = Vec::new();
        if !bytes.is_empty() {
            for chunk in FastCDC::new(bytes, CHUNK_MIN, CHUNK_AVG, CHUNK_MAX) {
                let slice = &bytes[chunk.offset..chunk.offset + chunk.length];
                let h = blake3::hash(slice).to_hex().to_string();
                if !self.has_chunk(&h) {
                    self.write_object(&h, slice)?;
                }
                chunk_hashes.push(h);
            }
        }
        self.put_manifest(&full, &chunk_hashes)?;
        Ok(full)
    }

    /// Read a file and store its content. Returns the full-content hash.
    pub fn put_path(&self, src: &Path) -> Result<String> {
        let bytes = fs::read(src).with_context(|| format!("reading {}", src.display()))?;
        self.put_bytes(&bytes)
    }

    /// Reassemble full content from its chunks, if we have all of them.
    pub fn read(&self, full_hash: &str) -> Result<Option<Vec<u8>>> {
        let Some(chunks) = self.get_manifest(full_hash)? else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for c in &chunks {
            match self.read_chunk(c)? {
                Some(bytes) => out.extend_from_slice(&bytes),
                None => return Ok(None), // missing a chunk → content not fully available
            }
        }
        Ok(Some(out))
    }

    /// Materialize full content to `dest` (reassembled from chunks, written atomically).
    pub fn materialize(&self, full_hash: &str, dest: &Path) -> Result<()> {
        let bytes = self
            .read(full_hash)?
            .ok_or_else(|| anyhow!("content {full_hash} not fully in store"))?;
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_extension("codrop-tmp");
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, dest)?;
        Ok(())
    }

    // ---- internals --------------------------------------------------------------------------

    /// Atomically write a content object (chunk) under `hash`, if not already present.
    fn write_object(&self, hash: &str, bytes: &[u8]) -> Result<()> {
        let dst = self.object_path(hash);
        if dst.exists() {
            return Ok(());
        }
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = dst.with_extension("tmp");
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, &dst)?;
        Ok(())
    }
}

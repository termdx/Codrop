//! Content-addressed blob store.
//!
//! Objects are keyed by their BLAKE3 hex digest, so identical content is stored once
//! (natural dedup) and integrity is verifiable. Materialization uses APFS `clonefile()`
//! copy-on-write where available — the real lever against the APFS small-file bottleneck
//! (the cost is inode/`fsync` overhead, not bytes; a CoW clone sidesteps both).

use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))?;
        Ok(Self { root })
    }

    /// `objects/<first-2-hex>/<rest>` — sharded to avoid one giant flat directory.
    /// Tombstones carry an empty hash and never touch the store, but guard anyway.
    fn object_path(&self, hash: &str) -> PathBuf {
        if hash.len() < 2 {
            return self.root.join("objects").join("__invalid__").join(hash);
        }
        let (prefix, rest) = hash.split_at(2);
        self.root.join("objects").join(prefix).join(rest)
    }

    pub fn has(&self, hash: &str) -> bool {
        self.object_path(hash).exists()
    }

    /// Store raw bytes under their BLAKE3 hash. Returns the hex digest. Idempotent.
    pub fn put_bytes(&self, bytes: &[u8]) -> Result<String> {
        let hash = blake3::hash(bytes).to_hex().to_string();
        let dst = self.object_path(&hash);
        if !dst.exists() {
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            // Write atomically (temp + rename) so a crash never leaves a partial object.
            let tmp = dst.with_extension("tmp");
            fs::write(&tmp, bytes)?;
            fs::rename(&tmp, &dst)?;
        }
        Ok(hash)
    }

    /// Read a file and store its bytes. Returns the hex digest.
    pub fn put_path(&self, src: &Path) -> Result<String> {
        let bytes = fs::read(src).with_context(|| format!("reading {}", src.display()))?;
        self.put_bytes(&bytes)
    }

    /// Read a stored blob's bytes, if present (the wire-serve path for peers).
    pub fn read(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.object_path(hash);
        if path.exists() {
            Ok(Some(fs::read(path)?))
        } else {
            Ok(None)
        }
    }

    /// Materialize a stored blob to `dest`, preferring a copy-on-write clone.
    pub fn materialize(&self, hash: &str, dest: &Path) -> Result<()> {
        let src = self.object_path(hash);
        anyhow::ensure!(src.exists(), "blob {hash} not in store");
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        // clonefile() requires the destination not to exist.
        if dest.exists() {
            fs::remove_file(dest)?;
        }
        if reflink(&src, dest).is_err() {
            fs::copy(&src, dest)?;
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn reflink(src: &Path, dst: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let s = CString::new(src.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "nul in path"))?;
    let d = CString::new(dst.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "nul in path"))?;
    // clonefile(2): instant copy-on-write clone on APFS.
    let rc = unsafe { libc::clonefile(s.as_ptr(), d.as_ptr(), 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "macos"))]
fn reflink(_src: &Path, _dst: &Path) -> std::io::Result<()> {
    // No reflink syscall wired up off macOS yet; force the copy fallback.
    // (Linux `FICLONE` ioctl can slot in here later.)
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "no reflink on this platform",
    ))
}

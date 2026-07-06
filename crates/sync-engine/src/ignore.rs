//! Ignore matching: which paths Codrop refuses to sync.
//!
//! Two layers, merged into one gitignore-style matcher:
//!   1. [`IGNORE_DIRS`] — the built-in defaults (`.codrop`, `node_modules`, `.git`, …). These
//!      are always applied; `.codrop` in particular *must* stay ignored or the daemon would
//!      echo its own `status.json` writes forever.
//!   2. `<root>/.codropignore` — an optional, user-editable file at the synced root using
//!      gitignore syntax (`*.log`, `.venv/`, `__pycache__`, `!keep.log`, …). Because it's a
//!      normal tree file, it syncs to peers like `.gitignore` does.
//!
//! Matching is **sender-side**: the daemon consults this before scanning or pushing, so ignored
//! paths never leave a machine. It does not filter *incoming* pushes — a peer that hasn't yet
//! received the synced `.codropignore` may still send a file, which converges away once the
//! ignore file reaches it.

use crate::IGNORE_DIRS;
use anyhow::Result;
use ::ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::path::Path;

/// Name of the user-editable ignore file at the synced root.
pub const IGNORE_FILE: &str = ".codropignore";

/// Turn a user-supplied `codrop ignore` argument into the pattern to store. If it names a path
/// under `root` (e.g. a tab-completed absolute path, or `root/sub/x.log`), rewrite it to the
/// root-relative form the ignore file expects; otherwise keep it verbatim (globs like `*.log`,
/// bare names like `__pycache__`, and negations like `!keep.log` pass through unchanged).
pub fn normalize_pattern(root: &Path, arg: &str) -> String {
    let candidate = Path::new(arg);
    let abs = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        root.join(candidate)
    };
    // Only rewrite when it genuinely lands under root; canonicalize the root prefix so an
    // absolute arg and a canonical root compare in the same form.
    if let Ok(rel) = abs.strip_prefix(root) {
        let rel = rel.to_string_lossy().replace('\\', "/");
        if !rel.is_empty() && rel != "." {
            return rel;
        }
    }
    arg.to_string()
}

/// Append `pattern` to `<root>/.codropignore` (creating the file). Idempotent: returns `false`
/// without writing if an equivalent line already exists. Mirrors the engine's `.gitignore`
/// upkeep so the file stays clean (one entry per line, trailing newline).
pub fn append_ignore(root: &Path, pattern: &str) -> Result<bool> {
    let path = root.join(IGNORE_FILE);
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let needle = pattern.trim();
    if existing.lines().any(|l| l.trim() == needle) {
        return Ok(false);
    }
    let mut content = existing;
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(needle);
    content.push('\n');
    std::fs::write(&path, content)?;
    Ok(true)
}

/// A compiled ignore policy for one synced root: built-in defaults + `<root>/.codropignore`.
pub struct Matcher {
    inner: Gitignore,
}

impl Matcher {
    /// Build the matcher for `root` (should be canonical — the same form paths are queried in).
    /// Seeds the built-in [`IGNORE_DIRS`], then layers `<root>/.codropignore` on top so user
    /// patterns (including `!` un-ignores) can override the defaults. Never fails: a malformed
    /// `.codropignore` falls back to just the built-ins.
    pub fn load(root: &Path) -> Self {
        let mut builder = GitignoreBuilder::new(root);
        for dir in IGNORE_DIRS {
            // Trailing slash isn't used: these names should match a file or dir anywhere, and a
            // dir match prunes its whole subtree via `matched_path_or_any_parents`.
            let _ = builder.add_line(None, dir);
        }
        // `add` reads the file if present; a returned error means a bad pattern — ignore it and
        // keep the good lines the builder already parsed.
        let _ = builder.add(root.join(IGNORE_FILE));
        let inner = builder.build().unwrap_or_else(|_| Gitignore::empty());
        Matcher { inner }
    }

    /// True if `abs` (an absolute path under the matcher's root) is ignored. Checks the path and
    /// all its parents, so ignoring a directory ignores everything beneath it. `is_dir` selects
    /// dir-only patterns (a trailing-slash pattern matches directories only).
    pub fn is_ignored(&self, abs: &Path, is_dir: bool) -> bool {
        self.inner
            .matched_path_or_any_parents(abs, is_dir)
            .is_ignore()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn built_in_dirs_are_ignored() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let m = Matcher::load(root);
        assert!(m.is_ignored(&root.join(".codrop"), true));
        assert!(m.is_ignored(&root.join(".codrop/status.json"), false));
        assert!(m.is_ignored(&root.join("node_modules/foo/index.js"), false));
        assert!(m.is_ignored(&root.join(".git/HEAD"), false));
        assert!(!m.is_ignored(&root.join("src/main.rs"), false));
    }

    #[test]
    fn codropignore_patterns_apply() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join(IGNORE_FILE), "*.log\n.venv/\nsecret.env\n").unwrap();
        let m = Matcher::load(root);
        assert!(m.is_ignored(&root.join("run.log"), false));
        assert!(m.is_ignored(&root.join(".venv"), true));
        assert!(m.is_ignored(&root.join(".venv/lib/x.py"), false));
        assert!(m.is_ignored(&root.join("secret.env"), false));
        assert!(!m.is_ignored(&root.join("keep.txt"), false));
    }

    #[test]
    fn negation_can_unignore() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join(IGNORE_FILE), "*.log\n!keep.log\n").unwrap();
        let m = Matcher::load(root);
        assert!(m.is_ignored(&root.join("run.log"), false));
        assert!(!m.is_ignored(&root.join("keep.log"), false));
    }
}

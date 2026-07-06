//! Codrop sync engine (Phase 1).
//!
//! The local state layer beneath the watcher: a content-addressed blob store, a SQLite
//! index mapping `path -> content hash + vector clock`, and copy-on-write materialization.
//! This is what later phases (transport, conflict resolution, VFS) build on.

pub mod engine;
pub mod ignore;
pub mod index;
pub mod store;
pub mod vclock;

/// Directory names Codrop never syncs — OS/toolchain-specific or Codrop's own state. Defined
/// once here so every entry point (daemon, watcher, one-shot CLI) shares one ignore policy
/// instead of drifting apart.
pub const IGNORE_DIRS: &[&str] = &[
    ".codrop",
    "node_modules",
    ".git",
    "target",
    "dist",
    "build",
    ".next",
];

pub use engine::{ignore_state_in_git, ApplyOutcome, Engine, Observation, SyncAction};
pub use ignore::{append_ignore, normalize_pattern, Matcher, IGNORE_FILE};
pub use index::{FileRecord, Index};
pub use store::BlobStore;
pub use vclock::{Causality, VClock};

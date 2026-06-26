//! Codrop sync engine (Phase 1).
//!
//! The local state layer beneath the watcher: a content-addressed blob store, a SQLite
//! index mapping `path -> content hash + vector clock`, and copy-on-write materialization.
//! This is what later phases (transport, conflict resolution, VFS) build on.

pub mod engine;
pub mod index;
pub mod store;
pub mod vclock;

pub use engine::{Engine, Observation, SyncAction};
pub use index::{FileRecord, Index};
pub use store::BlobStore;
pub use vclock::{Causality, VClock};

//! Codrop transport (Phase 2).
//!
//! Peer-to-peer sync over QUIC (`quinn`) with mDNS discovery (`mdns-sd`). A node serves its
//! local index and blobs; a peer pulls, comparing vector clocks to decide what to fetch and
//! applying it through the engine. Content-addressing makes apply idempotent, so re-syncs and
//! watcher echoes don't loop.
//!
//! P2P-LAN-first (per the plan): peers find each other on the LAN and connect directly.

pub mod discovery;
pub mod node;
pub mod proto;
mod tls;

pub use node::{pull, serve, SyncStats};

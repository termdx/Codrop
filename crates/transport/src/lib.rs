//! Codrop transport (Phase 2, on iroh).
//!
//! Peer-to-peer sync over **iroh** — QUIC with hole-punching and relay fallback, peers
//! addressed by public-key `EndpointId`. A node serves its index/blobs; a peer pulls,
//! comparing vector clocks to decide what to fetch and applying it through the engine.
//! Content-addressing makes apply idempotent, so re-syncs and watcher echoes don't loop.
//!
//! iroh gives us the tiered connectivity (direct → hole-punch → relay) that raw direct-QUIC
//! lacked — including past the Wi-Fi client-isolation that broke our earlier LAN test.

pub mod node;
pub mod proto;

pub use node::{
    connect, crypto_provider, endpoint_with_key, load_or_create_key, pull, pull_on, pull_over,
    push, serve, serve_on, SyncStats, ALPN,
};

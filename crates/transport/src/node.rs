//! Sync node over **iroh** — p2p QUIC with hole-punching + relay fallback.
//!
//! Peers are addressed by their `EndpointId` (an Ed25519 public key), not `IP:port`. iroh's
//! discovery + relay resolve that id to a working path: direct on the LAN, hole-punched across
//! NATs, or relayed when neither works (e.g. the Wi-Fi AP-isolation case that blocked our raw
//! QUIC transport). The endpoint key doubles as device identity — no skip-verify TLS.
//!
//! One connection carries both **pulls** (Index/Blob, for catch-up) and **pushes** (live
//! changes). The sync protocol and the engine are unchanged; only the transport swapped.

use crate::proto::{read_msg, write_msg, Req, Resp};
use anyhow::{bail, Result};
use codrop_sync_engine::{ApplyOutcome, Engine, FileRecord, SyncAction};
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use std::path::Path;
use std::sync::Arc;

/// ALPN identifying the Codrop sync protocol on an iroh connection.
pub const ALPN: &[u8] = b"codrop/sync/0";

/// The rustls crypto provider (ring) iroh's TLS requires. iroh's builder needs this passed
/// explicitly unless its `tls-ring` cfg happens to be active, so we always supply it.
/// Callers building endpoints directly (e.g. tests) pass this to `Endpoint::builder`.
pub fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// What a `pull` did, for logging/tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    pub total: usize,
    pub fetched: usize,
    pub skipped: usize,
    pub conflicts: usize,
}

/// Load a persisted device key from `path`, or generate one and save it. A stable key means a
/// stable `EndpointId` (device identity) across restarts.
pub fn load_or_create_key(path: &Path) -> Result<SecretKey> {
    if let Ok(bytes) = std::fs::read(path) {
        if let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice()) {
            return Ok(SecretKey::from_bytes(&arr));
        }
    }
    let key = SecretKey::generate();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, key.to_bytes())?;
    Ok(key)
}

/// Build a Codrop endpoint with full connectivity (N0: discovery + relay) and the given key.
pub async fn endpoint_with_key(secret_key: SecretKey) -> Result<Endpoint> {
    Ok(Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .crypto_provider(crypto_provider())
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?)
}

/// Build a Codrop endpoint with an ephemeral key (full connectivity).
async fn build_endpoint(preset: impl presets::Preset) -> Result<Endpoint> {
    Ok(Endpoint::builder(preset)
        .crypto_provider(crypto_provider())
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?)
}

/// Start serving `engine` on a freshly-bound endpoint (full connectivity). Returns the
/// endpoint — read `.id()` to share with peers, and keep it alive to stay reachable.
pub async fn serve(engine: Arc<Engine>) -> Result<Endpoint> {
    let endpoint = build_endpoint(presets::N0).await?;
    serve_on(engine, &endpoint);
    Ok(endpoint)
}

/// Spawn the accept loop for `engine` on an existing endpoint (used by `serve`, the daemon,
/// and tests). Handles both pulls and live pushes from connecting peers.
pub fn serve_on(engine: Arc<Engine>, endpoint: &Endpoint) {
    let endpoint = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let engine = engine.clone();
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    let peer = conn.remote_id();
                    eprintln!("peer connected: {}", peer.fmt_short());
                    let _ = serve_connection(engine, conn).await;
                }
            });
        }
    });
}

/// Serve requests (Index/Blob/Push) on an established connection until the peer closes it.
/// Public so callers (the daemon) can run their own accept loop and register connections.
pub async fn serve_connection(engine: Arc<Engine>, conn: Connection) -> Result<()> {
    // One bidirectional stream per request; loop until the peer closes the connection.
    loop {
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(_) => break, // peer closed — normal end of session
        };
        let resp = match read_msg::<Req>(&mut recv).await? {
            Req::Index => Resp::Index {
                records: engine.local_records()?,
            },
            Req::Blob { hash } => match engine.store().read(&hash)? {
                Some(bytes) => Resp::Blob { bytes },
                None => Resp::NotFound,
            },
            Req::Push { record, bytes } => {
                let outcome = engine.apply_incoming(&record, &bytes)?;
                if outcome != ApplyOutcome::Skipped {
                    eprintln!("push {}: {outcome:?}", record.path);
                }
                Resp::Ok
            }
        };
        write_msg(&mut send, &resp).await?;
        send.finish()?;
    }
    Ok(())
}

/// Open a connection to `peer` on an existing endpoint (reused for pulls and pushes).
pub async fn connect(endpoint: &Endpoint, peer: impl Into<EndpointAddr>) -> Result<Connection> {
    Ok(endpoint.connect(peer, ALPN).await?)
}

/// Push one changed file to a peer over an existing connection (live sync).
pub async fn push(conn: &Connection, record: &FileRecord, bytes: &[u8]) -> Result<()> {
    let req = Req::Push {
        record: record.clone(),
        bytes: bytes.to_vec(),
    };
    match request(conn, &req).await? {
        Resp::Ok => Ok(()),
        _ => bail!("unexpected response to Push"),
    }
}

/// Connect to `peer` on a fresh endpoint and pull (one-shot CLI path).
pub async fn pull(engine: &Engine, peer: impl Into<EndpointAddr>) -> Result<SyncStats> {
    let endpoint = build_endpoint(presets::N0).await?;
    let stats = pull_on(engine, &endpoint, peer).await?;
    endpoint.close().await;
    Ok(stats)
}

/// Pull from `peer` using an existing endpoint, on a fresh connection.
pub async fn pull_on(
    engine: &Engine,
    endpoint: &Endpoint,
    peer: impl Into<EndpointAddr>,
) -> Result<SyncStats> {
    let conn = connect(endpoint, peer).await?;
    let stats = pull_over(engine, &conn).await?;
    conn.close(0u32.into(), b"done");
    Ok(stats)
}

/// Pull over an already-open connection: fetch the peer's index and apply what supersedes us.
pub async fn pull_over(engine: &Engine, conn: &Connection) -> Result<SyncStats> {
    let records = match request(conn, &Req::Index).await? {
        Resp::Index { records } => records,
        _ => bail!("peer returned a non-index response to Index"),
    };

    let mut stats = SyncStats {
        total: records.len(),
        ..Default::default()
    };

    for rec in &records {
        // Fast-skip what we already have/supersede; otherwise fetch content (unless it's a
        // tombstone) and apply with full conflict handling.
        if engine.evaluate(rec)? == SyncAction::Skip {
            stats.skipped += 1;
            continue;
        }
        let bytes = if rec.deleted {
            Vec::new()
        } else {
            match request(conn, &Req::Blob { hash: rec.hash.clone() }).await? {
                Resp::Blob { bytes } => bytes,
                _ => {
                    eprintln!("peer is missing blob {} for {}", rec.hash, rec.path);
                    continue;
                }
            }
        };
        tally(&mut stats, engine.apply_incoming(rec, &bytes)?, &rec.path);
    }
    Ok(stats)
}

/// Fold an apply outcome into the running sync stats (and log conflicts).
fn tally(stats: &mut SyncStats, outcome: ApplyOutcome, path: &str) {
    match outcome {
        ApplyOutcome::Skipped => stats.skipped += 1,
        ApplyOutcome::Applied => stats.fetched += 1,
        ApplyOutcome::ConflictKeptLocal => {
            stats.conflicts += 1;
            eprintln!("conflict: kept local edit of {path} (peer deleted it)");
        }
        ApplyOutcome::Conflicted { copy } => {
            stats.conflicts += 1;
            eprintln!("conflict: {path} — kept both, peer's version at {copy}");
        }
    }
}

/// One request/response on a fresh bidirectional stream.
async fn request(conn: &Connection, req: &Req) -> Result<Resp> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_msg(&mut send, req).await?;
    send.finish()?;
    read_msg(&mut recv).await
}

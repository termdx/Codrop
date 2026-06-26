//! Sync node over **iroh** — p2p QUIC with hole-punching + relay fallback.
//!
//! Peers are addressed by their `EndpointId` (an Ed25519 public key), not `IP:port`. iroh's
//! discovery + relay resolve that id to a working path: direct on the LAN, hole-punched across
//! NATs, or relayed when neither works (e.g. the Wi-Fi AP-isolation case that blocked our raw
//! QUIC transport). The endpoint key doubles as device identity — no more skip-verify TLS.
//!
//! The sync protocol (`proto.rs`) and the engine are unchanged; only the transport swapped.

use crate::proto::{read_msg, write_msg, Req, Resp};
use anyhow::{bail, Result};
use codrop_sync_engine::{Engine, SyncAction};
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr};
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

/// Build a Codrop endpoint with the given iroh preset (N0 = discovery + relay).
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

/// Spawn the accept loop for `engine` on an existing endpoint (used by `serve` and tests).
pub fn serve_on(engine: Arc<Engine>, endpoint: &Endpoint) {
    let endpoint = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let engine = engine.clone();
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    let _ = handle_conn(engine, conn).await;
                }
            });
        }
    });
}

async fn handle_conn(engine: Arc<Engine>, conn: Connection) -> Result<()> {
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
        };
        write_msg(&mut send, &resp).await?;
        send.finish()?;
    }
    Ok(())
}

/// Connect to `peer` (an `EndpointId` or full `EndpointAddr`) on a fresh endpoint and pull.
pub async fn pull(engine: &Engine, peer: impl Into<EndpointAddr>) -> Result<SyncStats> {
    let endpoint = build_endpoint(presets::N0).await?;
    let stats = pull_on(engine, &endpoint, peer).await?;
    endpoint.close().await;
    Ok(stats)
}

/// Pull from `peer` using an existing endpoint (used by `pull` and tests). Closes only the
/// connection, leaving the endpoint reusable.
pub async fn pull_on(
    engine: &Engine,
    endpoint: &Endpoint,
    peer: impl Into<EndpointAddr>,
) -> Result<SyncStats> {
    let conn = endpoint.connect(peer, ALPN).await?;

    let records = match request(&conn, &Req::Index).await? {
        Resp::Index { records } => records,
        _ => bail!("peer returned a non-index response to Index"),
    };

    let mut stats = SyncStats {
        total: records.len(),
        ..Default::default()
    };

    for rec in &records {
        match engine.evaluate(rec)? {
            SyncAction::Skip => stats.skipped += 1,
            SyncAction::Conflict => {
                stats.conflicts += 1;
                eprintln!("conflict: {} (concurrent edit; deferred to Phase 4)", rec.path);
            }
            SyncAction::Fetch => match request(&conn, &Req::Blob { hash: rec.hash.clone() }).await? {
                Resp::Blob { bytes } => {
                    engine.apply_remote(rec, &bytes)?;
                    stats.fetched += 1;
                }
                _ => eprintln!("peer is missing blob {} for {}", rec.hash, rec.path),
            },
        }
    }

    conn.close(0u32.into(), b"done");
    Ok(stats)
}

/// One request/response on a fresh bidirectional stream.
async fn request(conn: &Connection, req: &Req) -> Result<Resp> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_msg(&mut send, req).await?;
    send.finish()?;
    read_msg(&mut recv).await
}

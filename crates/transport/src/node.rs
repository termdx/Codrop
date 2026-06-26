//! Sync node: a QUIC server that answers index/blob requests, and a `pull` that converges
//! the local engine toward a peer.

use crate::proto::{read_msg, write_msg, Req, Resp};
use crate::tls::{client_endpoint, server_endpoint};
use anyhow::{bail, Result};
use codrop_sync_engine::{Engine, SyncAction};
use quinn::{Connection, Endpoint};
use std::net::SocketAddr;
use std::sync::Arc;

/// What a `pull` did, for logging/tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    pub total: usize,
    pub fetched: usize,
    pub skipped: usize,
    pub conflicts: usize,
}

/// Start serving `engine` over QUIC on `bind`. Returns the bound endpoint (so the caller can
/// read `local_addr()` and keep it alive); connections are handled on spawned tasks.
pub async fn serve(engine: Arc<Engine>, bind: SocketAddr) -> Result<Endpoint> {
    let endpoint = server_endpoint(bind)?;
    let acceptor = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = acceptor.accept().await {
            let engine = engine.clone();
            tokio::spawn(async move {
                if let Ok(connecting) = incoming.accept() {
                    if let Ok(conn) = connecting.await {
                        let _ = handle_conn(engine, conn).await;
                    }
                }
            });
        }
    });
    Ok(endpoint)
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

/// Connect to `peer`, fetch its index, and apply every record that causally supersedes ours.
pub async fn pull(engine: &Engine, peer: SocketAddr) -> Result<SyncStats> {
    let endpoint = client_endpoint()?;
    let conn = endpoint.connect(peer, "codrop")?.await?;

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
    endpoint.wait_idle().await;
    Ok(stats)
}

/// One request/response on a fresh bidirectional stream.
async fn request(conn: &Connection, req: &Req) -> Result<Resp> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_msg(&mut send, req).await?;
    send.finish()?;
    read_msg(&mut recv).await
}

//! Wire protocol: length-prefixed JSON messages over QUIC bidirectional streams.
//!
//! One request/response per stream. JSON keeps the prototype legible; a binary codec and
//! chunked blob streaming are a later optimization (blobs currently ride as a byte array).

use codrop_sync_engine::FileRecord;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// One peer -> another. Content moves as manifests + chunks, so only missing chunks transfer.
#[derive(Debug, Serialize, Deserialize)]
pub enum Req {
    /// "Send me your whole index."
    Index,
    /// "Send me the chunk manifest for this full-content hash."
    Manifest { hash: String },
    /// "Send me the bytes for this chunk hash."
    Chunk { hash: String },
    /// "I have a new version of this path — pull the chunks you're missing and apply it."
    Push { record: FileRecord },
}

/// Response to a `Req`.
#[derive(Debug, Serialize, Deserialize)]
pub enum Resp {
    Index {
        records: Vec<FileRecord>,
    },
    Manifest {
        chunks: Vec<String>,
    },
    Chunk {
        bytes: Vec<u8>,
    },
    NotFound,
    /// Acknowledges a `Push` (sent after the pushed change has been fetched + applied).
    Ok,
}

/// Write a `u32` big-endian length prefix followed by the JSON body.
pub async fn write_msg<T: Serialize>(
    send: &mut iroh::endpoint::SendStream,
    msg: &T,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(msg)?;
    send.write_all(&(body.len() as u32).to_be_bytes()).await?;
    send.write_all(&body).await?;
    Ok(())
}

/// Upper bound on a single framed message, to stop a peer's length prefix from triggering a
/// multi-GiB allocation (DoS). Generous headroom over any real index/manifest/chunk message.
const MAX_MSG: usize = 256 * 1024 * 1024;

/// Read one length-prefixed JSON message.
pub async fn read_msg<T: DeserializeOwned>(
    recv: &mut iroh::endpoint::RecvStream,
) -> anyhow::Result<T> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    anyhow::ensure!(
        n <= MAX_MSG,
        "peer message too large: {n} bytes (max {MAX_MSG})"
    );
    let mut body = vec![0u8; n];
    recv.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
}

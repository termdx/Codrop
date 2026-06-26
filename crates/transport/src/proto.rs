//! Wire protocol: length-prefixed JSON messages over QUIC bidirectional streams.
//!
//! One request/response per stream. JSON keeps the prototype legible; a binary codec and
//! chunked blob streaming are a later optimization (blobs currently ride as a byte array).

use codrop_sync_engine::FileRecord;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Client -> server.
#[derive(Debug, Serialize, Deserialize)]
pub enum Req {
    /// "Send me your whole index."
    Index,
    /// "Send me the bytes for this content hash."
    Blob { hash: String },
}

/// Server -> client.
#[derive(Debug, Serialize, Deserialize)]
pub enum Resp {
    Index { records: Vec<FileRecord> },
    Blob { bytes: Vec<u8> },
    NotFound,
}

/// Write a `u32` big-endian length prefix followed by the JSON body.
pub async fn write_msg<T: Serialize>(send: &mut iroh::endpoint::SendStream, msg: &T) -> anyhow::Result<()> {
    let body = serde_json::to_vec(msg)?;
    send.write_all(&(body.len() as u32).to_be_bytes()).await?;
    send.write_all(&body).await?;
    Ok(())
}

/// Read one length-prefixed JSON message.
pub async fn read_msg<T: DeserializeOwned>(recv: &mut iroh::endpoint::RecvStream) -> anyhow::Result<T> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    let mut body = vec![0u8; n];
    recv.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
}

//! `codrop-net` — manual two-node demo driver for Phase 2 (iroh transport).
//!
//!   codrop-net serve <root>            # scan + serve a tree; prints this node's EndpointId
//!   codrop-net pull  <root> <id>       # pull the peer with that EndpointId into <root>
//!
//! Peers connect by EndpointId, so this works across LAN, NAT, and relay — no IP:port needed.

use anyhow::{anyhow, bail, Result};
use codrop_sync_engine::Engine;
use iroh::EndpointId;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use codrop_sync_engine::IGNORE_DIRS as IGNORE;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") => serve_cmd(&args).await,
        Some("pull") => pull_cmd(&args).await,
        _ => bail!("usage: codrop-net <serve|pull> ..."),
    }
}

async fn serve_cmd(args: &[String]) -> Result<()> {
    let root = PathBuf::from(args.get(2).cloned().unwrap_or_else(|| ".".into())).canonicalize()?;
    let engine = Engine::open(&root, root.join(".codrop"))?;
    let files = scan(&engine, &root)?;

    let endpoint = codrop_transport::serve(Arc::new(engine)).await?;
    let id = endpoint.id();
    println!("serving {} ({files} files)", root.display());
    println!("  endpoint id: {id}");
    println!("  on the other device, run:");
    println!("      codrop-net pull <dir> {id}");
    println!("ctrl-c to stop");

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn pull_cmd(args: &[String]) -> Result<()> {
    let root = PathBuf::from(args.get(2).ok_or_else(|| anyhow!("usage: pull <root> <id>"))?);
    std::fs::create_dir_all(&root)?;
    let root = root.canonicalize()?;
    let id: EndpointId = args
        .get(3)
        .ok_or_else(|| anyhow!("usage: pull <root> <id>"))?
        .parse()
        .map_err(|e| anyhow!("invalid endpoint id: {e}"))?;

    let engine = Engine::open(&root, root.join(".codrop"))?;
    let stats = codrop_transport::pull(&engine, id).await?;
    println!("pull complete: {stats:?}");
    Ok(())
}

/// Recursively observe every file under `root` (skipping ignored dirs) into the index.
fn scan(engine: &Engine, root: &Path) -> Result<usize> {
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if IGNORE.contains(&name.to_string_lossy().as_ref()) {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                engine.observe(&path)?;
                count += 1;
            }
        }
    }
    Ok(count)
}

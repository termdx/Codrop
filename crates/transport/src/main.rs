//! `codrop-net` — manual two-node demo driver for Phase 2.
//!
//!   codrop-net serve   <root> [bind=0.0.0.0:4500]   # scan + serve a tree
//!   codrop-net pull    <root> <peer-addr>           # pull a peer into <root>
//!   codrop-net discover                             # browse the LAN for peers

use anyhow::{anyhow, bail, Result};
use codrop_sync_engine::Engine;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const IGNORE: &[&str] = &[".codrop", "node_modules", ".git", "target", "dist", "build"];

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") => serve_cmd(&args).await,
        Some("pull") => pull_cmd(&args).await,
        Some("discover") => codrop_transport::discovery::discover(Duration::from_secs(5)),
        _ => bail!("usage: codrop-net <serve|pull|discover> ..."),
    }
}

async fn serve_cmd(args: &[String]) -> Result<()> {
    let root = PathBuf::from(args.get(2).cloned().unwrap_or_else(|| ".".into())).canonicalize()?;
    let bind: SocketAddr = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| "0.0.0.0:4500".into())
        .parse()?;

    let engine = Engine::open(&root, root.join(".codrop"))?;
    let files = scan(&engine, &root)?;
    let endpoint = codrop_transport::serve(Arc::new(engine), bind).await?;
    let addr = endpoint.local_addr()?;
    println!("serving {} ({files} files) on {addr}", root.display());

    // Best-effort LAN advertisement; ignore failures (e.g. restricted networks).
    let _mdns = codrop_transport::discovery::advertise("codrop-node", addr.port());
    if _mdns.is_err() {
        eprintln!("mDNS advertise unavailable; peers must connect by address");
    }

    println!("ctrl-c to stop");
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn pull_cmd(args: &[String]) -> Result<()> {
    let root = PathBuf::from(args.get(2).ok_or_else(|| anyhow!("usage: pull <root> <peer>"))?);
    std::fs::create_dir_all(&root)?;
    let root = root.canonicalize()?;
    let peer: SocketAddr = args
        .get(3)
        .ok_or_else(|| anyhow!("usage: pull <root> <peer>"))?
        .parse()?;

    let engine = Engine::open(&root, root.join(".codrop"))?;
    let stats = codrop_transport::pull(&engine, peer).await?;
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

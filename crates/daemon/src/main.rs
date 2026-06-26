//! `codrop` — the live-sync daemon.
//!
//!   codrop run <dir> [--peer <endpoint-id>]
//!
//! Fuses the three phases into one long-running process: it watches `<dir>` (Phase 0), keeps
//! the content-addressed index (Phase 1), and pushes/pulls changes to a peer over iroh
//! (Phase 2). Identity is a persisted key in `<dir>/.codrop/endpoint.key`, so the EndpointId
//! is stable across restarts.
//!
//! On startup it connects to `--peer` (if given), pulls the peer's files, and pushes its own
//! (initial convergence). Thereafter every local change is pushed live. Pushes received from a
//! peer are applied by the engine; because the index then holds that content's hash, the
//! watcher's `observe()` of the written file is a no-op — content-addressing suppresses echoes.
//!
//! Live sync is per outgoing connection: A learns B's edits only if A ran with `--peer B`.
//! For bidirectional live sync, run the daemon with `--peer` on both sides.

use anyhow::{anyhow, bail, Result};
use codrop_sync_engine::Engine;
use codrop_transport as net;
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointId};
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const IGNORE: &[&str] = &[".codrop", "node_modules", ".git", "target", "dist", "build", ".next"];

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) != Some("run") {
        bail!("usage: codrop run <dir> [--peer <endpoint-id>]");
    }

    let dir = PathBuf::from(args.get(2).ok_or_else(|| anyhow!("usage: codrop run <dir> [--peer <id>]"))?);
    std::fs::create_dir_all(&dir)?;
    let root = dir.canonicalize()?;

    let peer: Option<EndpointId> = match args.iter().position(|a| a == "--peer") {
        Some(i) => Some(
            args.get(i + 1)
                .ok_or_else(|| anyhow!("--peer needs an endpoint id"))?
                .parse()
                .map_err(|e| anyhow!("invalid endpoint id: {e}"))?,
        ),
        None => None,
    };

    let engine = Arc::new(Engine::open(&root, root.join(".codrop"))?);
    let indexed = scan(&engine, &root)?;

    // Stable device identity, persisted under .codrop.
    let key = net::load_or_create_key(&root.join(".codrop/endpoint.key"))?;
    let endpoint = net::endpoint_with_key(key).await?;
    println!("codrop: watching {} ({indexed} files)", root.display());
    println!("  endpoint id: {}", endpoint.id());
    if let Some(p) = peer {
        println!("  peer: {p}");
    }

    // Accept incoming connections (serve pulls + receive live pushes).
    net::serve_on(engine.clone(), &endpoint);

    // Connect to the peer (if any): pull theirs, push ours — initial convergence.
    let mut conn: Option<Connection> = None;
    if let Some(peer) = peer {
        match net::connect(&endpoint, peer).await {
            Ok(c) => {
                match net::pull_over(&engine, &c).await {
                    Ok(stats) => println!("initial pull: {stats:?}"),
                    Err(e) => eprintln!("initial pull failed: {e}"),
                }
                if let Err(e) = push_all(&engine, &c).await {
                    eprintln!("initial push failed: {e}");
                }
                conn = Some(c);
            }
            Err(e) => eprintln!("peer not reachable yet (will retry on first change): {e}"),
        }
    }

    // File watcher on a dedicated thread; events bridged to the async loop.
    let (raw_tx, raw_rx) = std::sync::mpsc::channel();
    let mut debouncer = new_debouncer(Duration::from_millis(400), None, raw_tx)?;
    debouncer.watcher().watch(&root, RecursiveMode::Recursive)?;
    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    std::thread::spawn(move || {
        for result in raw_rx {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.event.paths {
                        let _ = ev_tx.send(path.clone());
                    }
                }
            }
        }
    });

    println!("live. ctrl-c to stop.");
    while let Some(path) = ev_rx.recv().await {
        if is_ignored(&path) || !path.is_file() {
            continue;
        }
        let obs = match engine.observe(&path) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("observe {}: {e}", path.display());
                continue;
            }
        };
        if !obs.changed {
            continue; // unchanged content (incl. just-applied peer pushes) → no echo
        }
        println!("changed: {} ({})", obs.path, &obs.hash[..12]);

        if let Some(peer) = peer {
            let Some(rec) = engine.index().get(&obs.path)? else { continue };
            let Some(bytes) = engine.store().read(&obs.hash)? else { continue };
            match push_with_retry(&endpoint, peer, &rec, &bytes, &mut conn).await {
                Ok(()) => println!("  -> pushed to peer"),
                Err(e) => eprintln!("  -> push failed: {e}"),
            }
        }
    }

    Ok(())
}

/// Push every locally-indexed file to a peer (initial convergence). No-op on the peer for
/// content it already has.
async fn push_all(engine: &Engine, conn: &Connection) -> Result<()> {
    for rec in engine.local_records()? {
        if let Some(bytes) = engine.store().read(&rec.hash)? {
            net::push(conn, &rec, &bytes).await?;
        }
    }
    Ok(())
}

/// Push one change, (re)connecting once if the cached connection has dropped.
async fn push_with_retry(
    endpoint: &Endpoint,
    peer: EndpointId,
    rec: &codrop_sync_engine::FileRecord,
    bytes: &[u8],
    conn: &mut Option<Connection>,
) -> Result<()> {
    for attempt in 0..2 {
        if conn.is_none() {
            *conn = Some(net::connect(endpoint, peer).await?);
        }
        match net::push(conn.as_ref().unwrap(), rec, bytes).await {
            Ok(()) => return Ok(()),
            Err(_) if attempt == 0 => *conn = None, // drop & reconnect once
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn is_ignored(path: &Path) -> bool {
    path.components().any(|c| {
        c.as_os_str()
            .to_str()
            .map(|s| IGNORE.contains(&s))
            .unwrap_or(false)
    })
}

/// Recursively observe every file under `root` (skipping ignored dirs) into the index.
fn scan(engine: &Engine, root: &Path) -> Result<usize> {
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if IGNORE.contains(&entry.file_name().to_string_lossy().as_ref()) {
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

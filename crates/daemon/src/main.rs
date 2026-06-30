//! `codrop` — the live-sync daemon.
//!
//!   codrop run <dir> [--peer <endpoint-id>]   # watch + sync continuously
//!   codrop id  <dir>                           # print the dir's stable endpoint id
//!
//! Fuses the phases into one process: watches `<dir>` (Phase 0), keeps the content-addressed
//! index (Phase 1), and syncs over iroh (Phase 2). Identity is a persisted key in
//! `<dir>/.codrop/endpoint.key`, so the EndpointId is stable across restarts.
//!
//! Connections are **symmetric**: every link (whether we dialed it or accepted it) both serves
//! the peer's requests and carries our pushes. So pointing one side at the other with a single
//! `--peer` gives **bidirectional** live sync — no need to configure both ends. The `--peer`
//! link auto-(re)connects in the background; on connect we pull the peer's files and push ours.
//!
//! Echo loops are impossible: an applied push lands in the index, so the watcher's `observe()`
//! of that write is a no-op (content-addressing).

use anyhow::{anyhow, Result};
use codrop_sync_engine::{Engine, FileRecord};
use codrop_transport as net;
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointId};
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Snapshot the daemon writes to `<dir>/.codrop/status.json` for `status`/`stop` to read.
#[derive(Serialize, Deserialize)]
struct Status {
    pid: u32,
    endpoint_id: String,
    files: usize,
    peers: Vec<PeerStatus>,
    updated_ms: i64,
}

#[derive(Serialize, Deserialize)]
struct PeerStatus {
    id: String,
    connected: bool,
}

const IGNORE: &[&str] = &[".codrop", "node_modules", ".git", "target", "dist", "build", ".next"];

/// Live connections to peers (inbound + the outbound `--peer` link).
type PeerSet = Arc<Mutex<Vec<Connection>>>;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("run") => run(&args).await,
        Some("id") => id_cmd(&args),
        Some("status") => status_cmd(&args),
        Some("stop") => stop_cmd(&args),
        Some("--help") | Some("-h") | Some("help") | None => {
            print_help();
            Ok(())
        }
        Some("--version") | Some("-V") => {
            println!("codrop {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some(other) => {
            eprintln!("codrop: unknown command '{other}'\n");
            print_help();
            std::process::exit(2);
        }
    }
}

fn print_help() {
    print!(
        "\
codrop — a Dropbox for devs: live folder sync across your machines, over iroh.

USAGE:
    codrop run <dir> [--peer <endpoint-id>] [--detach]
                                              Watch <dir> and sync it live with a peer
                                              (--detach / -d runs it in the background)
    codrop id     <dir>                       Print <dir>'s stable endpoint id
    codrop status <dir>                       Show daemon status: connected peers + sync state
    codrop stop   <dir>                       Stop the daemon running for <dir>
    codrop --help                             Show this help
    codrop --version                          Show the version

EXAMPLES:
    # machine B: start syncing (its endpoint id prints in the banner)
    codrop run ~/code

    # machine A: point at B's id — one --peer syncs both directions
    codrop run ~/code --peer <B-endpoint-id>

NOTES:
    • Peers connect by EndpointId (a public key) — no IP addresses; works across NAT/relay.
    • A single --peer gives bidirectional sync; pass it on either side.
    • State lives in <dir>/.codrop, added to .gitignore automatically.
"
    );
}

/// Print the stable endpoint id for `<dir>` without starting the daemon (generates the key on
/// first use). Use it to learn a folder's id for the other side's `--peer`.
fn id_cmd(args: &[String]) -> Result<()> {
    let dir = PathBuf::from(args.get(2).ok_or_else(|| anyhow!("usage: codrop id <dir>"))?);
    std::fs::create_dir_all(&dir)?;
    let key = net::load_or_create_key(&dir.join(".codrop/endpoint.key"))?;
    // Creating .codrop here too → keep it consistent with `run` and out of git.
    codrop_sync_engine::ignore_state_in_git(&dir, &dir.join(".codrop"));
    println!("{}", key.public());
    Ok(())
}

/// Read the daemon's published status for `<dir>` and print peers + sync state.
fn status_cmd(args: &[String]) -> Result<()> {
    let dir = PathBuf::from(args.get(2).ok_or_else(|| anyhow!("usage: codrop status <dir>"))?);
    let status = match read_status(&dir)? {
        Some(s) => s,
        None => {
            println!("codrop: not running for {} (no daemon)", dir.display());
            return Ok(());
        }
    };
    if !pid_alive(status.pid) {
        println!("codrop: not running ({} — last pid {} is gone)", dir.display(), status.pid);
        return Ok(());
    }

    let age = (now_ms() - status.updated_ms).max(0) / 1000;
    println!("codrop: running (pid {})", status.pid);
    println!("  endpoint id: {}", status.endpoint_id);
    println!("  tracking:    {} files", status.files);
    println!("  status:      live ({age}s ago)");
    if status.peers.is_empty() {
        println!("  peers:       none connected");
    } else {
        println!("  peers:       {} connected", status.peers.len());
        for p in &status.peers {
            let mark = if p.connected { "●" } else { "○" };
            println!("    {mark} {}", p.id);
        }
    }
    Ok(())
}

/// Stop the daemon running for `<dir>` (SIGTERM to its recorded pid).
fn stop_cmd(args: &[String]) -> Result<()> {
    let dir = PathBuf::from(args.get(2).ok_or_else(|| anyhow!("usage: codrop stop <dir>"))?);
    let pid = match read_status(&dir)? {
        Some(s) => s.pid,
        None => {
            println!("codrop: not running for {} (no daemon)", dir.display());
            return Ok(());
        }
    };
    let status_path = dir.join(".codrop/status.json");
    if !pid_alive(pid) {
        println!("codrop: not running (pid {pid} already gone)");
        let _ = std::fs::remove_file(&status_path);
        return Ok(());
    }
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
    let _ = std::fs::remove_file(&status_path);
    println!("codrop: stopped (pid {pid})");
    Ok(())
}

fn read_status(dir: &Path) -> Result<Option<Status>> {
    match std::fs::read_to_string(dir.join(".codrop/status.json")) {
        Ok(data) => Ok(Some(
            serde_json::from_str(&data).map_err(|e| anyhow!("corrupt status file: {e}"))?,
        )),
        Err(_) => Ok(None),
    }
}

/// True if a process with `pid` exists (signal 0 probe). Assumes same-user ownership.
fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

/// Periodically publish the daemon's live status to `<state>/status.json`.
fn spawn_status_writer(state_dir: PathBuf, endpoint: Endpoint, engine: Arc<Engine>, peers: PeerSet) {
    tokio::spawn(async move {
        loop {
            let snapshot = snapshot_status(&endpoint, &engine, &peers).await;
            let _ = write_status(&state_dir, &snapshot);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

async fn snapshot_status(endpoint: &Endpoint, engine: &Engine, peers: &PeerSet) -> Status {
    let mut set = peers.lock().await;
    set.retain(|c| c.close_reason().is_none());
    let mut seen = HashSet::new();
    let peer_list: Vec<PeerStatus> = set
        .iter()
        .map(|c| c.remote_id().to_string())
        .filter(|id| seen.insert(id.clone())) // dedup (inbound + outbound to same peer)
        .map(|id| PeerStatus { id, connected: true })
        .collect();
    drop(set);

    let files = engine
        .local_records()
        .map(|r| r.iter().filter(|x| !x.deleted).count())
        .unwrap_or(0);

    Status {
        pid: std::process::id(),
        endpoint_id: endpoint.id().to_string(),
        files,
        peers: peer_list,
        updated_ms: now_ms(),
    }
}

fn write_status(state_dir: &Path, status: &Status) -> Result<()> {
    let tmp = state_dir.join("status.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(status)?)?;
    std::fs::rename(&tmp, state_dir.join("status.json"))?;
    Ok(())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Re-spawn this binary as a detached background process (new session, output to a log file),
/// then return so the parent exits. The child runs `run` normally (the flag is stripped).
fn detach(args: &[String]) -> Result<()> {
    // Child argv = our args minus the program name and the detach flag.
    let child_args: Vec<String> = args
        .iter()
        .skip(1)
        .filter(|a| *a != "--detach" && *a != "-d")
        .cloned()
        .collect();
    // child_args == ["run", <dir>, maybe "--peer", <id>]
    let dir_arg = child_args
        .get(1)
        .ok_or_else(|| anyhow!("usage: codrop run <dir> [--peer <id>] --detach"))?;
    let dir = PathBuf::from(dir_arg.as_str());

    let state = dir.join(".codrop");
    std::fs::create_dir_all(&state)?;
    let log_path = state.join("daemon.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let log_err = log.try_clone()?;

    let mut cmd = std::process::Command::new(std::env::current_exe()?);
    cmd.args(&child_args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(log_err));

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid() is async-signal-safe and runs in the forked child before exec,
        // giving it a new session so it survives the terminal/shell closing.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let child = cmd.spawn()?;
    println!("codrop: running detached (pid {})", child.id());
    println!("  logs:  {}", log_path.display());
    println!("  id:    codrop id {}", dir.display());
    println!("  stop:  kill {}", child.id());
    Ok(())
}

async fn run(args: &[String]) -> Result<()> {
    // --detach: re-spawn ourselves as a background process and return, before any heavy init.
    if args.iter().any(|a| a == "--detach" || a == "-d") {
        return detach(args);
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

    let key = net::load_or_create_key(&root.join(".codrop/endpoint.key"))?;
    let endpoint = net::endpoint_with_key(key).await?;
    println!("codrop: watching {} ({indexed} files)", root.display());
    println!("  endpoint id: {}", endpoint.id());
    if let Some(p) = peer {
        println!("  peer: {p}");
    }

    let peers: PeerSet = Arc::new(Mutex::new(Vec::new()));

    spawn_accept_loop(endpoint.clone(), engine.clone(), peers.clone());
    if let Some(peer) = peer {
        spawn_peer_link(endpoint.clone(), engine.clone(), peers.clone(), peer);
    }
    spawn_status_writer(root.join(".codrop"), endpoint.clone(), engine.clone(), peers.clone());

    // File watcher on a dedicated thread; events bridged to this async loop.
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
        if is_ignored(&path) {
            continue;
        }

        if path.is_file() {
            // Create or modify.
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
            let Some(rec) = engine.index().get(&obs.path)? else { continue };
            let Some(bytes) = engine.store().read(&obs.hash)? else { continue };
            broadcast(&peers, &rec, &bytes).await;
        } else if !path.exists() {
            // Possible deletion of a tracked file → tombstone + propagate (empty content).
            match engine.observe_delete(&path) {
                Ok(Some(tomb)) => {
                    println!("deleted: {}", tomb.path);
                    broadcast(&peers, &tomb, &[]).await;
                }
                Ok(None) => {} // not a tracked live file (dir, temp, already gone)
                Err(e) => eprintln!("observe_delete {}: {e}", path.display()),
            }
        }
    }

    Ok(())
}

/// Accept inbound connections: register each one and serve it (it also carries our pushes).
fn spawn_accept_loop(endpoint: Endpoint, engine: Arc<Engine>, peers: PeerSet) {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let engine = engine.clone();
            let peers = peers.clone();
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    println!("peer connected: {}", conn.remote_id().fmt_short());
                    peers.lock().await.push(conn.clone());
                    let _ = net::serve_connection(engine, conn).await;
                }
            });
        }
    });
}

/// Keep the outbound `--peer` link alive: (re)connect, converge, register, and serve it.
fn spawn_peer_link(endpoint: Endpoint, engine: Arc<Engine>, peers: PeerSet, peer: EndpointId) {
    tokio::spawn(async move {
        loop {
            // Drop dead links, then connect if we don't already have a live one to `peer`.
            let connected = {
                let mut set = peers.lock().await;
                set.retain(|c| c.close_reason().is_none());
                set.iter().any(|c| c.remote_id() == peer)
            };
            if !connected {
                if let Ok(conn) = net::connect(&endpoint, peer).await {
                    println!("connected to peer {}", peer.fmt_short());
                    match net::pull_over(&engine, &conn).await {
                        Ok(stats) => println!("  initial pull: {stats:?}"),
                        Err(e) => eprintln!("  initial pull failed: {e}"),
                    }
                    if let Err(e) = push_all(&engine, &conn).await {
                        eprintln!("  initial push failed: {e}");
                    }
                    peers.lock().await.push(conn.clone());
                    let engine = engine.clone();
                    tokio::spawn(async move {
                        let _ = net::serve_connection(engine, conn).await;
                    });
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Push a change to every live peer connection; prune any that have closed.
async fn broadcast(peers: &PeerSet, rec: &FileRecord, bytes: &[u8]) {
    let conns: Vec<Connection> = peers.lock().await.clone();
    let mut pushed = 0;
    for conn in &conns {
        match net::push(conn, rec, bytes).await {
            Ok(()) => pushed += 1,
            Err(e) => eprintln!("  push to {} failed: {e}", conn.remote_id().fmt_short()),
        }
    }
    peers.lock().await.retain(|c| c.close_reason().is_none());
    if pushed > 0 {
        println!("  -> pushed to {pushed} peer(s)");
    }
}

/// Push every locally-indexed file to a peer (initial convergence). No-op for content the peer
/// already has.
async fn push_all(engine: &Engine, conn: &Connection) -> Result<()> {
    for rec in engine.local_records()? {
        // Tombstones carry no blob; everything else ships its content.
        let bytes = if rec.deleted {
            Vec::new()
        } else {
            engine.store().read(&rec.hash)?.unwrap_or_default()
        };
        net::push(conn, &rec, &bytes).await?;
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

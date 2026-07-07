//! Codrop watcher daemon (Phase 0 + Phase 1).
//!
//! Recursively watches a directory via the OS-native backend (FSEvents on macOS), applies
//! ignore rules so OS-compiled forests never sync, debounces editor save-storms, and feeds
//! every real file change into the sync engine — which content-addresses the bytes and
//! updates the SQLite index with a bumped vector clock. Manifest changes are logged as a
//! DRY-RUN install trigger only (auto-install is an RCE vector; see plan Blocker 4).

use anyhow::Result;
use codrop_sync_engine::Engine;
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use std::{
    path::{Path, PathBuf},
    sync::mpsc::channel,
    time::Duration,
};

/// Directory names that are OS-compiled/regenerable or our own state — never synced (req 3).
/// `.codrop` holds the index + blob store; ignoring it prevents the engine's own writes from
/// echoing back as watch events.
use codrop_sync_engine::IGNORE_DIRS;

/// Lockfiles whose change should drive a (future, sandboxed) install. We key off lockfiles
/// rather than package.json so installs run against a fully-resolved dependency set.
const MANIFESTS: &[&str] = &[
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "Cargo.lock",
    "poetry.lock",
    "requirements.txt",
];

fn is_ignored(path: &Path) -> bool {
    path.components().any(|c| {
        c.as_os_str()
            .to_str()
            .map(|s| IGNORE_DIRS.contains(&s))
            .unwrap_or(false)
    })
}

fn is_manifest(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| MANIFESTS.contains(&n))
        .unwrap_or(false)
}

fn main() -> Result<()> {
    let root: PathBuf = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().expect("cwd"));
    // Canonicalize so paths match the (canonical) ones the watcher reports, letting the
    // engine strip the root prefix cleanly into stable relative keys.
    let root = root.canonicalize()?;

    // Engine state lives in <root>/.codrop, which is in IGNORE_DIRS (no self-echo).
    let engine = Engine::open(&root, root.join(".codrop"))?;
    println!(
        "codrop-watchd: watching {} (device {}, debounced)",
        root.display(),
        engine.device_id()
    );

    let (tx, rx) = channel();
    // 500ms debounce coalesces editor atomic-save rename storms into one event batch.
    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)?;
    debouncer.watcher().watch(&root, RecursiveMode::Recursive)?;

    for result in rx {
        let events = match result {
            Ok(events) => events,
            Err(errors) => {
                for e in errors {
                    eprintln!("watch error: {e:?}");
                }
                continue;
            }
        };

        for event in events {
            for path in &event.event.paths {
                if is_ignored(path) {
                    continue; // req 3: never sync OS-compiled forests or our own state
                }

                if is_manifest(path) {
                    // Blocker 4: lockfile changed -> DRY RUN only, never auto-exec here.
                    println!(
                        "manifest changed: {} -> would schedule sandboxed install (dry-run)",
                        path.display()
                    );
                    continue;
                }

                if !path.is_file() {
                    // Deletes, directory events, etc. Index tombstones land in a later phase.
                    println!("event:     {} (deleted or non-file)", path.display());
                    continue;
                }

                let short = |h: &str| h[..16.min(h.len())].to_string();
                match engine.observe(path) {
                    Ok(obs) if obs.changed => println!(
                        "indexed:   {} hash={} size={}",
                        obs.path,
                        short(&obs.hash),
                        obs.size
                    ),
                    Ok(obs) => println!("unchanged: {} ({})", obs.path, short(&obs.hash)),
                    Err(e) => eprintln!("observe failed for {}: {e}", path.display()),
                }
            }
        }
    }

    Ok(())
}

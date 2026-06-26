# Codrop

A "Dropbox for devs": a unified code folder that auto-syncs across all of a developer's
machines and cloud agents with zero manual effort. See the architecture plan for the full
vision, the blueprint review, and the build phases.

## Monorepo layout

This is a Cargo workspace. Rust components live under `crates/`; non-Rust components (e.g. the
macOS File Provider extension in Swift) will live in sibling top-level directories.

```
codrop/
├── Cargo.toml                  # workspace root: shared deps + metadata
├── crates/
│   ├── watcher-daemon/         # codrop-watchd — watches the tree, feeds the engine
│   │   └── src/main.rs
│   └── sync-engine/            # codrop-sync-engine — CAS + SQLite index + vector clocks
│       ├── src/{lib,engine,store,index,vclock}.rs
│       └── tests/integration.rs
└── README.md
```

### Build phases

| Component | Where | Status |
|---|---|---|
| `watcher-daemon` | `crates/watcher-daemon` | ✅ Phase 0 — debounced FS watcher, ignore rules, manifest dry-run |
| `sync-engine` | `crates/sync-engine` | ✅ Phase 1 — content-addressed store, SQLite index, vector clocks, `clonefile()` materialization |
| `transport` | `crates/transport` | ⬜ Phase 2 — QUIC (`quinn`) + mDNS (`mdns-sd`) peer sync |
| `delta` | `crates/delta` | ⬜ Phase 3 — `fast_rsync` / FastCDC for large in-place files |
| conflict engine | `crates/sync-engine` | ⬜ Phase 4 — conflicted-copies from concurrent vector clocks |
| File Provider ext | `apple/` (Swift) | ⬜ Phase 5 — macOS lazy VFS over XPC to the Rust core |

## How Phase 1 fits together

The watcher detects a change and calls `Engine::observe(path)`, which:

1. reads the file and stores its bytes in the **content-addressed blob store**
   (`.codrop/blobs/objects/<hash>`), deduping identical content;
2. looks up the previous record in the **SQLite index** (`.codrop/index.sqlite`);
3. if the content hash changed, bumps this device's **vector clock** and upserts the row.

Vector clocks (not wall-clock time) give a clean causal order, so Phase 4 can detect
*concurrent* edits as true conflicts instead of silently losing one side.

## Build & run

```bash
cargo build                                  # build the workspace
cargo test  -p codrop-sync-engine            # engine unit/integration tests
cargo run   -p codrop-watchd -- /path/to/watch
```

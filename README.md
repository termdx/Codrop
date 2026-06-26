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
└── crates/
    ├── watcher-daemon/         # codrop-watchd — standalone FS watcher (Phase 0 stepping stone)
    ├── sync-engine/            # codrop-sync-engine — CAS + SQLite index + vector clocks
    ├── transport/              # codrop-transport / codrop-net — iroh p2p index/blob/push sync
    └── daemon/                 # codrop — live-sync daemon: watch + push/pull, persisted identity
```

### Build phases

| Component | Where | Status |
|---|---|---|
| `watcher-daemon` | `crates/watcher-daemon` | ✅ Phase 0 — debounced FS watcher, ignore rules, manifest dry-run |
| `sync-engine` | `crates/sync-engine` | ✅ Phase 1 — content-addressed store, SQLite index, vector clocks, `clonefile()` |
| `transport` | `crates/transport` | ✅ Phase 2 — **iroh** p2p transport (hole-punch + relay fallback), index/blob/push sync |
| `daemon` | `crates/daemon` | ✅ Live sync — watch + push/pull over iroh, persisted device identity, bidirectional |
| `delta` | `crates/delta` | ⬜ Phase 3 — `fast_rsync` / FastCDC for large in-place files |
| conflict engine | `crates/sync-engine` | ⬜ Phase 4 — conflicted-copies from concurrent vector clocks |
| File Provider ext | `apple/` (Swift) | ⬜ Phase 5 — macOS lazy VFS over XPC to the Rust core |

## How sync works (Phases 1–2)

1. The **watcher** detects a change and calls `Engine::observe(path)`, which content-addresses
   the bytes into `.codrop/blobs/`, and — if the hash changed — bumps this device's **vector
   clock** and upserts the row in `.codrop/index.sqlite`.
2. A peer **pulls** over an **iroh** connection (QUIC, with hole-punching + relay fallback so
   it works across LAN/NAT/restrictive Wi-Fi): it fetches the server's index, and for each
   record asks the engine to `evaluate` it via vector-clock comparison → `Skip` / `Fetch` /
   `Conflict`. Peers are addressed by `EndpointId` (a public key), which also authenticates
   them — no IP:port, no skip-verify TLS.
3. `Fetch` requests the blob, then `apply_remote` materializes the file and records the
   **merged** clock. Because the index now holds that content's hash, the watcher's later
   `observe()` of the written file is a no-op — that idempotency is what kills sync echo loops.
   `Conflict` (concurrent edits) is logged and deferred to Phase 4.

## Build, test, run

```bash
cargo build
cargo test  -p codrop-sync-engine     # CAS / index / vector-clock tests
cargo test  -p codrop-transport       # two engines converge over iroh (loopback)
cargo run   -p codrop-watchd -- /path/to/watch
```

### Two-node demo (`codrop-net`)

Peers connect by **EndpointId**, so this works across LAN, NAT, and relay — no IP needed.

```bash
# device 1 — scan a tree and serve it; copy the printed endpoint id
cargo run -p codrop-transport --bin codrop-net -- serve ~/projectA
#   serving ~/projectA (N files)
#     endpoint id: c166f63006cc...

# device 2 — pull it into a fresh folder using that id
cargo run -p codrop-transport --bin codrop-net -- pull ~/projectB c166f63006cc...
```

### Live sync daemon (`codrop`)

Runs continuously: watches the folder and syncs changes to a peer automatically. Identity is
persisted in `<dir>/.codrop/endpoint.key`, so the endpoint id is stable across restarts.

```bash
# device 2 — start the daemon; note its (stable) endpoint id
cargo run -p codrop-daemon --bin codrop -- run ~/projectB

# device 1 — start the daemon pointed at device 2's id
cargo run -p codrop-daemon --bin codrop -- run ~/projectA --peer <device-2-id>
```

Now edits in `~/projectA` propagate to `~/projectB` within a second. For **bidirectional** live
sync, pass `--peer` on both sides (each daemon pushes its own changes over its outgoing
connection). Initial convergence on connect is automatic (pull theirs + push ours).

> Note: `.env` files currently sync in the clear. End-to-end encryption + selective-sync
> policy for secrets is Phase 7 — don't point this at real secrets yet.

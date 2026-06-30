# Codrop

A "Dropbox for devs" — a unified code folder that automatically syncs across all your machines
and cloud agents, with zero manual effort. No `git pull`, no copying `.env` files around, no
building on a stale base.

Codrop watches a folder, content-addresses every change, and syncs it to your other devices
over an encrypted peer-to-peer connection. Devices are identified and authenticated by public
key — you never deal with IP addresses, and it works across LAN, NAT, and restrictive Wi-Fi
(direct when possible, relayed when the network won't allow direct).

## Build

Requires **Rust ≥ 1.91**.

```bash
git clone https://github.com/termdx/Codrop.git
cd Codrop
cargo build --release
```

Binaries land in `target/release/`: `codrop` (the daemon), plus `codrop-net` and
`codrop-watchd`. Add that directory to your `PATH`, install with
`cargo install --path crates/daemon`, or prefix the commands below with `./target/release/`.

## Usage — the `codrop` daemon

Run the daemon on each machine and point one at the other. A **single `--peer` gives
bidirectional sync**.

```bash
# machine B — print its stable id (or just run it and read the banner)
codrop id ~/code
#   d951e2ed584d...

# machine B — start syncing ~/code
codrop run ~/code

# machine A — sync ~/code with machine B
codrop run ~/code --peer d951e2ed584d...
```

Now edits in `~/code` on either machine appear on the other within about a second. On connect,
the two sides converge automatically (each receives the other's files); the link reconnects on
its own if the network drops.

- **Stable identity.** A device key lives in `<dir>/.codrop/endpoint.key`, so a device's id is
  the same across restarts. `codrop id <dir>` prints it without starting the daemon.
- **Ignored by default:** `node_modules`, `.git`, `target`, `dist`, `build`, `.next` — the
  OS/toolchain-specific directories you don't want to sync.

## One-shot sync — `codrop-net`

For a single manual pull instead of a running daemon:

```bash
codrop-net serve ~/projectA           # prints an endpoint id
codrop-net pull  ~/projectB <id>      # pull projectA's files into projectB, once
```

## How it works

- Every change is **content-addressed** (BLAKE3) into a local blob store and recorded in a
  SQLite index keyed `path → hash → vector clock` (under `.codrop/`).
- Devices sync over **iroh** (QUIC). Peers are addressed by `EndpointId` (an Ed25519 public
  key) that also authenticates them; connectivity escalates direct → hole-punched → relayed.
- **Vector clocks** (not wall-clock time) order changes, so a newer edit is distinguishable
  from a concurrent one. Applying a change is idempotent (same content → no-op), which is what
  prevents sync echo loops.
- On macOS, files are materialized with copy-on-write (`clonefile`); other platforms copy.

## Behavior & limitations

- **Deletes propagate** across devices (as tombstones).
- **Concurrent edits keep both versions** — one wins the canonical path (deterministically); the
  other is preserved under `.codrop/conflicts/<same path>` (same name and folder structure), so
  your working tree stays clean and nothing is silently overwritten.
- Whole files are transferred on change — there's no block-level delta sync yet.
- `.env` and other secrets sync in **cleartext** — don't point Codrop at real secrets yet.

## Layout

Cargo workspace:

```
crates/
├── sync-engine/      content-addressed store + SQLite index + vector clocks
├── transport/        iroh p2p transport + sync protocol  (codrop-net)
├── daemon/           the codrop live-sync daemon
└── watcher-daemon/   standalone filesystem watcher
```

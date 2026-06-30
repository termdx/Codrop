# Codrop

A "Dropbox for devs" — a unified code folder that automatically syncs across all your machines
and cloud agents, with zero manual effort. No `git pull`, no copying `.env` files around, no
building on a stale base.

Codrop watches a folder, content-addresses every change, and syncs it to your other devices
over an encrypted peer-to-peer connection. Devices are identified and authenticated by public
key — you never deal with IP addresses, and it works across LAN, NAT, and restrictive Wi-Fi
(direct when possible, relayed when the network won't allow direct).

## Build & install

There's no hosted package yet, so build from source. Requires **Rust ≥ 1.91** (install via
[rustup](https://rustup.rs)).

```bash
git clone https://github.com/termdx/Codrop.git
cd Codrop
```

### Option A — install the `codrop` command (recommended)

`cargo install` compiles in release mode and copies the binary into `~/.cargo/bin`, which
rustup already puts on your `PATH` — so `codrop` works from anywhere:

```bash
cargo install --path crates/daemon       # installs `codrop`
cargo install --path crates/transport    # optional: installs `codrop-net` (one-shot sync)
```

Verify:

```bash
codrop --version
```

> If `codrop` isn't found afterwards, add Cargo's bin dir to your shell profile:
> `export PATH="$HOME/.cargo/bin:$PATH"`.

### Option B — build and link manually

Build once, then put the binary on your `PATH` yourself (symlink so `git pull` + rebuild
stays current):

```bash
cargo build --release
# binaries are in target/release/: codrop, codrop-net, codrop-watchd
ln -sf "$PWD/target/release/codrop" ~/.local/bin/codrop   # ensure ~/.local/bin is on PATH
```

Or skip installing and just run in place: `./target/release/codrop --help`.

## Usage — the `codrop` daemon

```
codrop run <dir> [--peer <endpoint-id>] [--detach]   watch <dir> and sync it with a peer
codrop id     <dir>                                  print <dir>'s stable endpoint id
codrop status <dir>                                  show connected peers + sync state
codrop stop   <dir>                                  stop the daemon for <dir>
codrop --help | --version
```

Run the daemon on each machine and point one at the other. A **single `--peer` gives
bidirectional sync**.

```bash
# machine B — get its stable id, then start syncing ~/code
codrop id ~/code            #  d951e2ed584d...   (also printed in the run banner)
codrop run ~/code

# machine A — sync ~/code with machine B (one --peer syncs both ways)
codrop run ~/code --peer d951e2ed584d...
```

Now edits in `~/code` on either machine appear on the other within about a second. On connect,
the two sides converge automatically (each receives the other's files); the `--peer` link
reconnects on its own if the network drops.

### Run in the background

```bash
codrop run ~/code --peer d951e2ed584d... --detach   # backgrounds it (new session)
#   codrop: running detached (pid 21063)
#     logs:  ~/code/.codrop/daemon.log

codrop status ~/code        # is it up? who's connected?
#   codrop: running (pid 21063)
#     tracking:    42 files
#     status:      live (1s ago)
#     peers:       1 connected
#       ● e18373aabf1e...

codrop stop ~/code          # stop it
```

- **Stable identity.** A device key lives in `<dir>/.codrop/endpoint.key`, so a device's id is
  the same across restarts. `codrop id <dir>` prints it without starting the daemon.
- **Ignored by default:** `node_modules`, `.git`, `target`, `dist`, `build`, `.next` — the
  OS/toolchain-specific directories you don't want to sync.
- **Git-friendly:** Codrop adds `.codrop/` to the folder's `.gitignore`, so its own state
  never lands in your commits.

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

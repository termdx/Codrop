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
codrop run  <dir> [--peer <endpoint-id>] [--detach]  watch <dir> and sync with its paired peers
codrop pair <dir> <endpoint-id>                      pair with a peer (trust it + dial it)
codrop id     <dir>                                  print <dir>'s stable endpoint id
codrop status <dir>                                  show connected peers + sync state
codrop stop   <dir>                                  stop the daemon for <dir>
codrop --help | --version
```

A daemon only talks to peers it's **paired** with. Pairing is mutual: run `codrop pair` on each
side with the other's id, then just `codrop run`.

```bash
# on each machine: get its id
codrop id ~/code            #  → the endpoint id (also printed in the run banner)

# pair the two (run on BOTH, each with the OTHER's id), then run
codrop pair ~/code <other-id>
codrop run  ~/code
```

Now edits in `~/code` on either machine appear on the other within about a second — new files,
changes, deletes. On connect the two sides converge automatically; the links reconnect on their
own if the network drops. Pairings persist in `<dir>/.codrop/peers`.

**More than two machines?** Pair every machine with every other one — `codrop run` dials all of
them, forming a full mesh so a change on any node reaches all the others. (`--peer <id>` on `run`
is a one-shot shortcut for "pair this id, then run".)

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
- Content is split into content-defined chunks (FastCDC), each stored by hash; a file is a
  manifest of chunk hashes. Syncing transfers only the chunks a peer lacks.
- Devices sync over **iroh** (QUIC). Peers are addressed by `EndpointId` (an Ed25519 public
  key) that also authenticates them; connectivity escalates direct → hole-punched → relayed.
- **Vector clocks** (not wall-clock time) order changes, so a newer edit is distinguishable
  from a concurrent one. Applying a change is idempotent (same content → no-op), which is what
  prevents sync echo loops.
- A daemon only accepts **trusted** peers, incoming content is verified against its hash, and
  peer paths are constrained to the synced tree — so a rogue peer can't read, overwrite, or
  escape your folder.

## Behavior & limitations

- **Deletes propagate** across devices (as tombstones).
- **Concurrent edits keep both versions** — one wins the canonical path (deterministically); the
  other is preserved under `.codrop/conflicts/<same path>` (same name and folder structure), so
  your working tree stays clean and nothing is silently overwritten.
- **Only changed data moves.** Files are split into content-defined chunks; a peer transfers
  just the chunks it's missing, so a small edit to a large file syncs a chunk or two, and
  identical content is deduplicated across files and versions.
- Transport is end-to-end encrypted by iroh, but blobs are stored **unencrypted at rest** under
  `.codrop/` — so don't sync secrets to a device you don't control.

## Layout

Cargo workspace:

```
crates/
├── sync-engine/      content-addressed store + SQLite index + vector clocks
├── transport/        iroh p2p transport + sync protocol  (codrop-net)
├── daemon/           the codrop live-sync daemon
└── watcher-daemon/   standalone filesystem watcher
```

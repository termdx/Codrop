# Codrop

A "Dropbox for devs" — a unified code folder that automatically syncs across all your machines
and cloud agents, with zero manual effort. No `git pull`, no copying `.env` files around, no
building on a stale base.

Codrop watches a folder, content-addresses every change, and syncs it to your other devices
over an encrypted peer-to-peer connection. Devices are identified and authenticated by public
key — you never deal with IP addresses, and it works across LAN, NAT, and restrictive Wi-Fi
(direct when possible, relayed when the network won't allow direct).

## How is this different from Git?

**Short answer:** Git records commits. Codrop mirrors your working folder in real time.

| | Git | Codrop |
|---|---|---|
| **Primary goal** | Version history & collaboration | Live sync of current state across devices |
| **Granularity** | Commit-level snapshots | Byte-level, content-addressed chunks |
| **Conflict model** | Manual merge commits | Vector-clock ordering; conflicts preserved automatically under `.codrop/conflicts/` |
| **Transports** | Central server or fetch/push | Encrypted peer-to-peer over QUIC (iroh), direct → hole-punch → relayed |
| **Auth model** | SSH keys / HTTPS tokens | Ed25519 public keys; peers addressed by key, never IP |
| **Use case fit** | Shipping code, PRs, release history | "I'm on my laptop, now I'm on desktop — where's my WIP?" |

Git and Codrop solve different problems. **Use both:** Git for commits, PRs, and history. Codrop for the *uncommitted stuff* between commits — the `.env`s, the half-written features, the state you'd otherwise shuttle around with `git stash`, USB sticks, or desperate Slack file drops.

> Codrop is not "git but faster." It is "stop using git to sync uncommitted work across machines."

## Compatibility

Codrop is **Unix-only** — the daemon relies on Unix process and file-permission APIs (`setsid`,
exec bits, symlinks), so Windows isn't supported. Prebuilt binaries are published for:

| OS | Architecture | Prebuilt binary | Target triple |
|---|---|:---:|---|
| macOS | Apple Silicon (arm64) | ✅ | `aarch64-apple-darwin` |
| macOS | Intel (x86-64) | ✅ | `x86_64-apple-darwin` |
| Linux | x86-64 (glibc) | ✅ | `x86_64-unknown-linux-gnu` |
| Linux | arm64 / musl | ⚙️ from source | — (build with `cargo build --release`) |
| Windows | any | ❌ | not supported |

Building from source additionally requires **Rust ≥ 1.91**.

## Installation

| Method | Command | Best for |
|---|---|---|
| **Homebrew** | `brew install termdx/tap/codrop` | macOS / Linux with Homebrew |
| **Shell installer** | `curl -LsSf https://github.com/termdx/Codrop/releases/latest/download/codrop-installer.sh \| sh` | quickest one-liner, no toolchain |
| **Prebuilt archive** | [download from Releases](https://github.com/termdx/Codrop/releases/latest) | pinning a version, air-gapped, manual |
| **`cargo install`** | `cargo install codrop` | Rust devs already on cargo, or an arch without a prebuilt |
| **From source** | `git clone` + `cargo install --path crates/daemon` | hacking on Codrop itself |

After any method, verify with `codrop --version`.

### Homebrew

```bash
brew install termdx/tap/codrop
# once the tap is added, `brew install codrop` works too
```

### Shell installer

Detects your platform, downloads the right binary, and puts `codrop` on your `PATH`:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/termdx/Codrop/releases/latest/download/codrop-installer.sh | sh
```

### Prebuilt archive

Grab the `.tar.xz` for your platform from the
[latest release](https://github.com/termdx/Codrop/releases/latest), extract it, and move the
`codrop` binary somewhere on your `PATH`.

### `cargo install`

Published on [crates.io](https://crates.io/crates/codrop). Requires **Rust ≥ 1.91**
([rustup](https://rustup.rs)) — this compiles from source on your machine rather than
downloading a prebuilt binary:

```bash
cargo install codrop
```

> If `codrop` isn't found afterwards, add Cargo's bin dir to your shell profile:
> `export PATH="$HOME/.cargo/bin:$PATH"`.

### From source

Hacking on Codrop itself, or want an unreleased commit:

```bash
git clone https://github.com/termdx/Codrop.git
cd Codrop
cargo install --path crates/daemon       # installs `codrop` into ~/.cargo/bin (on PATH via rustup)
```

Or build and symlink manually (so `git pull` + rebuild stays current):

```bash
cargo build --release                    # binaries land in target/release/
ln -sf "$PWD/target/release/codrop" ~/.local/bin/codrop   # ensure ~/.local/bin is on PATH
```

## Usage — the `codrop` daemon

`<dir>` is **optional on every command** — omit it and Codrop uses the current directory, so you
can just `cd` into a folder and run `codrop …` (git-style). An explicit path still works when you
want to target another folder or script it.

```
codrop run  [<dir>] [--peer <endpoint-id>] [--detach]  watch <dir> and sync with its paired peers
codrop pair [<dir>] <endpoint-id>                      pair with a peer (trust it + dial it)
codrop ignore [<dir>] <file|subdir|glob>               stop syncing matching paths (.codropignore)
codrop id     [<dir>]                                  print <dir>'s stable endpoint id
codrop status [<dir>]                                  show connected peers + sync state
codrop stop   [<dir>]                                  stop the daemon for <dir>
codrop --help | --version
```

A daemon only talks to peers it's **paired** with. Pairing is mutual: run `codrop pair` on each
side with the other's id, then just `codrop run`.

```bash
cd ~/code                   # work from inside the folder — no <dir> needed

# on each machine: get its id
codrop id                   #  → the endpoint id (also printed in the run banner)

# pair the two (run on BOTH, each with the OTHER's id), then run
codrop pair <other-id>
codrop run
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
  OS/toolchain-specific directories you don't want to sync. Add your own with
  `codrop ignore <dir> <pattern>` or by editing `<dir>/.codropignore` (gitignore syntax:
  `*.log`, `.venv/`, `__pycache__`, `!keep.log`). It's the escape hatch for secrets too —
  `codrop ignore ~/code .env` keeps it local. Ignores are **sender-side** (they filter what a
  machine sends) and propagate between peers as `.codropignore` itself syncs; a change to the
  ignore file takes effect on the next `codrop run`.
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

- **Exec bits & symlinks are preserved.** A file's unix permissions travel with it, so an
  executable script (`chmod +x`) stays executable on every machine — even a bare `chmod` with no
  content edit propagates. Symlinks sync as links (recorded by target, never followed and inlined
  as a copy). Mode preservation is a unix feature; Windows isn't supported yet.
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

## License

Codrop is released under the [MIT License](LICENSE).

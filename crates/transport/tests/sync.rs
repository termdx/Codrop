//! Two engines converge over a real QUIC connection on loopback.

use codrop_sync_engine::Engine;
use codrop_transport::{pull, serve};
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

fn engine_with_root(tmp: &std::path::Path, name: &str) -> (Engine, std::path::PathBuf) {
    let root = tmp.join(name);
    fs::create_dir_all(&root).unwrap();
    let engine = Engine::open(&root, root.join(".codrop")).unwrap();
    (engine, root)
}

#[tokio::test]
async fn peer_pull_converges_and_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();

    // --- Source node A: a tree with two files, indexed. ---
    let (a, a_root) = engine_with_root(tmp.path(), "A");
    fs::write(a_root.join("main.rs"), b"fn main() {}").unwrap();
    fs::create_dir_all(a_root.join("nested")).unwrap();
    fs::write(a_root.join("nested/util.rs"), b"pub fn util() {}").unwrap();
    a.observe(&a_root.join("main.rs")).unwrap();
    a.observe(&a_root.join("nested/util.rs")).unwrap();
    let a_device = a.device_id().to_string();

    // Serve A over QUIC on an ephemeral loopback port.
    let endpoint = serve(Arc::new(a), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = endpoint.local_addr().unwrap();

    // --- Empty node B pulls from A. ---
    let (b, b_root) = engine_with_root(tmp.path(), "B");
    let stats = pull(&b, addr).await.unwrap();

    // B fetched both files; nothing skipped or conflicting.
    assert_eq!(stats.total, 2);
    assert_eq!(stats.fetched, 2);
    assert_eq!(stats.conflicts, 0);

    // Files are materialized into B's tree with identical content.
    assert_eq!(fs::read(b_root.join("main.rs")).unwrap(), b"fn main() {}");
    assert_eq!(
        fs::read(b_root.join("nested/util.rs")).unwrap(),
        b"pub fn util() {}"
    );

    // B's index carries A's vector clock (causal history preserved across the wire).
    let rec = b.index().get("main.rs").unwrap().unwrap();
    assert_eq!(rec.vclock.get(&a_device), 1);

    // Pulling again is a no-op: everything is now identical (echo-loop safety).
    let again = pull(&b, addr).await.unwrap();
    assert_eq!(again.fetched, 0);
    assert_eq!(again.skipped, 2);
}

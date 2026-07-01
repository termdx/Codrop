//! Two engines converge over a real iroh connection on loopback (offline: Empty preset,
//! connecting by direct address — no relay/discovery/internet needed).

use codrop_sync_engine::Engine;
use codrop_transport::{connect, pull_on, push, serve_connection, serve_on, ALPN};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, TransportAddr};
use std::fs;
use std::sync::Arc;

fn addr_of(ep: &Endpoint) -> EndpointAddr {
    EndpointAddr::from_parts(ep.id(), ep.bound_sockets().into_iter().map(TransportAddr::Ip))
}

async fn loopback_endpoint() -> Endpoint {
    Endpoint::builder(presets::Empty)
        .crypto_provider(codrop_transport::crypto_provider())
        .alpns(vec![ALPN.to_vec()])
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap()
}

#[tokio::test]
async fn peer_pull_converges_over_iroh() {
    let tmp = tempfile::tempdir().unwrap();

    // --- Source node A: a tree with two files, indexed. ---
    let a_root = tmp.path().join("A");
    fs::create_dir_all(&a_root).unwrap();
    let a = Engine::open(&a_root, a_root.join(".codrop")).unwrap();
    fs::write(a_root.join("main.rs"), b"fn main() {}").unwrap();
    fs::create_dir_all(a_root.join("nested")).unwrap();
    fs::write(a_root.join("nested/util.rs"), b"pub fn util() {}").unwrap();
    a.observe(&a_root.join("main.rs")).unwrap();
    a.observe(&a_root.join("nested/util.rs")).unwrap();
    let a_device = a.device_id().to_string();

    // Serve A on an iroh endpoint, and build its direct loopback address for the client.
    let server_ep = loopback_endpoint().await;
    let server_addr = EndpointAddr::from_parts(
        server_ep.id(),
        server_ep.bound_sockets().into_iter().map(TransportAddr::Ip),
    );
    serve_on(Arc::new(a), &server_ep);

    // --- Empty node B pulls from A. ---
    let b_root = tmp.path().join("B");
    fs::create_dir_all(&b_root).unwrap();
    let b = Engine::open(&b_root, b_root.join(".codrop")).unwrap();
    let client_ep = loopback_endpoint().await;

    let stats = pull_on(&b, &client_ep, server_addr.clone()).await.unwrap();

    assert_eq!(stats.total, 2);
    assert_eq!(stats.fetched, 2);
    assert_eq!(stats.conflicts, 0);

    assert_eq!(fs::read(b_root.join("main.rs")).unwrap(), b"fn main() {}");
    assert_eq!(
        fs::read(b_root.join("nested/util.rs")).unwrap(),
        b"pub fn util() {}"
    );

    // Causal history preserved across the wire.
    let rec = b.index().get("main.rs").unwrap().unwrap();
    assert_eq!(rec.vclock.get(&a_device), 1);

    // Pulling again is a no-op (echo-loop safety).
    let again = pull_on(&b, &client_ep, server_addr).await.unwrap();
    assert_eq!(again.fetched, 0);
    assert_eq!(again.skipped, 2);
}

#[tokio::test]
async fn live_push_applies_on_peer() {
    let tmp = tempfile::tempdir().unwrap();

    // Sender A creates a file and indexes it (its content lands in A's store).
    let a_root = tmp.path().join("A");
    fs::create_dir_all(&a_root).unwrap();
    let a = Arc::new(Engine::open(&a_root, a_root.join(".codrop")).unwrap());
    fs::write(a_root.join("live.rs"), b"// edited live").unwrap();
    a.observe(&a_root.join("live.rs")).unwrap();
    let rec = a.index().get("live.rs").unwrap().unwrap();

    // Receiver B serves (accepts A's connection; on push it pulls the chunks back from A).
    let b_root = tmp.path().join("B");
    fs::create_dir_all(&b_root).unwrap();
    let b = Engine::open(&b_root, b_root.join(".codrop")).unwrap();
    let b_ep = loopback_endpoint().await;
    let b_addr = addr_of(&b_ep);
    serve_on(Arc::new(b), &b_ep);

    // A connects and must ALSO serve its side, so B can fetch the file's chunks over the same
    // connection when it handles the push.
    let a_ep = loopback_endpoint().await;
    let conn = connect(&a_ep, b_addr).await.unwrap();
    tokio::spawn(serve_connection(a.clone(), conn.clone()));
    push(&conn, &rec).await.unwrap();

    // push() returns only after B fetched the chunks + applied, so the file exists now.
    assert_eq!(fs::read(b_root.join("live.rs")).unwrap(), b"// edited live");
}

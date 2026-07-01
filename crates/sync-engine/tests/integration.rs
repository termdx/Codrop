use codrop_sync_engine::{ApplyOutcome, Causality, Engine, FileRecord, VClock};
use std::fs;
use std::path::PathBuf;

/// Two independent engines under one temp dir, for sync/conflict tests.
fn engine_at(tmp: &tempfile::TempDir, name: &str) -> (Engine, PathBuf) {
    let root = tmp.path().join(name);
    fs::create_dir_all(&root).unwrap();
    let engine = Engine::open(&root, root.join(".codrop")).unwrap();
    (engine, root)
}

/// Index a freshly-written file and return its record + content for handing to a peer.
fn record_of(engine: &Engine, root: &std::path::Path, rel: &str) -> (FileRecord, Vec<u8>) {
    let obs = engine.observe(&root.join(rel)).unwrap();
    let rec = engine.index().get(&obs.path).unwrap().unwrap();
    let bytes = engine.store().read(&obs.hash).unwrap().unwrap();
    (rec, bytes)
}

#[test]
fn observe_stores_indexes_and_bumps_clock() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("tree");
    let state = tmp.path().join("state");
    fs::create_dir_all(&root).unwrap();

    let engine = Engine::open(&root, &state).unwrap();
    let f = root.join("a.txt");
    fs::write(&f, b"hello").unwrap();

    // First observation: new file, clock bumped to 1, blob present.
    let obs = engine.observe(&f).unwrap();
    assert!(obs.changed);
    assert_eq!(obs.path, "a.txt");
    assert_eq!(obs.size, 5);
    assert!(engine.store().has(&obs.hash));
    let rec = engine.index().get("a.txt").unwrap().unwrap();
    assert_eq!(rec.vclock.get(engine.device_id()), 1);

    // Re-observe identical content: no change, clock stays at 1 (idempotent).
    let again = engine.observe(&f).unwrap();
    assert!(!again.changed);
    assert_eq!(
        engine.index().get("a.txt").unwrap().unwrap().vclock.get(engine.device_id()),
        1
    );

    // Edit: hash changes, clock advances to 2.
    fs::write(&f, b"world!!").unwrap();
    let edited = engine.observe(&f).unwrap();
    assert!(edited.changed);
    assert_ne!(edited.hash, obs.hash);
    assert_eq!(
        engine.index().get("a.txt").unwrap().unwrap().vclock.get(engine.device_id()),
        2
    );
}

#[test]
fn materialize_roundtrips() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("tree");
    let state = tmp.path().join("state");
    fs::create_dir_all(&root).unwrap();
    let engine = Engine::open(&root, &state).unwrap();

    let f = root.join("data.bin");
    fs::write(&f, b"some bytes here").unwrap();
    let obs = engine.observe(&f).unwrap();

    // Materialize the stored blob to a fresh path (clonefile on APFS, copy elsewhere).
    let dest = tmp.path().join("restored/data.bin");
    engine.store().materialize(&obs.hash, &dest).unwrap();
    assert_eq!(fs::read(&dest).unwrap(), b"some bytes here");
}

#[test]
fn delete_propagates_as_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, a_root) = engine_at(&tmp, "A");
    let (b, b_root) = engine_at(&tmp, "B");

    // A creates foo.txt; B receives it (the transport would deliver content into B's store).
    fs::write(a_root.join("foo.txt"), b"hi").unwrap();
    let (rec, bytes) = record_of(&a, &a_root, "foo.txt");
    b.store().put_bytes(&bytes).unwrap();
    assert_eq!(b.apply_incoming(&rec).unwrap(), ApplyOutcome::Applied);
    assert!(b_root.join("foo.txt").exists());

    // A deletes it → tombstone; B applies the tombstone → file removed, row marked deleted.
    fs::remove_file(a_root.join("foo.txt")).unwrap();
    let tomb = a.observe_delete(&a_root.join("foo.txt")).unwrap().unwrap();
    assert!(tomb.deleted);
    assert_eq!(b.apply_incoming(&tomb).unwrap(), ApplyOutcome::Applied);
    assert!(!b_root.join("foo.txt").exists());
    assert!(b.index().get("foo.txt").unwrap().unwrap().deleted);

    // Re-applying the tombstone is a no-op.
    assert_eq!(b.apply_incoming(&tomb).unwrap(), ApplyOutcome::Skipped);
}

#[test]
fn concurrent_edits_keep_both() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, a_root) = engine_at(&tmp, "A");
    let (b, b_root) = engine_at(&tmp, "B");

    // Both create foo.txt independently with different content (concurrent clocks).
    fs::write(a_root.join("foo.txt"), b"AAA").unwrap();
    let (arec, abytes) = record_of(&a, &a_root, "foo.txt");
    fs::write(b_root.join("foo.txt"), b"BBB").unwrap();
    let (brec, bbytes) = record_of(&b, &b_root, "foo.txt");

    // The transport would deliver the other side's content into each store before applying.
    a.store().put_bytes(&bbytes).unwrap();
    b.store().put_bytes(&abytes).unwrap();

    // Each applies the other's version → conflict, kept both.
    let out_a = a.apply_incoming(&brec).unwrap();
    let out_b = b.apply_incoming(&arec).unwrap();
    assert!(matches!(out_a, ApplyOutcome::Conflicted { .. }));
    assert!(matches!(out_b, ApplyOutcome::Conflicted { .. }));

    // Deterministic: both sides converge to the same winner at the canonical path, and the
    // working tree stays clean (no extra files alongside foo.txt).
    assert_eq!(
        fs::read(a_root.join("foo.txt")).unwrap(),
        fs::read(b_root.join("foo.txt")).unwrap()
    );
    for root in [&a_root, &b_root] {
        let tree: Vec<_> = fs::read_dir(root)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n != ".codrop" && n != ".gitignore")
            .collect();
        assert_eq!(tree, vec![std::ffi::OsString::from("foo.txt")]);
    }

    // The losing version is preserved under .codrop/conflicts/<path> (same name) on each side,
    // and is NOT indexed (it doesn't sync).
    for (e, root) in [(&a, &a_root), (&b, &b_root)] {
        let backup = root.join(".codrop/conflicts/foo.txt");
        assert!(backup.exists(), "conflict backup missing under .codrop/conflicts");
        let mut got = vec![
            fs::read(root.join("foo.txt")).unwrap(),
            fs::read(&backup).unwrap(),
        ];
        got.sort();
        assert_eq!(got, vec![b"AAA".to_vec(), b"BBB".to_vec()]);
        assert_eq!(e.local_records().unwrap().len(), 1); // only foo.txt is indexed
    }

    // Re-applying converges (identical content → Skip, no new conflict).
    assert_eq!(a.apply_incoming(&brec).unwrap(), ApplyOutcome::Skipped);
}

#[test]
fn rejects_unsafe_peer_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let (e, _root) = engine_at(&tmp, "E");

    let evil = |path: &str, deleted: bool| FileRecord {
        path: path.into(),
        hash: "00".into(),
        size: 0,
        vclock: VClock::new(),
        updated_ms: 0,
        deleted,
    };
    // Absolute, parent-traversal, and a traversal tombstone are all rejected before touching fs.
    assert!(e.apply_incoming(&evil("/etc/pwned", false)).is_err());
    assert!(e.apply_incoming(&evil("../escape.txt", false)).is_err());
    assert!(e.apply_incoming(&evil("../../.ssh/authorized_keys", true)).is_err());
    assert!(e.apply_incoming(&evil("a/../../b", false)).is_err());
}

#[test]
fn verify_content_catches_tampering() {
    use codrop_sync_engine::BlobStore;
    let tmp = tempfile::tempdir().unwrap();
    let store = BlobStore::open(tmp.path()).unwrap();

    let real = store.put_bytes(b"the real content").unwrap();
    assert!(store.verify_content(&real).unwrap());

    // A lying manifest: point a bogus full-hash at the real (individually-valid) chunks.
    // Reassembly won't equal the claimed hash, so verification must fail.
    let chunks = store.get_manifest(&real).unwrap().unwrap();
    let fake = "0".repeat(64);
    store.put_manifest(&fake, &chunks).unwrap();
    assert!(!store.verify_content(&fake).unwrap());
}

#[test]
fn concurrent_delete_vs_edit_keeps_edit() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, a_root) = engine_at(&tmp, "A");
    let (b, b_root) = engine_at(&tmp, "B");

    // Both start with foo.txt in sync.
    fs::write(a_root.join("foo.txt"), b"v1").unwrap();
    let (rec1, bytes1) = record_of(&a, &a_root, "foo.txt");
    b.store().put_bytes(&bytes1).unwrap();
    b.apply_incoming(&rec1).unwrap();

    // A deletes; B edits — concurrent.
    fs::remove_file(a_root.join("foo.txt")).unwrap();
    let tomb = a.observe_delete(&a_root.join("foo.txt")).unwrap().unwrap();
    fs::write(b_root.join("foo.txt"), b"v2-edited").unwrap();
    let (rec2, bytes2) = record_of(&b, &b_root, "foo.txt");

    // B applies A's delete → its edit wins (file kept).
    assert_eq!(b.apply_incoming(&tomb).unwrap(), ApplyOutcome::ConflictKeptLocal);
    assert_eq!(fs::read(b_root.join("foo.txt")).unwrap(), b"v2-edited");

    // A applies B's edit → resurrects over its own delete.
    a.store().put_bytes(&bytes2).unwrap();
    assert_eq!(a.apply_incoming(&rec2).unwrap(), ApplyOutcome::Applied);
    assert_eq!(fs::read(a_root.join("foo.txt")).unwrap(), b"v2-edited");
}

#[test]
fn concurrent_delete_vs_delete_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, a_root) = engine_at(&tmp, "A");
    let (b, b_root) = engine_at(&tmp, "B");

    fs::write(a_root.join("foo.txt"), b"v1").unwrap();
    let (rec1, bytes1) = record_of(&a, &a_root, "foo.txt");
    b.store().put_bytes(&bytes1).unwrap();
    b.apply_incoming(&rec1).unwrap();

    // Both delete concurrently, then exchange tombstones.
    fs::remove_file(a_root.join("foo.txt")).unwrap();
    let tomb_a = a.observe_delete(&a_root.join("foo.txt")).unwrap().unwrap();
    fs::remove_file(b_root.join("foo.txt")).unwrap();
    let tomb_b = b.observe_delete(&b_root.join("foo.txt")).unwrap().unwrap();

    b.apply_incoming(&tomb_a).unwrap();
    a.apply_incoming(&tomb_b).unwrap();

    // Both converge to "deleted"; no resurrection.
    assert!(!a_root.join("foo.txt").exists());
    assert!(!b_root.join("foo.txt").exists());
    assert!(a.index().get("foo.txt").unwrap().unwrap().deleted);
    assert!(b.index().get("foo.txt").unwrap().unwrap().deleted);
}

#[test]
fn chunking_dedups_and_deltas() {
    use codrop_sync_engine::BlobStore;
    let tmp = tempfile::tempdir().unwrap();
    let store = BlobStore::open(tmp.path()).unwrap();
    let objects = tmp.path().join("objects");

    // A large pseudo-random blob → many distinct chunks.
    let mut seed = 0x1234_5678u64;
    let mut a = vec![0u8; 300_000];
    for byte in a.iter_mut() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *byte = (seed >> 33) as u8;
    }
    let ha = store.put_bytes(&a).unwrap();
    let objs1 = count_files(&objects);
    assert!(objs1 > 5, "expected the blob to split into several chunks, got {objs1}");

    // Storing identical content again adds nothing (full dedup).
    store.put_bytes(&a).unwrap();
    assert_eq!(count_files(&objects), objs1);

    // A one-byte edit changes only a chunk or two (content-defined boundaries localize it).
    let mut b = a.clone();
    b[150_000] ^= 0xFF;
    let hb = store.put_bytes(&b).unwrap();
    assert_ne!(ha, hb);
    let new = count_files(&objects) - objs1;
    assert!((1..5).contains(&new), "expected a few new chunks, got {new} of {objs1}");

    // Reassembly is exact for both versions.
    assert_eq!(store.read(&ha).unwrap().unwrap(), a);
    assert_eq!(store.read(&hb).unwrap().unwrap(), b);
}

fn count_files(dir: &std::path::Path) -> usize {
    let mut n = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = fs::read_dir(&d) else { continue };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                n += 1;
            }
        }
    }
    n
}

#[test]
fn state_dir_is_gitignored() {
    let tmp = tempfile::tempdir().unwrap();
    let (_a, root) = engine_at(&tmp, "A");

    // Opening the engine adds .codrop/ to the root .gitignore.
    let gi = fs::read_to_string(root.join(".gitignore")).unwrap();
    assert!(gi.lines().any(|l| l.trim().trim_end_matches('/') == ".codrop"));

    // Idempotent and non-destructive: a pre-existing .gitignore keeps its entries and gets
    // .codrop appended exactly once.
    fs::write(root.join(".gitignore"), "target/\n").unwrap();
    let _b = Engine::open(&root, root.join(".codrop")).unwrap();
    let gi2 = fs::read_to_string(root.join(".gitignore")).unwrap();
    assert!(gi2.contains("target/"));
    assert_eq!(gi2.matches(".codrop").count(), 1);
}

#[test]
fn vclock_detects_concurrency() {
    let mut a = VClock::new();
    let mut b = VClock::new();
    assert_eq!(a.compare(&b), Causality::Equal);

    a.increment("dev1");
    assert_eq!(a.compare(&b), Causality::After);
    assert_eq!(b.compare(&a), Causality::Before);

    // Independent edit on another device => concurrent (a real conflict).
    b.increment("dev2");
    assert_eq!(a.compare(&b), Causality::Concurrent);

    // Merging takes the per-device max and dominates both inputs.
    let mut merged = a.clone();
    merged.merge(&b);
    assert_eq!(merged.compare(&a), Causality::After);
    assert_eq!(merged.compare(&b), Causality::After);
}

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

    // A creates foo.txt; B receives it.
    fs::write(a_root.join("foo.txt"), b"hi").unwrap();
    let (rec, bytes) = record_of(&a, &a_root, "foo.txt");
    assert_eq!(b.apply_incoming(&rec, &bytes).unwrap(), ApplyOutcome::Applied);
    assert!(b_root.join("foo.txt").exists());

    // A deletes it → tombstone; B applies the tombstone → file removed, row marked deleted.
    fs::remove_file(a_root.join("foo.txt")).unwrap();
    let tomb = a.observe_delete(&a_root.join("foo.txt")).unwrap().unwrap();
    assert!(tomb.deleted);
    assert_eq!(b.apply_incoming(&tomb, &[]).unwrap(), ApplyOutcome::Applied);
    assert!(!b_root.join("foo.txt").exists());
    assert!(b.index().get("foo.txt").unwrap().unwrap().deleted);

    // Re-applying the tombstone is a no-op.
    assert_eq!(b.apply_incoming(&tomb, &[]).unwrap(), ApplyOutcome::Skipped);
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

    // Each applies the other's version → conflict, kept both.
    let out_a = a.apply_incoming(&brec, &bbytes).unwrap();
    let out_b = b.apply_incoming(&arec, &abytes).unwrap();
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
    assert_eq!(a.apply_incoming(&brec, &bbytes).unwrap(), ApplyOutcome::Skipped);
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

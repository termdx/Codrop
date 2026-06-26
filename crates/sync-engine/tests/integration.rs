use codrop_sync_engine::{Causality, Engine, VClock};
use std::fs;

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

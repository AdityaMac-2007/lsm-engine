use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::engine::{EngineOptions, LsmEngine};
use lsm_engine::wal::FsyncPolicy;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir()
            .join(format!("lsm_m5_{name}_{}_{}", std::process::id(), nanos));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn mvcc_snapshot_isolation_and_watermark_gc() {
    let dir = TempDir::new("mvcc");
    let opts = EngineOptions {
        fsync_policy: FsyncPolicy::Never,
        memtable_threshold: 512,
        l0_compaction_trigger: 2,
        ..Default::default()
    };

    let db = LsmEngine::open(&dir.path, opts).expect("open db");
    db.put(b"account", b"100").expect("put 100");
    let snap1 = db.snapshot();

    // Mutate and delete after snap1, flushing multiple L0s to trigger compaction.
    db.put(b"account", b"200").expect("put 200");
    db.flush().expect("flush 1");
    db.delete(b"account").expect("delete");
    db.flush().expect("flush 2");
    db.compact_now().expect("compact with active snap");

    // Current view sees None (deleted), but snap1 still sees "100".
    assert_eq!(db.get(b"account").expect("get current"), None);
    assert_eq!(snap1.get(b"account").expect("get snap1"), Some(b"100".to_vec()));

    // Drop snap1, write a new L0 batch, and compact again -> watermark allows GC.
    drop(snap1);
    db.put(b"other", b"x").expect("put other");
    db.flush().expect("flush 3");
    db.put(b"other2", b"y").expect("put other2");
    db.flush().expect("flush 4");
    db.compact_now().expect("compact after snap drop");

    assert_eq!(db.get(b"account").expect("get after gc"), None);
    assert_eq!(db.get(b"other").expect("get other"), Some(b"x".to_vec()));
}

#[test]
fn property_based_oracle_matches_btreemap() {
    let dir = TempDir::new("oracle");
    let opts = EngineOptions {
        fsync_policy: FsyncPolicy::Never,
        memtable_threshold: 1024,
        l0_compaction_trigger: 3,
        target_sst_bytes: 2048,
        ..Default::default()
    };

    let db = LsmEngine::open(&dir.path, opts).expect("open db");
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng: u64 = 0xDEAD_BEEF_CAFE_BABEu64;

    let mut next_rand = || -> u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    for step in 0..2_000u64 {
        let key_id = next_rand() % 150;
        let key = format!("k_{key_id:04}").into_bytes();
        let op = next_rand() % 10;

        if op < 6 {
            let val = format!("v_{step}_{}", next_rand() % 9999).into_bytes();
            db.put(&key, &val).expect("put");
            oracle.insert(key, val);
        } else if op < 8 {
            db.delete(&key).expect("delete");
            oracle.remove(&key);
        } else {
            assert_eq!(db.get(&key).expect("get"), oracle.get(&key).cloned());
        }

        if step % 250 == 249 {
            db.flush().expect("flush");
            db.compact_now().expect("compact");
        }
    }

    // Verify full scan matches BTreeMap oracle.
    let db_all = db.scan(b"k_0000", b"k_9999").expect("scan");
    let oracle_all: Vec<(Vec<u8>, Vec<u8>)> = oracle.into_iter().collect();
    assert_eq!(db_all, oracle_all);
}

#[test]
fn sigkill_crash_recovery_preserves_acknowledged_writes() {
    // Subprocess mode when invoked as child crash target.
    if let Ok(target_dir) = std::env::var("LSM_SIGKILL_CHILD_DIR") {
        let opts = EngineOptions {
            fsync_policy: FsyncPolicy::Always,
            ..Default::default()
        };
        let db = LsmEngine::open(&target_dir, opts).expect("child open");
        for i in 0..200u64 {
            let k = format!("crash_key_{i:04}").into_bytes();
            let v = format!("crash_val_{i:04}").into_bytes();
            db.put(&k, &v).expect("child put");
        }
        // Signal readiness to parent via stdout, then spin until SIGKILL.
        println!("READY");
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    let dir = TempDir::new("sigkill");
    let exe = std::env::current_exe().expect("current exe");
    let mut child = Command::new(exe)
        .arg("--exact")
        .arg("sigkill_crash_recovery_preserves_acknowledged_writes")
        .arg("--nocapture")
        .env("LSM_SIGKILL_CHILD_DIR", &dir.path)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn child");

    use std::io::{BufRead, BufReader};
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut ready = false;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).expect("read line");
        if n == 0 {
            break;
        }
        if line.trim() == "READY" {
            ready = true;
            break;
        }
    }
    assert!(ready, "child never signaled READY");

    // Send SIGKILL (kill -9)
    let _ = Command::new("kill")
        .arg("-9")
        .arg(child.id().to_string())
        .status();
    let _ = child.wait();

    // Recover and verify all 200 acknowledged writes survived.
    let db = LsmEngine::open(&dir.path, EngineOptions::default()).expect("recover db");
    for i in 0..200u64 {
        let k = format!("crash_key_{i:04}").into_bytes();
        let v = format!("crash_val_{i:04}").into_bytes();
        assert_eq!(db.get(&k).expect("get after sigkill"), Some(v));
    }
}

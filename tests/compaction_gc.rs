use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::compaction::{compact_sstables, DEFAULT_TARGET_SST_BYTES};
use lsm_engine::memtable::{LookupResult, MemTable};
use lsm_engine::sstable::{SstableBuilder, SstableReader};

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
            .join(format!("lsm_cmp_{name}_{}_{}", std::process::id(), nanos));
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
fn k_way_merge_preserves_mvcc_above_watermark_and_purges_tombstones_only_at_bottommost() {
    let dir = TempDir::new("tombstone_gc");
    let id_gen = AtomicU64::new(10);

    // SST 1 (older): k1=v1@1, k2=v2@2, k3=v3@3
    let mt1 = MemTable::new(1);
    mt1.put(b"k1".to_vec(), 1, b"v1".to_vec());
    mt1.put(b"k2".to_vec(), 2, b"v2".to_vec());
    mt1.put(b"k3".to_vec(), 3, b"v3".to_vec());
    let s1 = SstableBuilder::build_from_memtable(1, 0, dir.path.join("s1.sst"), &mt1).expect("s1");

    // SST 2 (newer): k1=tombstone@4, k2=v2_new@5, k2=v2_newest@10
    let mt2 = MemTable::new(2);
    mt2.delete(b"k1".to_vec(), 4);
    mt2.put(b"k2".to_vec(), 5, b"v2_new".to_vec());
    mt2.put(b"k2".to_vec(), 10, b"v2_newest".to_vec());
    let s2 = SstableBuilder::build_from_memtable(2, 0, dir.path.join("s2.sst"), &mt2).expect("s2");

    // Case A: Non-bottommost compaction with watermark = 6.
    // - k1@4 (Tombstone) MUST be kept (not bottommost).
    // - k2@10 (> watermark 6) MUST be kept, k2@5 (visible at 6) kept, k2@2 (shadowed) dropped!
    // - k3@3 kept.
    let (out_non_bottom, _) = compact_sstables(
        &[s1.clone(), s2.clone()],
        1,
        false,
        6,
        DEFAULT_TARGET_SST_BYTES,
        || id_gen.fetch_add(1, Ordering::SeqCst),
        &dir.path,
    )
    .expect("compact non-bottom");

    assert_eq!(out_non_bottom.len(), 1);
    let mut r_nb = SstableReader::open(&out_non_bottom[0].path).expect("open nb");
    assert_eq!(
        r_nb.get(b"k1", 6, true).expect("get k1"),
        Some(LookupResult::Tombstone)
    );
    assert_eq!(
        r_nb.get(b"k2", 6, true).expect("get k2@6"),
        Some(LookupResult::Value(b"v2_new".to_vec()))
    );
    assert_eq!(
        r_nb.get(b"k2", 10, true).expect("get k2@10"),
        Some(LookupResult::Value(b"v2_newest".to_vec()))
    );
    // k2@2 was shadowed and dropped
    let all_nb = r_nb.read_all_entries().expect("all nb");
    assert!(!all_nb.iter().any(|(k, _)| k.user_key == b"k2" && k.seq == 2));

    // Case B: Bottommost compaction with watermark = u64::MAX (no active snapshots).
    // - k1@4 (Tombstone) MUST be purged!
    // - k2 keeps only k2@10.
    // - k3 keeps k3@3.
    let (out_bottom, _) = compact_sstables(
        &[s1, s2],
        2,
        true,
        u64::MAX,
        DEFAULT_TARGET_SST_BYTES,
        || id_gen.fetch_add(1, Ordering::SeqCst),
        &dir.path,
    )
    .expect("compact bottom");

    assert_eq!(out_bottom.len(), 1);
    let mut r_b = SstableReader::open(&out_bottom[0].path).expect("open bottom");
    let all_b = r_b.read_all_entries().expect("all bottom");
    assert_eq!(all_b.len(), 2);
    assert_eq!(all_b[0].0.user_key, b"k2");
    assert_eq!(all_b[0].0.seq, 10);
    assert_eq!(all_b[1].0.user_key, b"k3");
    assert_eq!(all_b[1].0.seq, 3);
}

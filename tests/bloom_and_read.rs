use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::memtable::{LookupResult, MemTable};
use lsm_engine::sstable::{BloomFilter, SstableBuilder, SstableReader};

struct TempSst {
    path: PathBuf,
}

impl TempSst {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir()
            .join(format!("lsm_m3_{name}_{}_{}.sst", std::process::id(), nanos));
        Self { path }
    }
}

impl Drop for TempSst {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn bloom_filter_has_zero_false_negatives_and_low_fpr() {
    let keys: Vec<Vec<u8>> = (0..10_000u64)
        .map(|i| format!("present_key_{i:06}").into_bytes())
        .collect();
    let bloom = BloomFilter::build(keys.iter().map(|k| k.as_slice()), keys.len(), 10);

    for k in &keys {
        assert!(bloom.may_contain(k), "false negative on existing key");
    }

    let mut false_positives = 0usize;
    let probes = 10_000u64;
    for i in 0..probes {
        let missing = format!("missing_key_{i:06}").into_bytes();
        if bloom.may_contain(&missing) {
            false_positives += 1;
        }
    }
    // At 10 bits/key, theoretical FPR is ~0.82%; assert < 2.5%
    assert!(
        false_positives < 250,
        "unexpectedly high FPR: {false_positives}/{probes}"
    );
}

#[test]
fn sstable_get_with_bloom_short_circuits_disk_reads_and_respects_mvcc() {
    let tmp = TempSst::new("get_bloom");
    let mt = MemTable::new(1);

    for i in 0..300u64 {
        let key = format!("k_{i:05}").into_bytes();
        mt.put(key, i + 1, format!("val_{i:05}").into_bytes());
    }
    // Multi-version key spanning updates & delete
    mt.put(b"k_00150".to_vec(), 1000, b"v_1000".to_vec());
    mt.delete(b"k_00150".to_vec(), 1001);

    SstableBuilder::build_from_memtable(1, 0, &tmp.path, &mt).expect("build");

    let mut reader = SstableReader::open(&tmp.path).expect("open");
    assert!(reader.footer().filter_len > 0);

    // Non-existent keys should be rejected by Bloom filter with 0 block reads.
    let before = reader.block_reads();
    for i in 0..100u64 {
        let missing = format!("absent_{i:05}").into_bytes();
        let res = reader.get(&missing, u64::MAX, true).expect("get");
        assert_eq!(res, None);
    }
    // At least 95% of 100 absent probes should do 0 disk block reads.
    let reads_with_bloom = reader.block_reads() - before;
    assert!(reads_with_bloom <= 5, "too many block reads: {reads_with_bloom}");

    // Verify MVCC sequence visibility on k_00150
    assert_eq!(
        reader.get(b"k_00150", 500, true).expect("get"),
        Some(LookupResult::Value(b"val_00150".to_vec()))
    );
    assert_eq!(
        reader.get(b"k_00150", 1000, true).expect("get"),
        Some(LookupResult::Value(b"v_1000".to_vec()))
    );
    assert_eq!(
        reader.get(b"k_00150", 1001, true).expect("get"),
        Some(LookupResult::Tombstone)
    );

    // Verify range scan
    let scanned = reader.scan(b"k_00010", b"k_00012").expect("scan");
    assert_eq!(scanned.len(), 3);
    assert_eq!(scanned[0].0.user_key, b"k_00010");
    assert_eq!(scanned[2].0.user_key, b"k_00012");
}

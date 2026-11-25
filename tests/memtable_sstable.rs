use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::memtable::{InternalKey, LookupResult, MemTable};
use lsm_engine::sstable::{SstableBuilder, SstableReader, BLOCK_SIZE};
use lsm_engine::wal::RecordKind;

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
            .join(format!("lsm_sst_{name}_{}_{}.sst", std::process::id(), nanos));
        Self { path }
    }
}

impl Drop for TempSst {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn internal_key_sorts_user_key_asc_and_seq_desc() {
    let k_new = InternalKey::new(b"alpha".to_vec(), 10, RecordKind::Put);
    let k_old = InternalKey::new(b"alpha".to_vec(), 5, RecordKind::Put);
    let k_other = InternalKey::new(b"beta".to_vec(), 100, RecordKind::Put);

    assert!(k_new < k_old);
    assert!(k_old < k_other);
}

#[test]
fn memtable_threshold_triggers_freeze_and_mvcc_lookup() {
    let mt = MemTable::with_threshold(1, 1024);
    assert!(!mt.is_frozen());

    assert!(!mt.put(b"k1".to_vec(), 1, vec![b'a'; 100]));
    assert!(!mt.put(b"k1".to_vec(), 2, vec![b'b'; 100]));
    assert!(!mt.delete(b"k1".to_vec(), 3));

    assert_eq!(mt.get(b"k1", 1), Some(LookupResult::Value(vec![b'a'; 100])));
    assert_eq!(mt.get(b"k1", 2), Some(LookupResult::Value(vec![b'b'; 100])));
    assert_eq!(mt.get(b"k1", 3), Some(LookupResult::Tombstone));

    let should_freeze = mt.put(b"k2".to_vec(), 4, vec![b'c'; 900]);
    assert!(should_freeze);
    mt.freeze();
    assert!(mt.is_frozen());
}

#[test]
fn flush_memtable_to_4kb_aligned_sstable_and_read_back() {
    let tmp = TempSst::new("flush_l0");
    let mt = MemTable::new(1);

    // Insert enough entries to span multiple 4KB Data Blocks, plus a >4KB value.
    for i in 0..120u64 {
        let key = format!("user_key_{i:04}").into_bytes();
        let val = format!("payload_value_{i:04}_padding_0123456789abcdef").into_bytes();
        mt.put(key, i + 1, val);
    }
    mt.put(b"user_key_0050".to_vec(), 500, b"updated_50".to_vec());
    mt.delete(b"user_key_0060".to_vec(), 501);
    mt.put(b"zzz_large".to_vec(), 502, vec![b'X'; 5000]);

    let expected = mt.snapshot_entries();
    let meta = SstableBuilder::build_from_memtable(1, 0, &tmp.path, &mt).expect("flush sst");
    assert_eq!(meta.entry_count, expected.len() as u64);

    let mut reader = SstableReader::open(&tmp.path).expect("open sst");
    assert!(reader.index().len() >= 3);

    for idx_entry in reader.index() {
        assert_eq!(idx_entry.block_offset % BLOCK_SIZE as u64, 0);
        assert_eq!(idx_entry.block_len % BLOCK_SIZE as u64, 0);
    }

    let recovered = reader.read_all_entries().expect("read all");
    assert_eq!(recovered, expected);
}

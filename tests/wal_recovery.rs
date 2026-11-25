use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::error::{StopReason, WalError};
use lsm_engine::wal::{recover, FsyncPolicy, Record, RecordKind, WalStorage, WalWriter};

struct TempWal {
    path: PathBuf,
}

impl TempWal {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir()
            .join(format!("lsm_wal_{name}_{}_{}.log", std::process::id(), nanos));
        Self { path }
    }
}

impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn truncate_at_every_byte_offset() {
    let tmp = TempWal::new("every_offset");
    let mut writer = WalWriter::open(&tmp.path, FsyncPolicy::Always, 1).expect("open");

    let inputs = vec![
        (RecordKind::Put, b"alpha".to_vec(), b"value-1".to_vec()),
        (RecordKind::Put, b"beta".to_vec(), b"value-2222".to_vec()),
        (RecordKind::Tombstone, b"alpha".to_vec(), Vec::new()),
        (RecordKind::Put, b"gamma".to_vec(), b"value-333333".to_vec()),
    ];

    let mut frame_end_offsets = Vec::new();
    let mut expected_records = Vec::new();
    for (kind, key, val) in &inputs {
        let seq = writer.append(*kind, key, val).expect("append");
        let len = fs::metadata(&tmp.path).expect("meta").len();
        frame_end_offsets.push(len);
        expected_records.push(Record {
            seq,
            kind: *kind,
            key: key.clone(),
            value: val.clone(),
        });
    }
    drop(writer);

    let full_bytes = fs::read(&tmp.path).expect("read full");
    let file_len = full_bytes.len() as u64;

    for cut in 0..=file_len {
        let probe = TempWal::new(&format!("cut_{cut}"));
        fs::write(&probe.path, &full_bytes[..cut as usize]).expect("write prefix");

        let expected_count = frame_end_offsets.iter().filter(|&&end| end <= cut).count();
        let (recs, report) = recover(&probe.path).expect("recover");

        assert_eq!(recs, expected_records[..expected_count], "cut={cut}");
        assert_eq!(report.records, expected_count as u64, "cut={cut}");
        let expected_valid = if expected_count == 0 {
            0
        } else {
            frame_end_offsets[expected_count - 1]
        };
        assert_eq!(report.valid_len, expected_valid, "cut={cut}");
        assert_eq!(fs::metadata(&probe.path).expect("meta").len(), expected_valid);
    }
}

#[test]
fn flip_every_single_bit_in_last_frame() {
    let tmp = TempWal::new("bitflip");
    let mut writer = WalWriter::open(&tmp.path, FsyncPolicy::Always, 1).expect("open");
    let seq1 = writer.append(RecordKind::Put, b"k1", b"v1").expect("append 1");
    let first_end = fs::metadata(&tmp.path).expect("meta").len() as usize;
    let _seq2 = writer.append(RecordKind::Put, b"k2", b"v2").expect("append 2");
    drop(writer);

    let full_bytes = fs::read(&tmp.path).expect("read full");
    let expected_first = vec![Record {
        seq: seq1,
        kind: RecordKind::Put,
        key: b"k1".to_vec(),
        value: b"v1".to_vec(),
    }];

    for byte_idx in first_end..full_bytes.len() {
        for bit in 0..8u8 {
            let mut corrupted = full_bytes.clone();
            corrupted[byte_idx] ^= 1 << bit;

            let probe = TempWal::new(&format!("flip_{byte_idx}_{bit}"));
            fs::write(&probe.path, &corrupted).expect("write corrupted");

            let (recs, report) = recover(&probe.path).expect("recover");
            assert_eq!(recs, expected_first, "byte={byte_idx} bit={bit}");
            assert_eq!(report.valid_len, first_end as u64);
            assert_eq!(report.last_seq, seq1);
        }
    }
}

#[test]
fn oversized_klen_garbage_truncates_without_oom() {
    let tmp = TempWal::new("oversized_klen");
    let mut writer = WalWriter::open(&tmp.path, FsyncPolicy::Always, 1).expect("open");
    let seq1 = writer.append(RecordKind::Put, b"ok", b"val").expect("append");
    let valid_end = fs::metadata(&tmp.path).expect("meta").len();
    drop(writer);

    let mut garbage = Vec::with_capacity(21);
    garbage.extend_from_slice(&0x1234_5678u32.to_le_bytes());
    garbage.extend_from_slice(&(seq1 + 1).to_le_bytes());
    garbage.push(RecordKind::Put as u8);
    garbage.extend_from_slice(&u32::MAX.to_le_bytes());
    garbage.extend_from_slice(&0u32.to_le_bytes());

    let mut f = OpenOptions::new().append(true).open(&tmp.path).expect("open");
    f.write_all(&garbage).expect("write garbage");
    drop(f);

    let (recs, report) = recover(&tmp.path).expect("recover");
    assert_eq!(recs.len(), 1);
    assert_eq!(report.valid_len, valid_end);
    assert_eq!(report.stop, StopReason::BadLen);
    assert_eq!(fs::metadata(&tmp.path).expect("meta").len(), valid_end);
}

#[test]
fn zero_filled_tail_truncates_with_bad_type() {
    let tmp = TempWal::new("zero_tail");
    let mut writer = WalWriter::open(&tmp.path, FsyncPolicy::Always, 1).expect("open");
    writer.append(RecordKind::Put, b"k", b"v").expect("append");
    let valid_end = fs::metadata(&tmp.path).expect("meta").len();
    drop(writer);

    let mut f = OpenOptions::new().append(true).open(&tmp.path).expect("open");
    f.write_all(&[0u8; 64]).expect("write zeros");
    drop(f);

    let (recs, report) = recover(&tmp.path).expect("recover");
    assert_eq!(recs.len(), 1);
    assert_eq!(report.valid_len, valid_end);
    assert_eq!(report.stop, StopReason::BadType);
    assert_eq!(fs::metadata(&tmp.path).expect("meta").len(), valid_end);
}

#[test]
fn recover_is_idempotent() {
    let tmp = TempWal::new("idempotent");
    let mut writer = WalWriter::open(&tmp.path, FsyncPolicy::Always, 1).expect("open");
    writer.append(RecordKind::Put, b"a", b"1").expect("append 1");
    writer.append(RecordKind::Tombstone, b"a", b"").expect("append 2");
    drop(writer);

    let mut f = OpenOptions::new().append(true).open(&tmp.path).expect("open");
    f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).expect("append torn");
    drop(f);

    let (recs1, rep1) = recover(&tmp.path).expect("first recover");
    let (recs2, rep2) = recover(&tmp.path).expect("second recover");

    assert_eq!(recs1, recs2);
    assert_eq!(rep1.records, 2);
    assert_eq!(rep2.records, 2);
    assert_eq!(rep1.valid_len, rep2.valid_len);
    assert_eq!(rep1.last_seq, rep2.last_seq);
    assert_eq!(rep2.stop, StopReason::CleanEof);
    assert_eq!(rep2.file_len, rep2.valid_len);
}

struct FailingStorage {
    fail_write: Arc<AtomicBool>,
    fail_sync: Arc<AtomicBool>,
    sync_calls: Arc<AtomicUsize>,
}

impl Write for FailingStorage {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.fail_write.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::Other, "injected write error"));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WalStorage for FailingStorage {
    fn sync_data(&self) -> io::Result<()> {
        self.sync_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::Other, "injected fsync error"));
        }
        Ok(())
    }
}

#[test]
fn writer_poisons_on_write_or_fsync_failure_and_never_retries_fsync() {
    let fail_write = Arc::new(AtomicBool::new(true));
    let fail_sync = Arc::new(AtomicBool::new(false));
    let sync_calls = Arc::new(AtomicUsize::new(0));

    let mut writer = WalWriter::from_storage(
        PathBuf::from("mock_write.wal"),
        FailingStorage {
            fail_write: Arc::clone(&fail_write),
            fail_sync: Arc::clone(&fail_sync),
            sync_calls: Arc::clone(&sync_calls),
        },
        FsyncPolicy::Always,
        1,
        0,
    );

    assert!(matches!(
        writer.append(RecordKind::Put, b"k", b"v"),
        Err(WalError::Io(_))
    ));
    fail_write.store(false, Ordering::SeqCst);
    assert!(matches!(
        writer.append(RecordKind::Put, b"k", b"v"),
        Err(WalError::Poisoned)
    ));
    assert!(matches!(writer.sync(), Err(WalError::Poisoned)));

    let fail_write2 = Arc::new(AtomicBool::new(false));
    let fail_sync2 = Arc::new(AtomicBool::new(true));
    let sync_calls2 = Arc::new(AtomicUsize::new(0));

    let mut writer2 = WalWriter::from_storage(
        PathBuf::from("mock_sync.wal"),
        FailingStorage {
            fail_write: Arc::clone(&fail_write2),
            fail_sync: Arc::clone(&fail_sync2),
            sync_calls: Arc::clone(&sync_calls2),
        },
        FsyncPolicy::Always,
        1,
        0,
    );

    assert!(matches!(
        writer2.append(RecordKind::Put, b"k", b"v"),
        Err(WalError::Io(_))
    ));
    assert_eq!(sync_calls2.load(Ordering::SeqCst), 1);

    fail_sync2.store(false, Ordering::SeqCst);
    assert!(matches!(writer2.sync(), Err(WalError::Poisoned)));
    assert_eq!(sync_calls2.load(Ordering::SeqCst), 1);
}

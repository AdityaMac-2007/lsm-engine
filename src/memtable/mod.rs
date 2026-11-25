use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::RwLock;

use crate::wal::RecordKind;

/// Default MemTable flush threshold: 4 MiB.
pub const MEMTABLE_FLUSH_THRESHOLD: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub kind: RecordKind,
}

impl InternalKey {
    pub fn new(user_key: impl Into<Vec<u8>>, seq: u64, kind: RecordKind) -> Self {
        Self {
            user_key: user_key.into(),
            seq,
            kind,
        }
    }
}

/// Orders entries by `(UserKey ASC, SequenceNumber DESC)`.
/// Higher sequence numbers are strictly newer and sort before lower sequence numbers.
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    Value(Vec<u8>),
    Tombstone,
}

struct MemTableInner {
    entries: BTreeMap<InternalKey, Vec<u8>>,
    approx_bytes: usize,
}

pub struct MemTable {
    id: u64,
    threshold: usize,
    frozen: AtomicBool,
    inner: RwLock<MemTableInner>,
}

impl MemTable {
    pub fn new(id: u64) -> Self {
        Self::with_threshold(id, MEMTABLE_FLUSH_THRESHOLD)
    }

    pub fn with_threshold(id: u64, threshold: usize) -> Self {
        Self {
            id,
            threshold,
            frozen: AtomicBool::new(false),
            inner: RwLock::new(MemTableInner {
                entries: BTreeMap::new(),
                approx_bytes: 0,
            }),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.load(AtomicOrdering::Acquire)
    }

    pub fn freeze(&self) {
        self.frozen.store(true, AtomicOrdering::Release);
    }

    pub fn put(&self, key: Vec<u8>, seq: u64, value: Vec<u8>) -> bool {
        let added = key.len() + value.len() + std::mem::size_of::<InternalKey>();
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .entries
            .insert(InternalKey::new(key, seq, RecordKind::Put), value);
        guard.approx_bytes = guard.approx_bytes.saturating_add(added);
        guard.approx_bytes >= self.threshold
    }

    pub fn delete(&self, key: Vec<u8>, seq: u64) -> bool {
        let added = key.len() + std::mem::size_of::<InternalKey>();
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .entries
            .insert(InternalKey::new(key, seq, RecordKind::Tombstone), Vec::new());
        guard.approx_bytes = guard.approx_bytes.saturating_add(added);
        guard.approx_bytes >= self.threshold
    }

    pub fn get(&self, user_key: &[u8], read_seq: u64) -> Option<LookupResult> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Seek to (user_key, read_seq) so we skip any entry with seq > read_seq.
        let seek_key = InternalKey::new(user_key.to_vec(), read_seq, RecordKind::Put);
        for (k, v) in guard.entries.range(seek_key..) {
            if k.user_key.as_slice() != user_key {
                break;
            }
            if k.seq <= read_seq {
                return Some(match k.kind {
                    RecordKind::Put => LookupResult::Value(v.clone()),
                    RecordKind::Tombstone => LookupResult::Tombstone,
                });
            }
        }
        None
    }

    pub fn approximate_size(&self) -> usize {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.approx_bytes
    }

    pub fn len(&self) -> usize {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshots all entries in `(UserKey ASC, SeqNum DESC)` order for SSTable flushing.
    pub fn snapshot_entries(&self) -> Vec<(InternalKey, Vec<u8>)> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

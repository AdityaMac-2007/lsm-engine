use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::memtable::InternalKey;
use crate::sstable::SstableReader;
use crate::wal::RecordKind;

/// Streaming iterator over a single SSTable, reading one 4KB-aligned block at a time.
pub struct SstableIter {
    reader: SstableReader,
    next_block: usize,
    current_entries: std::vec::IntoIter<(InternalKey, Vec<u8>)>,
}

impl SstableIter {
    pub fn new(reader: SstableReader) -> Self {
        Self {
            reader,
            next_block: 0,
            current_entries: Vec::new().into_iter(),
        }
    }

    pub fn next_entry(&mut self) -> Result<Option<(InternalKey, Vec<u8>)>> {
        loop {
            if let Some(item) = self.current_entries.next() {
                return Ok(Some(item));
            }
            if self.next_block >= self.reader.index().len() {
                return Ok(None);
            }
            let block = self.reader.read_block(self.next_block)?;
            self.next_block += 1;
            self.current_entries = block.into_iter();
        }
    }
}

struct HeapItem {
    key: InternalKey,
    value: Vec<u8>,
    source_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.source_idx.cmp(&other.source_idx))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// K-Way Merge Iterator across `K` SSTable iterators with MVCC Watermark & Tombstone GC.
pub struct CompactionMergeIterator {
    sources: Vec<SstableIter>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    watermark_seq: u64,
    is_bottommost: bool,
    last_user_key: Option<Vec<u8>>,
    saw_visible_at_watermark: bool,
}

impl CompactionMergeIterator {
    pub fn new(
        mut sources: Vec<SstableIter>,
        watermark_seq: u64,
        is_bottommost: bool,
    ) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (idx, src) in sources.iter_mut().enumerate() {
            if let Some((key, value)) = src.next_entry()? {
                heap.push(Reverse(HeapItem {
                    key,
                    value,
                    source_idx: idx,
                }));
            }
        }
        Ok(Self {
            sources,
            heap,
            watermark_seq,
            is_bottommost,
            last_user_key: None,
            saw_visible_at_watermark: false,
        })
    }

    pub fn next_output(&mut self) -> Result<Option<(InternalKey, Vec<u8>)>> {
        while let Some(Reverse(item)) = self.heap.pop() {
            if let Some((next_k, next_v)) = self.sources[item.source_idx].next_entry()? {
                self.heap.push(Reverse(HeapItem {
                    key: next_k,
                    value: next_v,
                    source_idx: item.source_idx,
                }));
            }

            let same_user_key = self
                .last_user_key
                .as_deref()
                .map(|k| k == item.key.user_key.as_slice())
                .unwrap_or(false);

            if !same_user_key {
                self.last_user_key = Some(item.key.user_key.clone());
                self.saw_visible_at_watermark = false;
            }

            // Versions strictly newer than the oldest active snapshot must always be kept.
            if item.key.seq > self.watermark_seq {
                return Ok(Some((item.key, item.value)));
            }

            // At or below watermark_seq: keep only the newest version (first encountered).
            if self.saw_visible_at_watermark {
                continue;
            }
            self.saw_visible_at_watermark = true;

            // Tombstones can be purged only when compacting into the bottom-most level
            // AND no active snapshot needs them (seq <= watermark_seq).
            if item.key.kind == RecordKind::Tombstone && self.is_bottommost {
                continue;
            }

            return Ok(Some((item.key, item.value)));
        }

        Ok(None)
    }
}

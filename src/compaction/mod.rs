pub mod merge;

use std::fs;
use std::path::Path;

use crate::error::Result;
use crate::memtable::InternalKey;
use crate::sstable::{SstableBuilder, SstableMeta, SstableReader};
use merge::{CompactionMergeIterator, SstableIter};

pub const DEFAULT_TARGET_SST_BYTES: usize = 4 * 1024 * 1024;

pub fn overlaps_range(
    a_min: &Option<InternalKey>,
    a_max: &Option<InternalKey>,
    b_min: &Option<InternalKey>,
    b_max: &Option<InternalKey>,
) -> bool {
    match (a_min, a_max, b_min, b_max) {
        (Some(amin), Some(amax), Some(bmin), Some(bmax)) => {
            amin.user_key <= bmax.user_key && bmin.user_key <= amax.user_key
        }
        _ => false,
    }
}

/// Runs a K-way merge compaction over `inputs`, splitting output into SSTables of
/// `target_sst_bytes` at `target_level`, returning the newly created SSTable metadata
/// and total bytes written.
pub fn compact_sstables<F>(
    inputs: &[SstableMeta],
    target_level: u32,
    is_bottommost: bool,
    watermark_seq: u64,
    target_sst_bytes: usize,
    mut next_sst_id: F,
    data_dir: &Path,
) -> Result<(Vec<SstableMeta>, u64)>
where
    F: FnMut() -> u64,
{
    if inputs.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let mut iters = Vec::with_capacity(inputs.len());
    for meta in inputs {
        let reader = SstableReader::open(&meta.path)?;
        iters.push(SstableIter::new(reader));
    }

    let mut merger = CompactionMergeIterator::new(iters, watermark_seq, is_bottommost)?;
    let mut outputs = Vec::new();
    let mut bytes_written: u64 = 0;

    let mut chunk: Vec<(InternalKey, Vec<u8>)> = Vec::new();
    let mut chunk_bytes: usize = 0;
    let mut prev_user_key: Option<Vec<u8>> = None;

    while let Some((key, val)) = merger.next_output()? {
        let same_user_key = prev_user_key
            .as_deref()
            .map(|k| k == key.user_key.as_slice())
            .unwrap_or(false);

        // Only split between distinct user_keys so all versions of a key stay in one SSTable.
        if !chunk.is_empty() && !same_user_key && chunk_bytes >= target_sst_bytes {
            let id = next_sst_id();
            let path = data_dir.join(format!("L{target_level}_{id:08}.sst"));
            let meta = SstableBuilder::build_from_entries(id, target_level, &path, &chunk)?;
            bytes_written += meta.file_size;
            outputs.push(meta);
            chunk.clear();
            chunk_bytes = 0;
        }

        prev_user_key = Some(key.user_key.clone());
        chunk_bytes += 17 + key.user_key.len() + val.len();
        chunk.push((key, val));
    }

    if !chunk.is_empty() {
        let id = next_sst_id();
        let path = data_dir.join(format!("L{target_level}_{id:08}.sst"));
        let meta = SstableBuilder::build_from_entries(id, target_level, &path, &chunk)?;
        bytes_written += meta.file_size;
        outputs.push(meta);
    }

    Ok((outputs, bytes_written))
}

pub fn remove_sst_files(metas: &[SstableMeta]) {
    for m in metas {
        let _ = fs::remove_file(&m.path);
    }
}

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::memtable::{InternalKey, MemTable};
use crate::sstable::block::DataBlockBuilder;
use crate::sstable::bloom::BloomFilter;
use crate::sstable::format::{Footer, IndexEntry};
use crate::wal::reader::fsync_parent_dir;

pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstableMeta {
    pub id: u64,
    pub level: u32,
    pub path: PathBuf,
    pub file_size: u64,
    pub entry_count: u64,
    pub min_key: Option<InternalKey>,
    pub max_key: Option<InternalKey>,
}

pub struct SstableBuilder;

impl SstableBuilder {
    /// Flushes a MemTable to an immutable SSTable at `path` with a Bloom Filter.
    pub fn build_from_memtable(
        id: u64,
        level: u32,
        path: impl AsRef<Path>,
        memtable: &MemTable,
    ) -> Result<SstableMeta> {
        let entries = memtable.snapshot_entries();
        Self::build_from_entries(id, level, path, &entries)
    }

    pub fn build_from_entries(
        id: u64,
        level: u32,
        path: impl AsRef<Path>,
        entries: &[(InternalKey, Vec<u8>)],
    ) -> Result<SstableMeta> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path_buf)?;
        let mut writer = BufWriter::new(file);

        let mut offset: u64 = 0;
        let mut index_entries = Vec::new();
        let mut current_block = DataBlockBuilder::new();

        for (key, value) in entries {
            if !current_block.is_empty() && !current_block.can_fit(&key.user_key, value) {
                let (first_key, block_bytes) = current_block.finish();
                let block_len = block_bytes.len() as u64;
                writer.write_all(&block_bytes)?;
                index_entries.push(IndexEntry {
                    first_key,
                    block_offset: offset,
                    block_len,
                });
                offset += block_len;
                current_block = DataBlockBuilder::new();
            }
            current_block.add(key, value)?;
        }

        if !current_block.is_empty() {
            let (first_key, block_bytes) = current_block.finish();
            let block_len = block_bytes.len() as u64;
            writer.write_all(&block_bytes)?;
            index_entries.push(IndexEntry {
                first_key,
                block_offset: offset,
                block_len,
            });
            offset += block_len;
        }

        // Filter Block: per-SSTable Bloom Filter over user keys.
        let filter_offset = offset;
        let filter_bytes = if entries.is_empty() {
            Vec::new()
        } else {
            let bloom = BloomFilter::build(
                entries.iter().map(|(k, _)| k.user_key.as_slice()),
                entries.len(),
                DEFAULT_BLOOM_BITS_PER_KEY,
            );
            bloom.encode()
        };
        if !filter_bytes.is_empty() {
            writer.write_all(&filter_bytes)?;
        }
        let filter_len = filter_bytes.len() as u64;
        offset += filter_len;

        // Sparse Index Block.
        let index_offset = offset;
        let mut index_buf = Vec::new();
        let num_idx = u32::try_from(index_entries.len())
            .map_err(|_| crate::error::WalError::InvalidArg("index count exceeds u32"))?;
        index_buf.extend_from_slice(&num_idx.to_le_bytes());
        for entry in &index_entries {
            entry.encode_into(&mut index_buf)?;
        }
        writer.write_all(&index_buf)?;
        let index_len = index_buf.len() as u64;
        offset += index_len;

        // 48-byte Footer.
        let footer = Footer {
            filter_offset,
            filter_len,
            index_offset,
            index_len,
        };
        let footer_bytes = footer.encode();
        writer.write_all(&footer_bytes)?;
        offset += footer_bytes.len() as u64;

        writer.flush()?;
        writer.get_ref().sync_all()?;
        fsync_parent_dir(&path_buf)?;

        let min_key = entries.first().map(|(k, _)| k.clone());
        let max_key = entries.last().map(|(k, _)| k.clone());

        Ok(SstableMeta {
            id,
            level,
            path: path_buf,
            file_size: offset,
            entry_count: entries.len() as u64,
            min_key,
            max_key,
        })
    }
}

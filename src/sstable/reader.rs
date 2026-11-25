use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{Result, StopReason, WalError};
use crate::memtable::{InternalKey, LookupResult};
use crate::sstable::block::decode_data_block;
use crate::sstable::bloom::BloomFilter;
use crate::sstable::format::{Footer, IndexEntry, FOOTER_SIZE};
use crate::wal::RecordKind;

pub struct SstableReader {
    path: PathBuf,
    file: File,
    footer: Footer,
    filter: Option<BloomFilter>,
    index: Vec<IndexEntry>,
    block_reads: u64,
}

impl SstableReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = File::open(&path_buf)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_SIZE as u64 {
            return Err(WalError::Corruption {
                offset: 0,
                reason: StopReason::TornHeader,
            });
        }

        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE as u64))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)?;

        let filter = if footer.filter_len > 0 {
            file.seek(SeekFrom::Start(footer.filter_offset))?;
            let mut fbuf = vec![0u8; footer.filter_len as usize];
            file.read_exact(&mut fbuf)?;
            BloomFilter::decode(&fbuf)
        } else {
            None
        };

        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_buf = vec![0u8; footer.index_len as usize];
        file.read_exact(&mut index_buf)?;

        if index_buf.len() < 4 {
            return Err(WalError::Corruption {
                offset: footer.index_offset,
                reason: StopReason::TornHeader,
            });
        }
        let num_entries =
            u32::from_le_bytes([index_buf[0], index_buf[1], index_buf[2], index_buf[3]]) as usize;
        let mut cursor = 4usize;
        let mut index = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            index.push(IndexEntry::decode_from(&index_buf, &mut cursor)?);
        }

        Ok(Self {
            path: path_buf,
            file,
            footer,
            filter,
            index,
            block_reads: 0,
        })
    }

    pub fn footer(&self) -> Footer {
        self.footer
    }

    pub fn index(&self) -> &[IndexEntry] {
        &self.index
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn block_reads(&self) -> u64 {
        self.block_reads
    }

    pub fn may_contain(&self, user_key: &[u8]) -> bool {
        match &self.filter {
            Some(f) => f.may_contain(user_key),
            None => true,
        }
    }

    pub fn read_block(&mut self, idx: usize) -> Result<Vec<(InternalKey, Vec<u8>)>> {
        let entry = &self.index[idx];
        self.file.seek(SeekFrom::Start(entry.block_offset))?;
        let mut block = vec![0u8; entry.block_len as usize];
        self.file.read_exact(&mut block)?;
        self.block_reads += 1;
        decode_data_block(&block)
    }

    pub fn read_all_entries(&mut self) -> Result<Vec<(InternalKey, Vec<u8>)>> {
        let mut all = Vec::new();
        for idx in 0..self.index.len() {
            let mut block_entries = self.read_block(idx)?;
            all.append(&mut block_entries);
        }
        Ok(all)
    }

    /// Point lookup for `user_key` visible at `read_seq`.
    /// When `use_bloom` is true, short-circuits before any Data Block disk read.
    pub fn get(
        &mut self,
        user_key: &[u8],
        read_seq: u64,
        use_bloom: bool,
    ) -> Result<Option<LookupResult>> {
        if self.index.is_empty() {
            return Ok(None);
        }
        if use_bloom && !self.may_contain(user_key) {
            return Ok(None);
        }

        // Start at the block before the first index entry with user_key >= target,
        // because an earlier block may contain newer versions (higher seq) of target.
        let mut block_idx = self
            .index
            .partition_point(|e| e.first_key.user_key.as_slice() < user_key);
        if block_idx > 0 {
            block_idx -= 1;
        }

        while block_idx < self.index.len() {
            if self.index[block_idx].first_key.user_key.as_slice() > user_key {
                break;
            }
            let entries = self.read_block(block_idx)?;
            for (k, v) in entries {
                if k.user_key.as_slice() > user_key {
                    return Ok(None);
                }
                if k.user_key.as_slice() == user_key && k.seq <= read_seq {
                    return Ok(Some(match k.kind {
                        RecordKind::Put => LookupResult::Value(v),
                        RecordKind::Tombstone => LookupResult::Tombstone,
                    }));
                }
            }
            block_idx += 1;
        }

        Ok(None)
    }

    /// Range scan returning all internal entries with `start_key <= user_key <= end_key`.
    pub fn scan(&mut self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(InternalKey, Vec<u8>)>> {
        if self.index.is_empty() || start_key > end_key {
            return Ok(Vec::new());
        }

        let mut block_idx = self
            .index
            .partition_point(|e| e.first_key.user_key.as_slice() < start_key);
        if block_idx > 0 {
            block_idx -= 1;
        }

        let mut out = Vec::new();
        while block_idx < self.index.len() {
            if self.index[block_idx].first_key.user_key.as_slice() > end_key {
                break;
            }
            let entries = self.read_block(block_idx)?;
            for (k, v) in entries {
                if k.user_key.as_slice() > end_key {
                    return Ok(out);
                }
                if k.user_key.as_slice() >= start_key {
                    out.push((k, v));
                }
            }
            block_idx += 1;
        }

        Ok(out)
    }
}

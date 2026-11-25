use crate::error::{StopReason, WalError};
use crate::memtable::InternalKey;
use crate::sstable::format::BLOCK_SIZE;
use crate::wal::RecordKind;

/// Data Block Layout (4KB-aligned):
/// `[num_entries: u32 LE][Entry 0][Entry 1]...[Zero Padding to 4KB boundary]`
/// Entry Layout:
/// `[SeqNum: u64 LE][RecordType: u8][KeyLen: u32 LE][ValLen: u32 LE][Key][Value]`
pub struct DataBlockBuilder {
    buf: Vec<u8>,
    num_entries: u32,
    first_key: Option<InternalKey>,
}

impl DataBlockBuilder {
    pub fn new() -> Self {
        let mut buf = Vec::with_capacity(BLOCK_SIZE);
        buf.extend_from_slice(&0u32.to_le_bytes()); // Reserve num_entries header
        Self {
            buf,
            num_entries: 0,
            first_key: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    pub fn entry_encoded_len(key: &[u8], value: &[u8]) -> usize {
        8 + 1 + 4 + 4 + key.len() + value.len()
    }

    pub fn can_fit(&self, key: &[u8], value: &[u8]) -> bool {
        self.buf.len() + Self::entry_encoded_len(key, value) <= BLOCK_SIZE
    }

    pub fn add(&mut self, key: &InternalKey, value: &[u8]) -> Result<(), WalError> {
        let klen = u32::try_from(key.user_key.len())
            .map_err(|_| WalError::InvalidArg("SSTable key exceeds u32"))?;
        let vlen = u32::try_from(value.len())
            .map_err(|_| WalError::InvalidArg("SSTable value exceeds u32"))?;

        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }

        self.buf.extend_from_slice(&key.seq.to_le_bytes());
        self.buf.push(key.kind as u8);
        self.buf.extend_from_slice(&klen.to_le_bytes());
        self.buf.extend_from_slice(&vlen.to_le_bytes());
        self.buf.extend_from_slice(&key.user_key);
        self.buf.extend_from_slice(value);
        self.num_entries = self
            .num_entries
            .checked_add(1)
            .ok_or(WalError::InvalidArg("block entry count overflow"))?;
        Ok(())
    }

    /// Finalizes the block, writing `num_entries` and zero-padding to a 4KB boundary.
    pub fn finish(mut self) -> (InternalKey, Vec<u8>) {
        self.buf[0..4].copy_from_slice(&self.num_entries.to_le_bytes());
        let rem = self.buf.len() % BLOCK_SIZE;
        if rem != 0 {
            self.buf.resize(self.buf.len() + (BLOCK_SIZE - rem), 0);
        }
        let first = self
            .first_key
            .unwrap_or_else(|| InternalKey::new(Vec::new(), 0, RecordKind::Put));
        (first, self.buf)
    }
}

pub fn decode_data_block(block: &[u8]) -> Result<Vec<(InternalKey, Vec<u8>)>, WalError> {
    if block.len() < 4 {
        return Err(WalError::Corruption {
            offset: 0,
            reason: StopReason::TornHeader,
        });
    }
    let num_entries = u32::from_le_bytes([block[0], block[1], block[2], block[3]]) as usize;
    let mut cursor = 4usize;
    let mut entries = Vec::with_capacity(num_entries);

    for _ in 0..num_entries {
        if cursor + 17 > block.len() {
            return Err(WalError::Corruption {
                offset: cursor as u64,
                reason: StopReason::TornHeader,
            });
        }
        let seq = u64::from_le_bytes([
            block[cursor],
            block[cursor + 1],
            block[cursor + 2],
            block[cursor + 3],
            block[cursor + 4],
            block[cursor + 5],
            block[cursor + 6],
            block[cursor + 7],
        ]);
        cursor += 8;

        let kind = RecordKind::from_u8(block[cursor]).map_err(|reason| WalError::Corruption {
            offset: cursor as u64,
            reason,
        })?;
        cursor += 1;

        let klen = u32::from_le_bytes([
            block[cursor],
            block[cursor + 1],
            block[cursor + 2],
            block[cursor + 3],
        ]) as usize;
        cursor += 4;

        let vlen = u32::from_le_bytes([
            block[cursor],
            block[cursor + 1],
            block[cursor + 2],
            block[cursor + 3],
        ]) as usize;
        cursor += 4;

        if cursor + klen + vlen > block.len() {
            return Err(WalError::Corruption {
                offset: cursor as u64,
                reason: StopReason::TornBody,
            });
        }

        let user_key = block[cursor..cursor + klen].to_vec();
        cursor += klen;
        let value = block[cursor..cursor + vlen].to_vec();
        cursor += vlen;

        entries.push((InternalKey::new(user_key, seq, kind), value));
    }

    Ok(entries)
}

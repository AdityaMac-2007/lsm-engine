use std::io;

use crate::error::{StopReason, WalError};
use crate::memtable::InternalKey;
use crate::wal::RecordKind;

/// 4KB Data Block alignment.
pub const BLOCK_SIZE: usize = 4096;
/// 48-byte fixed SSTable Footer.
pub const FOOTER_SIZE: usize = 48;
/// Magic number identifying an LSM-Tree SSTable ("LSMTREE1").
pub const SSTABLE_MAGIC: u64 = 0x4C53_4D54_5245_4531;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub first_key: InternalKey,
    pub block_offset: u64,
    pub block_len: u64,
}

impl IndexEntry {
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), WalError> {
        let klen = u32::try_from(self.first_key.user_key.len())
            .map_err(|_| WalError::InvalidArg("index key exceeds u32"))?;
        buf.extend_from_slice(&klen.to_le_bytes());
        buf.extend_from_slice(&self.first_key.seq.to_le_bytes());
        buf.push(self.first_key.kind as u8);
        buf.extend_from_slice(&self.first_key.user_key);
        buf.extend_from_slice(&self.block_offset.to_le_bytes());
        buf.extend_from_slice(&self.block_len.to_le_bytes());
        Ok(())
    }

    pub fn decode_from(bytes: &[u8], cursor: &mut usize) -> Result<Self, WalError> {
        if *cursor + 4 + 8 + 1 > bytes.len() {
            return Err(WalError::Corruption {
                offset: *cursor as u64,
                reason: StopReason::TornHeader,
            });
        }
        let klen = u32::from_le_bytes([
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
        ]) as usize;
        *cursor += 4;

        let seq = u64::from_le_bytes([
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
            bytes[*cursor + 4],
            bytes[*cursor + 5],
            bytes[*cursor + 6],
            bytes[*cursor + 7],
        ]);
        *cursor += 8;

        let kind = RecordKind::from_u8(bytes[*cursor]).map_err(|reason| WalError::Corruption {
            offset: *cursor as u64,
            reason,
        })?;
        *cursor += 1;

        if *cursor + klen + 16 > bytes.len() {
            return Err(WalError::Corruption {
                offset: *cursor as u64,
                reason: StopReason::TornBody,
            });
        }

        let user_key = bytes[*cursor..*cursor + klen].to_vec();
        *cursor += klen;

        let block_offset = u64::from_le_bytes([
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
            bytes[*cursor + 4],
            bytes[*cursor + 5],
            bytes[*cursor + 6],
            bytes[*cursor + 7],
        ]);
        *cursor += 8;

        let block_len = u64::from_le_bytes([
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
            bytes[*cursor + 4],
            bytes[*cursor + 5],
            bytes[*cursor + 6],
            bytes[*cursor + 7],
        ]);
        *cursor += 8;

        Ok(Self {
            first_key: InternalKey::new(user_key, seq, kind),
            block_offset,
            block_len,
        })
    }
}

/// Fixed 48-byte SSTable Footer:
/// `[FilterOffset: u64][FilterLen: u64][IndexOffset: u64][IndexLen: u64][Padding: 8B][MagicNumber: u64]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub filter_offset: u64,
    pub filter_len: u64,
    pub index_offset: u64,
    pub index_len: u64,
}

impl Footer {
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..8].copy_from_slice(&self.filter_offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.filter_len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.index_len.to_le_bytes());
        // [32..40] = 8-byte zero padding
        buf[40..48].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; FOOTER_SIZE]) -> Result<Self, WalError> {
        let magic = u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
        ]);
        if magic != SSTABLE_MAGIC {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid SSTable magic: {magic:#018x}"),
            )));
        }

        Ok(Self {
            filter_offset: u64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]),
            filter_len: u64::from_le_bytes([
                buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            ]),
            index_offset: u64::from_le_bytes([
                buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
            ]),
            index_len: u64::from_le_bytes([
                buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
            ]),
        })
    }
}

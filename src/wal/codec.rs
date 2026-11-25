use crate::error::{RecordField, Result, StopReason, WalError};
use crate::wal::crc::crc32;

/// Fixed frame header:
/// CRC32 (4B) + SeqNum (8B) + RecordType (1B) + KeyLen (4B) + ValLen (4B) = 21 bytes.
pub const WAL_HEADER_SIZE: usize = 21;
pub const CRC_OFFSET: usize = 4;
pub const MAX_KEY_LEN: usize = 64 * 1024;
pub const MAX_VAL_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Put = 1,
    Tombstone = 2,
}

impl RecordKind {
    pub fn from_u8(v: u8) -> std::result::Result<Self, StopReason> {
        match v {
            1 => Ok(RecordKind::Put),
            2 => Ok(RecordKind::Tombstone),
            _ => Err(StopReason::BadType),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub kind: RecordKind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub fn validate_record_args(kind: RecordKind, key: &[u8], value: &[u8]) -> Result<()> {
    if kind == RecordKind::Tombstone && !value.is_empty() {
        return Err(WalError::InvalidArg("tombstone must have empty value"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(WalError::RecordTooLarge {
            field: RecordField::Key,
            len: key.len(),
            max: MAX_KEY_LEN,
        });
    }
    if value.len() > MAX_VAL_LEN {
        return Err(WalError::RecordTooLarge {
            field: RecordField::Value,
            len: value.len(),
            max: MAX_VAL_LEN,
        });
    }
    Ok(())
}

/// Encodes a single WAL frame into one contiguous little-endian buffer:
/// `[CRC32: u32][SeqNum: u64][RecordType: u8][KeyLen: u32][ValLen: u32][Key][Value]`
pub fn encode_frame(seq: u64, kind: RecordKind, key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    if seq == 0 {
        return Err(WalError::InvalidArg("sequence number 0 is invalid"));
    }
    validate_record_args(kind, key, value)?;

    let klen = u32::try_from(key.len()).map_err(|_| WalError::RecordTooLarge {
        field: RecordField::Key,
        len: key.len(),
        max: MAX_KEY_LEN,
    })?;
    let vlen = u32::try_from(value.len()).map_err(|_| WalError::RecordTooLarge {
        field: RecordField::Value,
        len: value.len(),
        max: MAX_VAL_LEN,
    })?;

    let total_len = WAL_HEADER_SIZE + key.len() + value.len();
    let mut buf = Vec::with_capacity(total_len);
    buf.extend_from_slice(&[0u8; CRC_OFFSET]);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.push(kind as u8);
    buf.extend_from_slice(&klen.to_le_bytes());
    buf.extend_from_slice(&vlen.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);

    let checksum = crc32(&buf[CRC_OFFSET..]);
    buf[..CRC_OFFSET].copy_from_slice(&checksum.to_le_bytes());
    Ok(buf)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedHeader {
    pub expected_crc: u32,
    pub seq: u64,
    pub kind: RecordKind,
    pub klen: usize,
    pub vlen: usize,
}

pub fn decode_header(
    hdr: &[u8; WAL_HEADER_SIZE],
    last_seq: u64,
) -> std::result::Result<DecodedHeader, StopReason> {
    let expected_crc = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let seq = u64::from_le_bytes([
        hdr[4], hdr[5], hdr[6], hdr[7], hdr[8], hdr[9], hdr[10], hdr[11],
    ]);
    let kind = RecordKind::from_u8(hdr[12])?;
    let klen = u32::from_le_bytes([hdr[13], hdr[14], hdr[15], hdr[16]]) as usize;
    let vlen = u32::from_le_bytes([hdr[17], hdr[18], hdr[19], hdr[20]]) as usize;

    if kind == RecordKind::Tombstone && vlen > 0 {
        return Err(StopReason::BadType);
    }
    if klen > MAX_KEY_LEN || vlen > MAX_VAL_LEN {
        return Err(StopReason::BadLen);
    }
    if seq == 0 || seq <= last_seq {
        return Err(StopReason::SeqRegression);
    }

    Ok(DecodedHeader {
        expected_crc,
        seq,
        kind,
        klen,
        vlen,
    })
}

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Result, StopReason, WalError};
use crate::wal::codec::{decode_header, Record, CRC_OFFSET, WAL_HEADER_SIZE};
use crate::wal::crc::crc32_parts;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub records: u64,
    pub valid_len: u64,
    pub file_len: u64,
    pub last_seq: u64,
    pub stop: StopReason,
}

pub struct WalReader {
    file: File,
    file_len: u64,
    offset: u64,
    last_seq: u64,
    done: bool,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            file,
            file_len,
            offset: 0,
            last_seq: 0,
            done: false,
        })
    }

    fn read_next_frame(&mut self) -> std::result::Result<Option<Record>, WalError> {
        if self.done || self.offset >= self.file_len {
            self.done = true;
            return Ok(None);
        }

        let remaining = self.file_len - self.offset;
        if remaining < WAL_HEADER_SIZE as u64 {
            self.done = true;
            return Err(WalError::Corruption {
                offset: self.offset,
                reason: StopReason::TornHeader,
            });
        }

        let mut hdr = [0u8; WAL_HEADER_SIZE];
        self.file.read_exact(&mut hdr)?;

        let decoded = match decode_header(&hdr, self.last_seq) {
            Ok(d) => d,
            Err(reason) => {
                self.done = true;
                return Err(WalError::Corruption {
                    offset: self.offset,
                    reason,
                });
            }
        };

        let body_len = decoded.klen + decoded.vlen;
        let frame_len = (WAL_HEADER_SIZE + body_len) as u64;
        if frame_len > remaining {
            self.done = true;
            return Err(WalError::Corruption {
                offset: self.offset,
                reason: StopReason::TornBody,
            });
        }

        let mut body = vec![0u8; body_len];
        self.file.read_exact(&mut body)?;

        let actual_crc = crc32_parts(&hdr[CRC_OFFSET..], &body);
        if actual_crc != decoded.expected_crc {
            self.done = true;
            return Err(WalError::Corruption {
                offset: self.offset,
                reason: StopReason::BadCrc,
            });
        }

        let value = body.split_off(decoded.klen);
        let key = body;
        self.last_seq = decoded.seq;
        self.offset += frame_len;

        Ok(Some(Record {
            seq: decoded.seq,
            kind: decoded.kind,
            key,
            value,
        }))
    }
}

impl Iterator for WalReader {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_frame() {
            Ok(Some(rec)) => Some(Ok(rec)),
            Ok(None) => None,
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }
}

/// Scans the WAL from offset 0, truncates any torn or corrupted tail,
/// then fsyncs both the file and its parent directory.
pub fn recover(path: impl AsRef<Path>) -> Result<(Vec<Record>, RecoveryReport)> {
    let path_ref = path.as_ref();
    let mut reader = WalReader::open(path_ref)?;
    let file_len = reader.file_len;
    let mut records = Vec::new();
    let mut stop = StopReason::CleanEof;

    for item in &mut reader {
        match item {
            Ok(rec) => records.push(rec),
            Err(WalError::Corruption { reason, .. }) => {
                stop = reason;
                break;
            }
            Err(err) => return Err(err),
        }
    }

    let valid_len = reader.offset;
    let last_seq = reader.last_seq;
    drop(reader);

    if valid_len < file_len {
        let file = OpenOptions::new().write(true).open(path_ref)?;
        file.set_len(valid_len)?;
        file.sync_all()?;
        fsync_parent_dir(path_ref)?;
    }

    let report = RecoveryReport {
        records: records.len() as u64,
        valid_len,
        file_len,
        last_seq,
        stop,
    };

    Ok((records, report))
}

pub(crate) fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        let dir = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let dir_handle = File::open(dir)?;
        dir_handle.sync_all()?;
    }
    Ok(())
}

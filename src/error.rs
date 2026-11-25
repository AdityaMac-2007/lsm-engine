use std::fmt;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordField {
    Key,
    Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    CleanEof,
    TornHeader,
    TornBody,
    BadCrc,
    BadType,
    BadLen,
    SeqRegression,
}

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    RecordTooLarge {
        field: RecordField,
        len: usize,
        max: usize,
    },
    Corruption {
        offset: u64,
        reason: StopReason,
    },
    Poisoned,
    InvalidArg(&'static str),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(err) => write!(f, "WAL I/O error: {err}"),
            WalError::RecordTooLarge { field, len, max } => {
                write!(f, "WAL {field:?} length {len} exceeds max {max}")
            }
            WalError::Corruption { offset, reason } => {
                write!(f, "WAL corruption at offset {offset}: {reason:?}")
            }
            WalError::Poisoned => write!(f, "WAL writer is poisoned after a prior I/O failure"),
            WalError::InvalidArg(msg) => write!(f, "invalid WAL argument: {msg}"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        WalError::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, WalError>;

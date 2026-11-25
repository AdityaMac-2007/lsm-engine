use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{Result, WalError};
use crate::wal::codec::{encode_frame, RecordKind};
use crate::wal::reader::fsync_parent_dir;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EveryN(u32),
    Interval(Duration),
    Never,
}

pub trait WalStorage: Write + Send {
    fn sync_data(&self) -> io::Result<()>;
}

impl WalStorage for File {
    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}

struct WalWriterInner<S: WalStorage> {
    file: S,
    next_seq: u64,
    unsynced: u32,
    offset: u64,
    last_sync: Instant,
    failed: bool,
}

pub struct WalWriter<S: WalStorage = File> {
    path: PathBuf,
    policy: FsyncPolicy,
    inner: Mutex<WalWriterInner<S>>,
}

impl WalWriter<File> {
    pub fn open(path: impl AsRef<Path>, policy: FsyncPolicy, start_seq: u64) -> Result<Self> {
        if start_seq == 0 {
            return Err(WalError::InvalidArg("start_seq must be >= 1"));
        }
        let path_buf = path.as_ref().to_path_buf();
        let existed = path_buf.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path_buf)?;
        if !existed {
            fsync_parent_dir(&path_buf)?;
        }
        let offset = file.metadata()?.len();

        Ok(Self::from_storage(path_buf, file, policy, start_seq, offset))
    }
}

impl<S: WalStorage> WalWriter<S> {
    pub fn from_storage(
        path: PathBuf,
        file: S,
        policy: FsyncPolicy,
        start_seq: u64,
        offset: u64,
    ) -> Self {
        Self {
            path,
            policy,
            inner: Mutex::new(WalWriterInner {
                file,
                next_seq: start_seq,
                unsynced: 0,
                offset,
                last_sync: Instant::now(),
                failed: false,
            }),
        }
    }

    pub fn append(&mut self, kind: RecordKind, key: &[u8], value: &[u8]) -> Result<u64> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| WalError::Poisoned)?;

        if guard.failed {
            return Err(WalError::Poisoned);
        }

        let seq = guard.next_seq;
        let frame = encode_frame(seq, kind, key, value)?;

        if let Err(err) = guard.file.write_all(&frame) {
            guard.failed = true;
            return Err(WalError::Io(err));
        }

        guard.offset = guard.offset.saturating_add(frame.len() as u64);
        guard.next_seq = guard
            .next_seq
            .checked_add(1)
            .ok_or(WalError::InvalidArg("sequence number overflow"))?;
        guard.unsynced = guard.unsynced.saturating_add(1);

        let should_sync = match self.policy {
            FsyncPolicy::Always => true,
            FsyncPolicy::EveryN(n) => n > 0 && guard.unsynced >= n,
            FsyncPolicy::Interval(d) => guard.last_sync.elapsed() >= d,
            FsyncPolicy::Never => false,
        };

        if should_sync {
            if let Err(err) = guard.file.sync_data() {
                guard.failed = true;
                return Err(WalError::Io(err));
            }
            guard.unsynced = 0;
            guard.last_sync = Instant::now();
        }

        Ok(seq)
    }

    pub fn sync(&mut self) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| WalError::Poisoned)?;

        if guard.failed {
            return Err(WalError::Poisoned);
        }

        if let Err(err) = guard.file.sync_data() {
            guard.failed = true;
            return Err(WalError::Io(err));
        }

        guard.unsynced = 0;
        guard.last_sync = Instant::now();
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

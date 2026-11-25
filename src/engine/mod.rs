use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crate::compaction::{compact_sstables, overlaps_range, DEFAULT_TARGET_SST_BYTES};
use crate::error::{Result, WalError};
use crate::memtable::{InternalKey, LookupResult, MemTable, MEMTABLE_FLUSH_THRESHOLD};
use crate::sstable::{SstableBuilder, SstableMeta, SstableReader};
use crate::wal::{recover, FsyncPolicy, RecordKind, WalWriter};

#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub fsync_policy: FsyncPolicy,
    pub memtable_threshold: usize,
    pub l0_compaction_trigger: usize,
    pub l1_max_bytes: u64,
    pub level_multiplier: u64,
    pub target_sst_bytes: usize,
    pub use_bloom_filters: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            fsync_policy: FsyncPolicy::Always,
            memtable_threshold: MEMTABLE_FLUSH_THRESHOLD,
            l0_compaction_trigger: 4,
            l1_max_bytes: 16 * 1024 * 1024,
            level_multiplier: 10,
            target_sst_bytes: DEFAULT_TARGET_SST_BYTES,
            use_bloom_filters: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub wal_bytes_written: u64,
    pub flush_bytes_written: u64,
    pub compaction_bytes_written: u64,
    pub logical_user_bytes: u64,
}

impl EngineStats {
    pub fn write_amplification(&self) -> f64 {
        if self.logical_user_bytes == 0 {
            return 0.0;
        }
        let total_disk = self.wal_bytes_written + self.flush_bytes_written + self.compaction_bytes_written;
        total_disk as f64 / self.logical_user_bytes as f64
    }
}

struct SstHandle {
    meta: SstableMeta,
    obsolete: AtomicBool,
}

impl Drop for SstHandle {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.meta.path);
        }
    }
}

struct EngineState {
    active_mem: Arc<MemTable>,
    imm_mems: Vec<Arc<MemTable>>,
    levels: Vec<Vec<Arc<SstHandle>>>,
}

struct SnapshotRegistry {
    active: BTreeMap<u64, usize>,
}

impl SnapshotRegistry {
    fn register(&mut self, seq: u64) {
        *self.active.entry(seq).or_insert(0) += 1;
    }

    fn unregister(&mut self, seq: u64) {
        if let Some(cnt) = self.active.get_mut(&seq) {
            if *cnt <= 1 {
                self.active.remove(&seq);
            } else {
                *cnt -= 1;
            }
        }
    }

    fn watermark(&self) -> u64 {
        self.active.keys().next().copied().unwrap_or(u64::MAX)
    }
}

pub struct Snapshot<'a> {
    engine: &'a LsmEngine,
    read_seq: u64,
}

impl<'a> Snapshot<'a> {
    pub fn read_seq(&self) -> u64 {
        self.read_seq
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.engine.get_at(key, self.read_seq)
    }

    pub fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.engine.scan_at(start_key, end_key, self.read_seq)
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        if let Ok(mut reg) = self.engine.snapshots.lock() {
            reg.unregister(self.read_seq);
        }
    }
}

struct WorkerSignal {
    pending: bool,
    shutdown: bool,
}

pub struct LsmEngine {
    dir: PathBuf,
    options: EngineOptions,
    wal: Mutex<WalWriter>,
    state: Arc<RwLock<EngineState>>,
    latest_seq: AtomicU64,
    next_id: Arc<AtomicU64>,
    snapshots: Arc<Mutex<SnapshotRegistry>>,
    stats: Arc<Mutex<EngineStats>>,
    flush_lock: Arc<Mutex<()>>,
    compaction_lock: Arc<Mutex<()>>,
    worker_signal: Arc<(Mutex<WorkerSignal>, Condvar)>,
    worker_handle: Option<JoinHandle<()>>,
}

impl LsmEngine {
    pub fn open(dir: impl AsRef<Path>, options: EngineOptions) -> Result<Self> {
        let dir_path = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;

        // Recover existing SSTables from directory.
        let mut levels: Vec<Vec<Arc<SstHandle>>> = vec![Vec::new(); 7];
        let mut max_id: u64 = 0;
        let mut max_sst_seq: u64 = 0;

        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some((level, id)) = parse_sst_filename(stem) {
                        max_id = max_id.max(id);
                        let mut reader = SstableReader::open(&path)?;
                        let all = reader.read_all_entries()?;
                        for (k, _) in &all {
                            max_sst_seq = max_sst_seq.max(k.seq);
                        }
                        let min_key = all.first().map(|(k, _)| k.clone());
                        let max_key = all.last().map(|(k, _)| k.clone());
                        let file_size = fs::metadata(&path)?.len();
                        let lvl_idx = (level as usize).min(levels.len() - 1);
                        levels[lvl_idx].push(Arc::new(SstHandle {
                            meta: SstableMeta {
                                id,
                                level,
                                path,
                                file_size,
                                entry_count: all.len() as u64,
                                min_key,
                                max_key,
                            },
                            obsolete: AtomicBool::new(false),
                        }));
                    }
                }
            }
        }

        // L0 newest-first; L1+ sorted by min_key.
        levels[0].sort_by(|a, b| b.meta.id.cmp(&a.meta.id));
        for lvl in &mut levels[1..] {
            lvl.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));
        }

        // Recover WAL.
        let wal_path = dir_path.join("wal.log");
        let active_mem = Arc::new(MemTable::with_threshold(
            max_id + 1,
            options.memtable_threshold,
        ));
        let mut last_seq = max_sst_seq;
        let mut wal_recovered_bytes = 0u64;

        if wal_path.exists() {
            let (records, report) = recover(&wal_path)?;
            wal_recovered_bytes = report.valid_len;
            for rec in records {
                last_seq = last_seq.max(rec.seq);
                match rec.kind {
                    RecordKind::Put => {
                        active_mem.put(rec.key, rec.seq, rec.value);
                    }
                    RecordKind::Tombstone => {
                        active_mem.delete(rec.key, rec.seq);
                    }
                }
            }
        }

        let start_seq = last_seq + 1;
        let wal = WalWriter::open(&wal_path, options.fsync_policy, start_seq)?;

        let state = Arc::new(RwLock::new(EngineState {
            active_mem,
            imm_mems: Vec::new(),
            levels,
        }));
        let next_id = Arc::new(AtomicU64::new(max_id + 2));
        let snapshots = Arc::new(Mutex::new(SnapshotRegistry {
            active: BTreeMap::new(),
        }));
        let stats = Arc::new(Mutex::new(EngineStats {
            wal_bytes_written: wal_recovered_bytes,
            ..Default::default()
        }));
        let flush_lock = Arc::new(Mutex::new(()));
        let compaction_lock = Arc::new(Mutex::new(()));
        let worker_signal = Arc::new((
            Mutex::new(WorkerSignal {
                pending: false,
                shutdown: false,
            }),
            Condvar::new(),
        ));

        let bg_dir = dir_path.clone();
        let bg_opts = options.clone();
        let bg_state = Arc::clone(&state);
        let bg_next_id = Arc::clone(&next_id);
        let bg_snaps = Arc::clone(&snapshots);
        let bg_stats = Arc::clone(&stats);
        let bg_flush_lock = Arc::clone(&flush_lock);
        let bg_compaction_lock = Arc::clone(&compaction_lock);
        let bg_sig = Arc::clone(&worker_signal);

        let handle = thread::spawn(move || {
            background_worker(
                bg_dir,
                bg_opts,
                bg_state,
                bg_next_id,
                bg_snaps,
                bg_stats,
                bg_flush_lock,
                bg_compaction_lock,
                bg_sig,
            );
        });

        Ok(Self {
            dir: dir_path,
            options,
            wal: Mutex::new(wal),
            state,
            latest_seq: AtomicU64::new(last_seq),
            next_id,
            snapshots,
            stats,
            flush_lock,
            compaction_lock,
            worker_signal,
            worker_handle: Some(handle),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        self.write_internal(RecordKind::Put, key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<u64> {
        self.write_internal(RecordKind::Tombstone, key, &[])
    }

    fn write_internal(&self, kind: RecordKind, key: &[u8], value: &[u8]) -> Result<u64> {
        // Strict locking order: WAL -> MemTable -> State (if freeze needed).
        let seq = {
            let mut wal = self.wal.lock().map_err(|_| WalError::Poisoned)?;
            wal.append(kind, key, value)?
        };

        let frame_bytes = (21 + key.len() + value.len()) as u64;
        let logical_bytes = (key.len() + value.len()) as u64;
        if let Ok(mut st) = self.stats.lock() {
            st.wal_bytes_written += frame_bytes;
            st.logical_user_bytes += logical_bytes;
        }

        let should_freeze = {
            let state = match self.state.read() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            match kind {
                RecordKind::Put => state.active_mem.put(key.to_vec(), seq, value.to_vec()),
                RecordKind::Tombstone => state.active_mem.delete(key.to_vec(), seq),
            }
        };

        self.latest_seq.store(seq, Ordering::Release);

        if should_freeze {
            self.rotate_memtable()?;
        }

        Ok(seq)
    }

    fn rotate_memtable(&self) -> Result<()> {
        {
            let mut state = match self.state.write() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if state.active_mem.approximate_size() >= self.options.memtable_threshold {
                let new_id = self.next_id.fetch_add(1, Ordering::SeqCst);
                let new_mem = Arc::new(MemTable::with_threshold(
                    new_id,
                    self.options.memtable_threshold,
                ));
                let old = std::mem::replace(&mut state.active_mem, new_mem);
                old.freeze();
                state.imm_mems.insert(0, old);
            }
        }
        self.notify_worker();
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        {
            let mut state = match self.state.write() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if !state.active_mem.is_empty() {
                let new_id = self.next_id.fetch_add(1, Ordering::SeqCst);
                let new_mem = Arc::new(MemTable::with_threshold(
                    new_id,
                    self.options.memtable_threshold,
                ));
                let old = std::mem::replace(&mut state.active_mem, new_mem);
                old.freeze();
                state.imm_mems.insert(0, old);
            }
        }
        flush_all_imm(
            &self.dir,
            &self.state,
            &self.next_id,
            &self.stats,
            &self.flush_lock,
        )?;
        Ok(())
    }

    pub fn compact_now(&self) -> Result<()> {
        run_compaction_pass(
            &self.dir,
            &self.options,
            &self.state,
            &self.next_id,
            &self.snapshots,
            &self.stats,
            &self.compaction_lock,
        )
    }

    pub fn snapshot(&self) -> Snapshot<'_> {
        let read_seq = self.latest_seq.load(Ordering::Acquire);
        if let Ok(mut reg) = self.snapshots.lock() {
            reg.register(read_seq);
        }
        Snapshot {
            engine: self,
            read_seq,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let read_seq = self.latest_seq.load(Ordering::Acquire);
        self.get_at(key, read_seq)
    }

    pub fn get_with_bloom_setting(&self, key: &[u8], use_bloom: bool) -> Result<Option<Vec<u8>>> {
        let read_seq = self.latest_seq.load(Ordering::Acquire);
        self.get_at_with_bloom(key, read_seq, use_bloom)
    }

    pub fn get_at(&self, key: &[u8], read_seq: u64) -> Result<Option<Vec<u8>>> {
        self.get_at_with_bloom(key, read_seq, self.options.use_bloom_filters)
    }

    fn get_at_with_bloom(
        &self,
        key: &[u8],
        read_seq: u64,
        use_bloom: bool,
    ) -> Result<Option<Vec<u8>>> {
        let (active_mem, imm_mems, levels) = {
            let state = match self.state.read() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            (
                Arc::clone(&state.active_mem),
                state.imm_mems.clone(),
                state.levels.clone(),
            )
        };

        if let Some(res) = active_mem.get(key, read_seq) {
            return Ok(match res {
                LookupResult::Value(v) => Some(v),
                LookupResult::Tombstone => None,
            });
        }

        for imm in &imm_mems {
            if let Some(res) = imm.get(key, read_seq) {
                return Ok(match res {
                    LookupResult::Value(v) => Some(v),
                    LookupResult::Tombstone => None,
                });
            }
        }

        // L0: overlapping SSTables, newest-first.
        for sst in &levels[0] {
            if let Some(res) = lookup_sst(sst, key, read_seq, use_bloom)? {
                return Ok(match res {
                    LookupResult::Value(v) => Some(v),
                    LookupResult::Tombstone => None,
                });
            }
        }

        // L1+: non-overlapping key ranges.
        for level in &levels[1..] {
            for sst in level {
                if key_in_sst_range(key, &sst.meta) {
                    if let Some(res) = lookup_sst(sst, key, read_seq, use_bloom)? {
                        return Ok(match res {
                            LookupResult::Value(v) => Some(v),
                            LookupResult::Tombstone => None,
                        });
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let read_seq = self.latest_seq.load(Ordering::Acquire);
        self.scan_at(start_key, end_key, read_seq)
    }

    pub fn scan_at(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_seq: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start_key > end_key {
            return Ok(Vec::new());
        }

        let (active_mem, imm_mems, levels) = {
            let state = match self.state.read() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            (
                Arc::clone(&state.active_mem),
                state.imm_mems.clone(),
                state.levels.clone(),
            )
        };

        // Collect all visible versions keyed by InternalKey (UserKey ASC, Seq DESC).
        let mut merged: BTreeMap<InternalKey, Vec<u8>> = BTreeMap::new();

        let mut collect_entries = |entries: Vec<(InternalKey, Vec<u8>)>| {
            for (k, v) in entries {
                if k.user_key.as_slice() >= start_key
                    && k.user_key.as_slice() <= end_key
                    && k.seq <= read_seq
                {
                    merged.insert(k, v);
                }
            }
        };

        collect_entries(active_mem.snapshot_entries());
        for imm in &imm_mems {
            collect_entries(imm.snapshot_entries());
        }
        for level in &levels {
            for sst in level {
                let mut reader = SstableReader::open(&sst.meta.path)?;
                collect_entries(reader.scan(start_key, end_key)?);
            }
        }

        let mut out = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;
        for (k, v) in merged {
            if last_user_key.as_deref() == Some(k.user_key.as_slice()) {
                continue;
            }
            last_user_key = Some(k.user_key.clone());
            if k.kind == RecordKind::Put {
                out.push((k.user_key, v));
            }
        }

        Ok(out)
    }

    pub fn stats(&self) -> EngineStats {
        match self.stats.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    pub fn level_counts(&self) -> Vec<usize> {
        let state = match self.state.read() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        state.levels.iter().map(|l| l.len()).collect()
    }

    fn notify_worker(&self) {
        let (lock, cvar) = &*self.worker_signal;
        if let Ok(mut sig) = lock.lock() {
            sig.pending = true;
            cvar.notify_one();
        }
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.worker_signal;
        if let Ok(mut sig) = lock.lock() {
            sig.shutdown = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

fn parse_sst_filename(stem: &str) -> Option<(u32, u64)> {
    let rest = stem.strip_prefix('L')?;
    let (lvl_str, id_str) = rest.split_once('_')?;
    let level = lvl_str.parse::<u32>().ok()?;
    let id = id_str.parse::<u64>().ok()?;
    Some((level, id))
}

fn key_in_sst_range(key: &[u8], meta: &SstableMeta) -> bool {
    match (&meta.min_key, &meta.max_key) {
        (Some(min_k), Some(max_k)) => {
            key >= min_k.user_key.as_slice() && key <= max_k.user_key.as_slice()
        }
        _ => false,
    }
}

fn lookup_sst(
    sst: &Arc<SstHandle>,
    key: &[u8],
    read_seq: u64,
    use_bloom: bool,
) -> Result<Option<LookupResult>> {
    if !key_in_sst_range(key, &sst.meta) {
        return Ok(None);
    }
    let mut reader = SstableReader::open(&sst.meta.path)?;
    reader.get(key, read_seq, use_bloom)
}

fn flush_all_imm(
    dir: &Path,
    state: &Arc<RwLock<EngineState>>,
    next_id: &Arc<AtomicU64>,
    stats: &Arc<Mutex<EngineStats>>,
    flush_lock: &Mutex<()>,
) -> Result<()> {
    let _flush_guard = match flush_lock.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    loop {
        let imm_opt = {
            let guard = match state.read() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.imm_mems.last().cloned()
        };

        let Some(imm) = imm_opt else {
            break;
        };

        let sst_id = next_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = dir.join(format!("L0_{sst_id:08}.sst"));
        let meta = SstableBuilder::build_from_memtable(sst_id, 0, &sst_path, &imm)?;
        let written = meta.file_size;

        let handle = Arc::new(SstHandle {
            meta,
            obsolete: AtomicBool::new(false),
        });

        {
            let mut guard = match state.write() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.imm_mems.retain(|m| m.id() != imm.id());
            guard.levels[0].insert(0, handle);
        }

        if let Ok(mut st) = stats.lock() {
            st.flush_bytes_written += written;
        }
    }
    Ok(())
}

fn run_compaction_pass(
    dir: &Path,
    options: &EngineOptions,
    state: &Arc<RwLock<EngineState>>,
    next_id: &Arc<AtomicU64>,
    snapshots: &Arc<Mutex<SnapshotRegistry>>,
    stats: &Arc<Mutex<EngineStats>>,
    compaction_lock: &Mutex<()>,
) -> Result<()> {
    let _cmp_guard = match compaction_lock.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    // 1. L0 -> L1 compaction when L0 reaches trigger.
    loop {
        let (l0_handles, l1_handles, is_bottommost) = {
            let guard = match state.read() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if guard.levels[0].len() < options.l0_compaction_trigger {
                break;
            }
            let l0 = guard.levels[0].clone();
            let mut l0_min: Option<InternalKey> = None;
            let mut l0_max: Option<InternalKey> = None;
            for h in &l0 {
                if let Some(ref mk) = h.meta.min_key {
                    if l0_min.as_ref().map(|m| mk < m).unwrap_or(true) {
                        l0_min = Some(mk.clone());
                    }
                }
                if let Some(ref xk) = h.meta.max_key {
                    if l0_max.as_ref().map(|m| xk > m).unwrap_or(true) {
                        l0_max = Some(xk.clone());
                    }
                }
            }
            let l1: Vec<Arc<SstHandle>> = guard.levels[1]
                .iter()
                .filter(|h| overlaps_range(&l0_min, &l0_max, &h.meta.min_key, &h.meta.max_key))
                .cloned()
                .collect();
            let bottom = guard.levels[2..].iter().all(|l| l.is_empty());
            (l0, l1, bottom)
        };

        let mut inputs: Vec<SstableMeta> = l0_handles.iter().map(|h| h.meta.clone()).collect();
        inputs.extend(l1_handles.iter().map(|h| h.meta.clone()));

        let watermark = snapshots
            .lock()
            .map(|r| r.watermark())
            .unwrap_or(u64::MAX);

        let (new_metas, bytes_written) = compact_sstables(
            &inputs,
            1,
            is_bottommost,
            watermark,
            options.target_sst_bytes,
            || next_id.fetch_add(1, Ordering::SeqCst),
            dir,
        )?;

        let new_handles: Vec<Arc<SstHandle>> = new_metas
            .into_iter()
            .map(|meta| {
                Arc::new(SstHandle {
                    meta,
                    obsolete: AtomicBool::new(false),
                })
            })
            .collect();

        {
            let mut guard = match state.write() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let l0_ids: Vec<u64> = l0_handles.iter().map(|h| h.meta.id).collect();
            let l1_ids: Vec<u64> = l1_handles.iter().map(|h| h.meta.id).collect();
            guard.levels[0].retain(|h| !l0_ids.contains(&h.meta.id));
            guard.levels[1].retain(|h| !l1_ids.contains(&h.meta.id));
            guard.levels[1].extend(new_handles);
            guard.levels[1].sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));

            for h in l0_handles.iter().chain(l1_handles.iter()) {
                h.obsolete.store(true, Ordering::Release);
            }
        }

        if let Ok(mut st) = stats.lock() {
            st.compaction_bytes_written += bytes_written;
        }
    }

    // 2. Level N -> Level N+1 compaction when Level N exceeds byte budget.
    let mut level_budget = options.l1_max_bytes;
    for lvl in 1..6usize {
        let (picked, next_overlapping, is_bottommost) = {
            let guard = match state.read() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let lvl_bytes: u64 = guard.levels[lvl].iter().map(|h| h.meta.file_size).sum();
            if lvl_bytes <= level_budget || guard.levels[lvl].is_empty() {
                level_budget = level_budget.saturating_mul(options.level_multiplier);
                continue;
            }
            let picked = guard.levels[lvl][0].clone();
            let overlapping: Vec<Arc<SstHandle>> = guard.levels[lvl + 1]
                .iter()
                .filter(|h| {
                    overlaps_range(
                        &picked.meta.min_key,
                        &picked.meta.max_key,
                        &h.meta.min_key,
                        &h.meta.max_key,
                    )
                })
                .cloned()
                .collect();
            let bottom = guard.levels[(lvl + 2)..].iter().all(|l| l.is_empty());
            (picked, overlapping, bottom)
        };

        let mut inputs = vec![picked.meta.clone()];
        inputs.extend(next_overlapping.iter().map(|h| h.meta.clone()));

        let watermark = snapshots
            .lock()
            .map(|r| r.watermark())
            .unwrap_or(u64::MAX);

        let (new_metas, bytes_written) = compact_sstables(
            &inputs,
            (lvl + 1) as u32,
            is_bottommost,
            watermark,
            options.target_sst_bytes,
            || next_id.fetch_add(1, Ordering::SeqCst),
            dir,
        )?;

        let new_handles: Vec<Arc<SstHandle>> = new_metas
            .into_iter()
            .map(|meta| {
                Arc::new(SstHandle {
                    meta,
                    obsolete: AtomicBool::new(false),
                })
            })
            .collect();

        {
            let mut guard = match state.write() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let next_ids: Vec<u64> = next_overlapping.iter().map(|h| h.meta.id).collect();
            guard.levels[lvl].retain(|h| h.meta.id != picked.meta.id);
            guard.levels[lvl + 1].retain(|h| !next_ids.contains(&h.meta.id));
            guard.levels[lvl + 1].extend(new_handles);
            guard.levels[lvl + 1].sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));

            picked.obsolete.store(true, Ordering::Release);
            for h in &next_overlapping {
                h.obsolete.store(true, Ordering::Release);
            }
        }

        if let Ok(mut st) = stats.lock() {
            st.compaction_bytes_written += bytes_written;
        }
        level_budget = level_budget.saturating_mul(options.level_multiplier);
    }

    Ok(())
}

fn background_worker(
    dir: PathBuf,
    options: EngineOptions,
    state: Arc<RwLock<EngineState>>,
    next_id: Arc<AtomicU64>,
    snapshots: Arc<Mutex<SnapshotRegistry>>,
    stats: Arc<Mutex<EngineStats>>,
    flush_lock: Arc<Mutex<()>>,
    compaction_lock: Arc<Mutex<()>>,
    signal: Arc<(Mutex<WorkerSignal>, Condvar)>,
) {
    let (lock, cvar) = &*signal;
    loop {
        {
            let mut sig = match lock.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            while !sig.pending && !sig.shutdown {
                sig = match cvar.wait(sig) {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
            }
            if sig.shutdown {
                break;
            }
            sig.pending = false;
        }

        let _ = flush_all_imm(&dir, &state, &next_id, &stats, &flush_lock);
        let _ = run_compaction_pass(
            &dir,
            &options,
            &state,
            &next_id,
            &snapshots,
            &stats,
            &compaction_lock,
        );
    }
}

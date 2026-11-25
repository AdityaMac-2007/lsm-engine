use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::engine::EngineOptions;
use lsm_engine::tsdb::{Aggregation, DataPoint, Tsdb};
use lsm_engine::wal::FsyncPolicy;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir()
            .join(format!("lsm_tsdb_{name}_{}_{}", std::process::id(), nanos));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn out_of_order_ingest_returns_chronological_and_isolates_metric_prefixes() {
    let dir = TempDir::new("ordering");
    let db = Tsdb::open(&dir.path, EngineOptions { fsync_policy: FsyncPolicy::Never, ..Default::default() })
        .expect("open tsdb");

    // Ingest out-of-order timestamps + similar prefix metric names ("cpu" vs "cpu_load").
    db.ingest("cpu", 3_000, 30.0).expect("ingest");
    db.ingest("cpu", 1_000, 10.0).expect("ingest");
    db.ingest("cpu", 2_000, 20.0).expect("ingest");
    db.ingest("cpu_load", 1_500, 99.0).expect("ingest");

    let cpu_pts = db.query_range("cpu", 0, 10_000).expect("query cpu");
    assert_eq!(
        cpu_pts,
        vec![
            DataPoint { timestamp_ms: 1_000, value: 10.0 },
            DataPoint { timestamp_ms: 2_000, value: 20.0 },
            DataPoint { timestamp_ms: 3_000, value: 30.0 },
        ]
    );

    let load_pts = db.query_range("cpu_load", 0, 10_000).expect("query cpu_load");
    assert_eq!(
        load_pts,
        vec![DataPoint { timestamp_ms: 1_500, value: 99.0 }]
    );
}

#[test]
fn downsampling_aggregates_buckets_accurately() {
    let dir = TempDir::new("downsample");
    let db = Tsdb::open(&dir.path, EngineOptions { fsync_policy: FsyncPolicy::Never, ..Default::default() })
        .expect("open tsdb");

    // Bucket 10_000: [10, 20, 30] -> avg 20, max 30, min 10, sum 60, count 3
    // Bucket 20_000: [40, 60]     -> avg 50, max 60, min 40, sum 100, count 2
    let batch = [
        ("mem.mb", 10_100, 10.0),
        ("mem.mb", 12_500, 20.0),
        ("mem.mb", 19_999, 30.0),
        ("mem.mb", 20_000, 40.0),
        ("mem.mb", 25_000, 60.0),
    ];
    db.ingest_batch(&batch).expect("ingest batch");

    let avg = db.downsample("mem.mb", 0, 30_000, 10_000, Aggregation::Avg).expect("avg");
    assert_eq!(avg, vec![
        DataPoint { timestamp_ms: 10_000, value: 20.0 },
        DataPoint { timestamp_ms: 20_000, value: 50.0 },
    ]);

    let max = db.downsample("mem.mb", 0, 30_000, 10_000, Aggregation::Max).expect("max");
    assert_eq!(max[0].value, 30.0);
    assert_eq!(max[1].value, 60.0);

    let min = db.downsample("mem.mb", 0, 30_000, 10_000, Aggregation::Min).expect("min");
    assert_eq!(min[0].value, 10.0);
    assert_eq!(min[1].value, 40.0);

    let count = db.downsample("mem.mb", 0, 30_000, 10_000, Aggregation::Count).expect("count");
    assert_eq!(count[0].value, 3.0);
    assert_eq!(count[1].value, 2.0);
}

#[test]
fn retention_gc_and_restart_persistence() {
    let dir = TempDir::new("retention");
    let opts = EngineOptions {
        fsync_policy: FsyncPolicy::Always,
        memtable_threshold: 512,
        l0_compaction_trigger: 2,
        ..Default::default()
    };

    {
        let db = Tsdb::open(&dir.path, opts.clone()).expect("open");
        for ts in [1_000, 2_000, 3_000, 4_000, 5_000] {
            db.ingest("req.latency", ts, ts as f64 / 100.0).expect("ingest");
        }
        db.flush().expect("flush 1");

        let deleted = db.enforce_retention("req.latency", 2_500).expect("retention");
        assert_eq!(deleted, 2);
        db.flush().expect("flush 2");
        db.compact_now().expect("compact");

        let remaining = db.query_range("req.latency", 0, 10_000).expect("query");
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[0].timestamp_ms, 3_000);
    }

    // Reopen from disk and verify durability of surviving points & tombstones.
    let db2 = Tsdb::open(&dir.path, opts).expect("reopen");
    let after_restart = db2.query_range("req.latency", 0, 10_000).expect("query after restart");
    assert_eq!(after_restart.len(), 3);
    assert_eq!(after_restart[0].timestamp_ms, 3_000);
    assert_eq!(after_restart[2].timestamp_ms, 5_000);
}

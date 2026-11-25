use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lsm_engine::engine::{EngineOptions, LsmEngine};
use lsm_engine::wal::FsyncPolicy;

fn percentile_us(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx.min(sorted_us.len() - 1)]
}

fn run_read_bench(
    db: &LsmEngine,
    num_records: u64,
    num_probes: u64,
    present: bool,
    use_bloom: bool,
) -> (f64, f64, f64) {
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut latencies_us = Vec::with_capacity(num_probes as usize);

    for i in 0..num_probes {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = if present {
            let k_idx = rng % num_records;
            format!("k_{k_idx:010}").into_bytes()
        } else {
            format!("z_missing_{i:010}").into_bytes()
        };

        let t0 = Instant::now();
        let _ = db
            .get_with_bloom_setting(&key, use_bloom)
            .expect("bench get");
        latencies_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }

    latencies_us.sort_by(|a, b| a.total_cmp(b));
    (
        percentile_us(&latencies_us, 0.50),
        percentile_us(&latencies_us, 0.95),
        percentile_us(&latencies_us, 0.99),
    )
}

fn main() {
    let mut num_records: u64 = 10_000_000;
    let mut num_probes: u64 = 5_000;

    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--records" if i + 1 < args.len() => {
                num_records = args[i + 1].parse().expect("valid --records");
                i += 2;
            }
            "--probes" if i + 1 < args.len() => {
                num_probes = args[i + 1].parse().expect("valid --probes");
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let bench_dir: PathBuf = std::env::temp_dir().join(format!("lsm_bench_{nanos}"));
    fs::create_dir_all(&bench_dir).expect("create bench dir");

    let opts = EngineOptions {
        fsync_policy: FsyncPolicy::EveryN(10_000),
        memtable_threshold: 4 * 1024 * 1024,
        l0_compaction_trigger: 4,
        l1_max_bytes: 32 * 1024 * 1024,
        level_multiplier: 10,
        target_sst_bytes: 4 * 1024 * 1024,
        use_bloom_filters: true,
    };

    let db = LsmEngine::open(&bench_dir, opts).expect("open bench db");
    let val = vec![b'V'; 64];

    let t_write_start = Instant::now();
    for idx in 0..num_records {
        let key = format!("k_{idx:010}");
        db.put(key.as_bytes(), &val).expect("bench put");
    }
    db.flush().expect("final flush");
    db.compact_now().expect("final compact");
    let write_secs = t_write_start.elapsed().as_secs_f64();

    let stats = db.stats();
    let waf = stats.write_amplification();
    let write_throughput = num_records as f64 / write_secs;

    let (pos_bloom_p50, pos_bloom_p95, pos_bloom_p99) =
        run_read_bench(&db, num_records, num_probes, true, true);
    let (pos_nobloom_p50, pos_nobloom_p95, pos_nobloom_p99) =
        run_read_bench(&db, num_records, num_probes, true, false);
    let (neg_bloom_p50, neg_bloom_p95, neg_bloom_p99) =
        run_read_bench(&db, num_records, num_probes, false, true);
    let (neg_nobloom_p50, neg_nobloom_p95, neg_nobloom_p99) =
        run_read_bench(&db, num_records, num_probes, false, false);

    println!(
        "{{\n  \"records\": {},\n  \"write_secs\": {:.3},\n  \"write_ops_per_sec\": {:.1},\n  \"wal_bytes\": {},\n  \"flush_bytes\": {},\n  \"compaction_bytes\": {},\n  \"logical_bytes\": {},\n  \"waf\": {:.3},\n  \"present_bloom_us\": [{:.2}, {:.2}, {:.2}],\n  \"present_nobloom_us\": [{:.2}, {:.2}, {:.2}],\n  \"absent_bloom_us\": [{:.2}, {:.2}, {:.2}],\n  \"absent_nobloom_us\": [{:.2}, {:.2}, {:.2}]\n}}",
        num_records,
        write_secs,
        write_throughput,
        stats.wal_bytes_written,
        stats.flush_bytes_written,
        stats.compaction_bytes_written,
        stats.logical_user_bytes,
        waf,
        pos_bloom_p50, pos_bloom_p95, pos_bloom_p99,
        pos_nobloom_p50, pos_nobloom_p95, pos_nobloom_p99,
        neg_bloom_p50, neg_bloom_p95, neg_bloom_p99,
        neg_nobloom_p50, neg_nobloom_p95, neg_nobloom_p99,
    );

    drop(db);
    let _ = fs::remove_dir_all(&bench_dir);
}

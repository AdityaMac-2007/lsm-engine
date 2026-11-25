pub mod codec;

use std::collections::BTreeMap;
use std::path::Path;

use crate::engine::{EngineOptions, EngineStats, LsmEngine};
use crate::error::{Result, WalError};
use codec::{decode_ts_key, decode_value, encode_ts_key, encode_value};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    pub timestamp_ms: u64,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    Avg,
    Min,
    Max,
    Sum,
    Count,
}

impl Aggregation {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "avg" => Ok(Aggregation::Avg),
            "min" => Ok(Aggregation::Min),
            "max" => Ok(Aggregation::Max),
            "sum" => Ok(Aggregation::Sum),
            "count" => Ok(Aggregation::Count),
            _ => Err(WalError::InvalidArg("unsupported aggregation (use avg, min, max, sum, count)")),
        }
    }
}

pub struct Tsdb {
    engine: LsmEngine,
}

impl Tsdb {
    pub fn open(dir: impl AsRef<Path>, options: EngineOptions) -> Result<Self> {
        let engine = LsmEngine::open(dir, options)?;
        Ok(Self { engine })
    }

    pub fn ingest(&self, metric: &str, timestamp_ms: u64, value: f64) -> Result<u64> {
        if !value.is_finite() {
            return Err(WalError::InvalidArg("metric value must be finite"));
        }
        let key = encode_ts_key(metric, timestamp_ms)?;
        let val = encode_value(value);
        self.engine.put(&key, &val)
    }

    pub fn ingest_batch(&self, points: &[(&str, u64, f64)]) -> Result<usize> {
        let mut count = 0usize;
        for &(metric, ts, val) in points {
            self.ingest(metric, ts, val)?;
            count += 1;
        }
        Ok(count)
    }

    /// Queries all data points for `metric` in `[start_ms, end_ms]` (inclusive)
    /// using an MVCC snapshot for point-in-time consistency.
    pub fn query_range(
        &self,
        metric: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<DataPoint>> {
        if start_ms > end_ms {
            return Ok(Vec::new());
        }
        let start_key = encode_ts_key(metric, start_ms)?;
        let end_key = encode_ts_key(metric, end_ms)?;
        let snap = self.engine.snapshot();
        let raw = snap.scan(&start_key, &end_key)?;

        let mut points = Vec::with_capacity(raw.len());
        for (k, v) in raw {
            let (decoded_metric, ts) = decode_ts_key(&k)?;
            if decoded_metric == metric {
                let value = decode_value(&v)?;
                points.push(DataPoint {
                    timestamp_ms: ts,
                    value,
                });
            }
        }
        Ok(points)
    }

    /// Downsamples `metric` over `[start_ms, end_ms]` into fixed buckets of `bucket_ms`.
    pub fn downsample(
        &self,
        metric: &str,
        start_ms: u64,
        end_ms: u64,
        bucket_ms: u64,
        agg: Aggregation,
    ) -> Result<Vec<DataPoint>> {
        if bucket_ms == 0 {
            return Err(WalError::InvalidArg("bucket_ms must be > 0"));
        }
        let raw = self.query_range(metric, start_ms, end_ms)?;
        if raw.is_empty() {
            return Ok(Vec::new());
        }

        let mut buckets: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
        for pt in raw {
            let bucket_start = (pt.timestamp_ms / bucket_ms) * bucket_ms;
            buckets.entry(bucket_start).or_default().push(pt.value);
        }

        let mut out = Vec::with_capacity(buckets.len());
        for (bucket_ts, vals) in buckets {
            let aggregated = match agg {
                Aggregation::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
                Aggregation::Min => vals
                    .iter()
                    .copied()
                    .fold(f64::INFINITY, f64::min),
                Aggregation::Max => vals
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, f64::max),
                Aggregation::Sum => vals.iter().sum::<f64>(),
                Aggregation::Count => vals.len() as f64,
            };
            out.push(DataPoint {
                timestamp_ms: bucket_ts,
                value: aggregated,
            });
        }
        Ok(out)
    }

    /// Deletes all points for `metric` with `timestamp_ms <= older_than_ms`
    /// by writing LSM Tombstones. Physical space is reclaimed on bottommost compaction.
    pub fn enforce_retention(&self, metric: &str, older_than_ms: u64) -> Result<usize> {
        let expired = self.query_range(metric, 0, older_than_ms)?;
        for pt in &expired {
            let key = encode_ts_key(metric, pt.timestamp_ms)?;
            self.engine.delete(&key)?;
        }
        Ok(expired.len())
    }

    pub fn flush(&self) -> Result<()> {
        self.engine.flush()
    }

    pub fn compact_now(&self) -> Result<()> {
        self.engine.compact_now()
    }

    pub fn stats(&self) -> EngineStats {
        self.engine.stats()
    }

    pub fn level_counts(&self) -> Vec<usize> {
        self.engine.level_counts()
    }
}

use crate::error::{Result, WalError};

/// Composite key format:
/// `[metric_name UTF-8 bytes][0x00 separator][timestamp_ms: u64 BIG-ENDIAN]`
///
/// Big-endian timestamp encoding ensures lexicographical byte ordering in the
/// LSM-Tree matches strict chronological order for every metric.
/// The `0x00` separator prevents prefix collisions (e.g., `cpu` vs `cpu_load`).
pub const METRIC_SEPARATOR: u8 = 0x00;

pub fn encode_ts_key(metric: &str, timestamp_ms: u64) -> Result<Vec<u8>> {
    if metric.is_empty() {
        return Err(WalError::InvalidArg("metric name must not be empty"));
    }
    if metric.as_bytes().contains(&METRIC_SEPARATOR) {
        return Err(WalError::InvalidArg("metric name must not contain null bytes"));
    }
    let mut key = Vec::with_capacity(metric.len() + 1 + 8);
    key.extend_from_slice(metric.as_bytes());
    key.push(METRIC_SEPARATOR);
    key.extend_from_slice(&timestamp_ms.to_be_bytes());
    Ok(key)
}

pub fn decode_ts_key(key: &[u8]) -> Result<(String, u64)> {
    if key.len() < 10 {
        return Err(WalError::InvalidArg("time-series key too short"));
    }
    let ts_start = key.len() - 8;
    let sep_idx = ts_start - 1;
    if key[sep_idx] != METRIC_SEPARATOR {
        return Err(WalError::InvalidArg("missing metric separator in key"));
    }
    let metric = std::str::from_utf8(&key[..sep_idx])
        .map_err(|_| WalError::InvalidArg("metric name is not valid UTF-8"))?
        .to_string();
    let ts = u64::from_be_bytes([
        key[ts_start],
        key[ts_start + 1],
        key[ts_start + 2],
        key[ts_start + 3],
        key[ts_start + 4],
        key[ts_start + 5],
        key[ts_start + 6],
        key[ts_start + 7],
    ]);
    Ok((metric, ts))
}

#[inline]
pub fn encode_value(value: f64) -> [u8; 8] {
    value.to_le_bytes()
}

pub fn decode_value(bytes: &[u8]) -> Result<f64> {
    if bytes.len() != 8 {
        return Err(WalError::InvalidArg("time-series value must be 8 bytes (f64)"));
    }
    Ok(f64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

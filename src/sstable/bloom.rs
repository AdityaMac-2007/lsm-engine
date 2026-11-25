/// Per-SSTable Bloom Filter using Kirsch-Mitzenmacher double hashing.
/// Binary layout: `[num_probes: u8][bitset_bytes...]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    num_probes: u8,
    bits: Vec<u8>,
    bit_len: usize,
}

impl BloomFilter {
    /// Builds a Bloom filter for `num_keys` with `bits_per_key` (recommended: 10 for ~1% FPR).
    pub fn build<'a, I>(keys: I, num_keys: usize, bits_per_key: usize) -> Self
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let bits_per_key = bits_per_key.max(2);
        let bit_len = (num_keys.saturating_mul(bits_per_key)).max(64);
        let byte_len = (bit_len + 7) / 8;
        let actual_bit_len = byte_len * 8;

        // Optimal k = ln(2) * bits_per_key ≈ 0.69 * bits_per_key, clamped to [1, 30].
        let k = (((bits_per_key as f64) * std::f64::consts::LN_2).round() as usize).clamp(1, 30);
        let mut bits = vec![0u8; byte_len];

        for key in keys {
            let (h1, h2) = hash_pair(key);
            for i in 0..k {
                let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
                let bit_pos = (combined as usize) % actual_bit_len;
                bits[bit_pos / 8] |= 1 << (bit_pos % 8);
            }
        }

        Self {
            num_probes: k as u8,
            bits,
            bit_len: actual_bit_len,
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bit_len == 0 || self.bits.is_empty() {
            return true;
        }
        let (h1, h2) = hash_pair(key);
        for i in 0..self.num_probes as usize {
            let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit_pos = (combined as usize) % self.bit_len;
            if (self.bits[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
        }
        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.bits.len());
        out.push(self.num_probes);
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }
        let num_probes = bytes[0];
        let bits = bytes[1..].to_vec();
        let bit_len = bits.len() * 8;
        Some(Self {
            num_probes,
            bits,
            bit_len,
        })
    }
}

/// Produces two independent 64-bit hashes (FNV-1a 64 + SplitMix64 avalanche).
#[inline]
fn hash_pair(key: &[u8]) -> (u64, u64) {
    let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(0x0100_0000_01b3);
    }

    let mut h2 = h1.wrapping_add(0x9e37_79b9_7f4a_7c15);
    h2 = (h2 ^ (h2 >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h2 = (h2 ^ (h2 >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h2 ^= h2 >> 31;
    // Ensure h2 is odd so double-hashing cycles cleanly.
    (h1, h2 | 1)
}

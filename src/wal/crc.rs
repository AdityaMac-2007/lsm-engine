/// Standard IEEE 802.3 CRC32 (polynomial 0xEDB88320, reflected).
const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

const CRC32_TABLE: [u32; 256] = build_crc32_table();

#[inline]
pub fn crc32_update(mut state: u32, data: &[u8]) -> u32 {
    for &byte in data {
        let idx = ((state ^ u32::from(byte)) & 0xFF) as usize;
        state = (state >> 8) ^ CRC32_TABLE[idx];
    }
    state
}

#[inline]
pub fn crc32_parts(first: &[u8], second: &[u8]) -> u32 {
    let state = crc32_update(0xFFFF_FFFF, first);
    !crc32_update(state, second)
}

#[inline]
pub fn crc32(data: &[u8]) -> u32 {
    !crc32_update(0xFFFF_FFFF, data)
}

use crate::filter::FilterPolicy;

pub struct BloomFilter {
    pub bits_per_key: usize,
    pub k: usize,
}

impl BloomFilter {
    pub fn new(bits_per_key: usize) -> Self {
        // We intentionally round down to reduce probing cost a little bit.
        let mut k = bits_per_key as f32 * 0.69; // 0.69 =~ ln(2)
        if k < 1f32 {
            k = 1f32;
        } else if k > 30f32 {
            k = 30f32;
        }
        BloomFilter {
            bits_per_key,
            k: k as usize,
        }
    }
}

impl FilterPolicy for BloomFilter {
    fn name(&self) -> &str {
        "BloomFilter"
    }

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        todo!()
    }

    fn key_may_match(&self, filter: &[u8], key: &[u8]) -> bool {
        todo!()
    }
}
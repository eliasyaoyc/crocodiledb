use std::sync::Arc;
use crate::filter::FilterPolicy;
use crate::util::coding::{decode_fixed_32, put_fixed_32};

/// Generate new filter every 2KB of data.
const K_FILTER_BASE_LG: usize = 11;
const K_FILTER_BASE: usize = 1 << K_FILTER_BASE_LG;
const FILTER_META_LENGTH: usize = 5;
// 4bytes filter offsets length + 1bytes base log
const FILTER_OFFSET_LEN: usize = 4; // u32 length

/// A `FilterBlockBuilder` is used to construct all of the filters for a
/// particular Table. It generates a single string which is stored as
/// a special block in the Table.
///
/// The sequence of calls to `FilterBlockBuilder` must match the regexp:
///     (StartBlock AddKey) Finish.
pub struct FilterBlockBuilder {
    policy: Arc<dyn FilterPolicy>,
    // Key contents reused by every block.
    keys: Vec<Vec<u8>>,
    // All the filter block data computed so far
    // `data` includes filter trailer only after calling `finish`.
    //
    // |----- filter data -----|----- filter offsets ----|--- filter offsets len ---|--- BASE_LG ---|
    //
    data: Vec<u8>,
    // The offset of every filter in the data.
    filter_offsets: Vec<u32>,
}

impl FilterBlockBuilder {
    pub fn new(policy: Arc<dyn FilterPolicy>) -> Self {
        FilterBlockBuilder {
            policy,
            keys: vec![],
            data: vec![],
            filter_offsets: vec![],
        }
    }

    /// Generate filter data for the data block on given `block_offset`.
    pub fn start_block(&mut self, block_offset: u64) {
        let filter_index = block_offset / K_FILTER_BASE as u64;
        let filter_len = self.filter_offsets.len() as u64;
        assert!(
            filter_index >= filter_len,
            "[FilterBlockBuilder] the filter block index {} should larger the built filters {}.",
            filter_index,
            filter_len,
        );

        while filter_index > self.filter_offsets.len() as u64 {
            self.generate_filter();
        }
    }

    /// Adds the given key into the builder.
    pub fn add_key(&mut self, key: &[u8]) {
        let key = Vec::from(key);
        self.keys.push(key);
    }

    /// Appends the trailer of filter block and returns the filter block data in bytes
    pub fn finish(&mut self) -> &[u8] {
        if !self.keys.is_empty() {
            // Clean up the remaining keys.
            self.generate_filter();
        }

        // Append per-filter offsets
        for offset in self.filter_offsets.iter() {
            put_fixed_32(&mut self.data, *offset);
        }

        // Append the 4bytes offset length
        put_fixed_32(&mut self.data, self.filter_offsets.len() as u32);
        // Append the 1byte base lg.
        self.data.push(K_FILTER_BASE_LG as u8);
        &self.data
    }

    /// Converts 'keys' to an encoded filter vec by 'policy'
    fn generate_filter(&mut self) {
        if self.keys.is_empty() {
            // Fast path if there are no keys.
            self.filter_offsets.push(self.data.len() as u32);
            return;
        }
        // Generate filter for current set of keys and append to data.
        self.filter_offsets.push(self.data.len() as u32);
        let filter = self.policy.create_filter(&self.keys);
        self.data.extend(filter);
        // Clear the keys.
        self.keys.clear();
    }
}

pub struct FilterBlockReader {
    policy: Arc<dyn FilterPolicy>,
    // All filter block data without filter meta
    // | ----- filter data ----- | ----- filter offsets ----|
    //                                   num * 4 bytes
    data: Vec<u8>,
    // The amount of filter data.
    num: usize,
    base_lg: usize,
}

impl FilterBlockReader {
    pub fn new(policy: Arc<dyn FilterPolicy>, mut filter_block: Vec<u8>) -> Self {
        let mut r = FilterBlockReader {
            policy,
            data: vec![],
            num: 0,
            base_lg: 0,
        };
        let n = filter_block.len();
        if n < FILTER_META_LENGTH {
            return r;
        }
        r.num = decode_fixed_32(&filter_block[n - FILTER_META_LENGTH..n - 1]) as usize;
        // invalid filter offset length.
        if r.num * FILTER_OFFSET_LEN + FILTER_META_LENGTH > n {
            return r;
        }
        r.base_lg = filter_block[n - 1] as usize;
        filter_block.truncate(n - FILTER_META_LENGTH);
        r.data = filter_block;
        r
    }

    /// Returns true if the given key is probably contained in the given `block_offset` block.
    /// `block_offset` represent the offset of data_block in sstable.
    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
        let i = block_offset as usize >> self.base_lg; // a >> b == a / (1 << b)
        if i < self.num {
            let (filter, offset) = &self.data.split_at(self.data.len() - self.num * FILTER_OFFSET_LEN);
            let start = decode_fixed_32(&offset[i * FILTER_OFFSET_LEN..(i + 1) * FILTER_OFFSET_LEN]) as usize;
            let end = {
                if i + 1 >= self.num {
                    // this is the last filter
                    filter.len()
                } else {
                    decode_fixed_32(&offset[(i + 1) * FILTER_OFFSET_LEN..(i + 2) * FILTER_OFFSET_LEN]) as usize
                }
            };
            let filter = &self.data[start..end];
            return self.policy.key_may_match(filter, key);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::filter::FilterPolicy;
    use crate::sstable::filter_block::{FilterBlockBuilder, FilterBlockReader, K_FILTER_BASE_LG};
    use crate::util::coding::{decode_fixed_32, put_fixed_32};
    use crate::util::hash::hash;

    struct TestHashFilter {}

    impl FilterPolicy for TestHashFilter {
        fn name(&self) -> &str {
            "TestHashFilter"
        }

        fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
            let mut f = vec![];
            for i in 0..keys.len() {
                let h = hash(keys[i].as_slice(), 1);
                put_fixed_32(&mut f, h);
            }
            f
        }

        fn key_may_match(&self, filter: &[u8], key: &[u8]) -> bool {
            let h = hash(key, 1);
            let mut i = 0;
            while i + 4 <= filter.len() {
                if h == decode_fixed_32(&filter[i..i + 4]) {
                    return true;
                }
                i += 4;
            }
            false
        }
    }

    fn new_test_builder() -> FilterBlockBuilder {
        FilterBlockBuilder::new(Arc::new(TestHashFilter{}))
    }

    fn new_test_reader(block: Vec<u8>) -> FilterBlockReader {
        FilterBlockReader::new(Arc::new(TestHashFilter{}), block)
    }

    #[test]
    fn test_empty_builder() {
        let mut b = new_test_builder();
        let block = b.finish();
        assert_eq!(&[0, 0, 0, 0, K_FILTER_BASE_LG as u8], block);
        let r = new_test_reader(Vec::from(block));
        assert_eq!(r.key_may_match(0, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(10000,"foo".as_bytes()), true);
    }

    #[test]
    fn test_single_chunk(){
        let mut b = new_test_builder();
        b.start_block(100);
        b.add_key("foo".as_bytes());
        b.add_key("bar".as_bytes());
        b.add_key("box".as_bytes());
        b.start_block(200);
        b.add_key("box".as_bytes());
        b.start_block(300);
        b.add_key("hello".as_bytes());
        let block = b.finish();
        let r = new_test_reader(Vec::from(block));
        assert_eq!(r.key_may_match(100, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "bar".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "box".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "hello".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "missing".as_bytes()), false);
        assert_eq!(r.key_may_match(100, "other".as_bytes()), false);
    }

    #[test]
    fn test_multiple_chunk() {
        let mut b = new_test_builder();
        // first filter
        b.start_block(0);
        b.add_key("foo".as_bytes());
        b.start_block(2000);
        b.add_key("bar".as_bytes());

        // second filter
        b.start_block(3100);
        b.add_key("box".as_bytes());

        // third filter is empty

        // last filter
        b.start_block(9000);
        b.add_key("box".as_bytes());
        b.add_key("hello".as_bytes());
        let block = b.finish();
        let r = new_test_reader(Vec::from(block));

        // check first filter
        assert_eq!(r.key_may_match(0, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(2000, "bar".as_bytes()), true);
        assert_eq!(r.key_may_match(0, "box".as_bytes()), false);
        assert_eq!(r.key_may_match(0, "hello".as_bytes()), false);
        // check second filter
        assert_eq!(r.key_may_match(3100, "box".as_bytes()), true);
        assert_eq!(r.key_may_match(3100, "foo".as_bytes()), false);
        assert_eq!(r.key_may_match(3100, "bar".as_bytes()), false);
        assert_eq!(r.key_may_match(3100, "hello".as_bytes()), false);
        // check third filter (empty)
        assert_eq!(r.key_may_match(4100, "box".as_bytes()), false);
        assert_eq!(r.key_may_match(4100, "foo".as_bytes()), false);
        assert_eq!(r.key_may_match(4100, "bar".as_bytes()), false);
        assert_eq!(r.key_may_match(4100, "hello".as_bytes()), false);
        // check last filter
        assert_eq!(r.key_may_match(9000, "box".as_bytes()), true);
        assert_eq!(r.key_may_match(9000, "foo".as_bytes()), false);
        assert_eq!(r.key_may_match(9000, "bar".as_bytes()), false);
        assert_eq!(r.key_may_match(9000, "hello".as_bytes()), true);
    }
}
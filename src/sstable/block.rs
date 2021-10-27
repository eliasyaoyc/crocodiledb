use std::cmp::{min, Ordering};
use std::detect::__is_feature_detected::sha;
use std::mem::size_of;
use std::sync::Arc;
use crate::error::Error;
use crate::IResult;
use crate::iterator::Iter;
use crate::opt::Options;
use crate::util::coding::{decode_fixed_32, put_fixed_32, VarintU32};
use crate::util::comparator::Comparator;

/// BlockBuilder generates blocks where keys are prefix-compressed:
///
/// When we store a key, we drop the prefix shared with the previous
/// string. This helps reduce the space requirement significantly.
/// Furthermore, once every K keys, we do not apply the prefix
/// compression and store the entire key. We call this a "restart point".
/// The tail end of the block stores the offsets of all the restart points,
/// and can be used to do a binary search when looking for a particular key.
/// Values are stored as-is(without compression) immediately following the
/// corresponding key.
///
/// An entry for a particular key-value pair has the form:
///      shared_bytes: varint.
///      unshared_bytes: varint.
///      value_length: varint.
///      key_delta: char[unshared_bytes]
///      value: char[value_lengtj=h]
/// shared_bytes == 0 for restart points.
///
/// The trailer of the block has the form:
///      restarts: vec[i]    represent the offset of the u restart point in the block.
///      num_restarts: u32   represent the number of the restart point.
/// restarts[i] contains the offset within the block of the ith restart point.
#[derive(Debug)]
pub struct BlockBuilder<C: Comparator> {
    c: C,
    options: Options,
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    counter: u32,
    finished: bool,
    last_key: Vec<u8>,
}

impl<C: Comparator> BlockBuilder<C> {
    pub fn new(options: Options, c: C) -> Self {
        assert!(options.block_restart_interval >= 1,
                "[BlockBuilder] block restart interval must greater than 1, but got {}",
                options.block_restart_interval);

        let mut restarts = vec![];
        restarts.push(0); // First restart point is at offset 0.

        BlockBuilder {
            c,
            options,
            buffer: vec![],
            restarts,
            counter: 0,
            finished: false,
            last_key: vec![],
        }
    }

    /// Reset the contents as if the `BlockBuilder` was just constructed.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0); // First restart point is at offset 0.
        self.counter = 0;
        self.finished = false;
        self.last_key.clear();
    }

    /// `Finish()` has not been called since the last call to `Reset()`.
    /// Key is larger than any previously added key.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished,
                "[BlockBuilder] current block is over,so can't write.");
        assert!(self.counter <= self.options.block_restart_interval,
                "[BlockBuilder] The number of restart nodes is greater than the configured number {}",
                self.options.block_restart_interval);
        assert!(self.empty() || self.c.compare(key, self.last_key.as_slice()) == Ordering::Greater,
                "[BlockBuilder] Empty block or the key that you given greater than the last key {:?}",
                self.last_key);

        let mut shared = 0;
        if self.counter < self.options.block_restart_interval {
            // See how much sharing to do with previous string.
            let min_length = min(self.last_key.len(), key.len());
            while shared < min_length && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // Restart compression.
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        // Add "<shared><non-shared><value_size>" to buffer.
        VarintU32::put_varint(&mut self.buffer, shared as u32);
        VarintU32::put_varint(&mut self.buffer, non_shared as u32);
        VarintU32::put_varint(&mut self.buffer, value.len() as u32);

        // Add string delta to buffer followed by value.
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(&value[..]);

        // Update state.
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    /// Finish building the block and return a slice that refers to the
    /// block contents. The returned slice will remain valid for the
    /// lifetime of this builder or until `Reset()` is called.
    pub fn finish(&mut self) -> &[u8] {
        let len = self.restarts.len();
        // Append restart array.
        for i in 0..len {
            put_fixed_32(&mut self.buffer, self.restarts[i]);
        }
        // Append the restart length.
        put_fixed_32(&mut self.buffer, len as u32);
        self.buffer.as_slice()
    }

    /// Returns an estimate of the current(uncompressed) size of the block
    /// we are building.
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len()    // Raw data buffer.
            + self.restarts.len() * size_of::<u32>() // Restart array.
            + size_of::<u32>() // Restart array length.
    }

    /// Return true iff no entries have been added since the last `Reset()`.
    pub fn empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[derive(Debug)]
pub struct Block {
    // Arc instead of normal vec that avoid the data copy.
    data: Arc<Vec<u8>>,
    // Offset in data of restart array.
    restart_offset: u32,
    // The length o restarts.
    restarts_len: u32,
}

impl Block {
    pub fn new(data: Vec<u8>) -> IResult<Self> {
        let size = data.len();
        let u32_len = std::mem::size_of::<u32>();
        if size >= u32_len {
            let max_restarts_allowed = (data.len() - u32_len) / u32_len;
            let restarts_len = Block::restarts_len(&data);
            // Make sure the size is enough for restarts.
            if restarts_len as usize <= max_restarts_allowed {
                return Ok(Self {
                    data: Arc::new(data),
                    restart_offset: (size - (1 + restarts_len as usize) * u32_len) as u32,
                    restarts_len,
                });
            }
        }
        Err(Error::Corruption("[Block] read invalid block content."))
    }

    /// Returns the length of restarts vec.
    /// The last for bytes in data.
    pub fn restarts_len(data: &[u8]) -> u32 {
        let u32_len = std::mem::size_of::<u32>();
        assert!(
            data.len() >= u32_len,
            "[Block] size must be greater than u32 size, but got{}",
            data.len()
        );
        decode_fixed_32(&data[data.len() - u32_len..])
    }

    pub fn iter<C: Comparator>(&self, comparator: C) -> BlockIterator<C> {
        BlockIterator::new(
            comparator,
            self.data.clone(),
            self.restart_offset,
            self.restarts_len,
        )
    }
}

struct BlockIterator<C: Comparator> {
    comparator: C,
    // Underlying block contents.
    data: Arc<Vec<u8>>,
    // Offset of restart array(list of fixed32) in data.
    restarts: u32,
    // Number of u32 entries in restart array.
    restarts_len: u32,

    // Current is offset in data of current entry >= restart if !Valid.
    current: u32,
    // Index of restart block in which current falls.
    restart_index: u32,
    shared: u32,
    not_shared: u32,
    value_len: u32,
    key_offset: u32,
    key: Vec<u8>,
    err: Option<Error>,
}

impl<C: Comparator> BlockIterator<C> {
    pub fn new(comparator: C, data: Arc<Vec<u8>>, restarts: u32, restarts_len: u32) -> Self {
        BlockIterator {
            comparator,
            data,
            restarts,
            restarts_len,
            current: restarts,
            restart_index: restarts_len,
            shared: 0,
            not_shared: 0,
            value_len: 0,
            key_offset: 0,
            key: vec![],
            err: None,
        }
    }

    /// Returns the offset just pasts the end of the current entry.
    #[inline]
    fn next_entry_offset(&self) -> u32 {
        self.key_offset + self.not_shared + self.value_len
    }

    #[inline]
    fn get_restart_point(&self, index: u32) -> u32 {
        decode_fixed_32(&self.data[self.restarts as usize + index as usize * std::mem::size_of::<u32>()..])
    }

    #[inline]
    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
    }

    /// Decode a block entry from `current`
    /// mark as corrupted when the current entry tail overflows the starting offset of restarts.
    fn parse_block_entry(&mut self) -> bool {
        if self.current >= self.restarts {
            // Mark as invalid.
            self.current = self.restarts;
            self.restart_index = self.restarts_len;
            return false;
        }
        let offset = self.current;
        let src = &self.data[offset as usize..];
        let (shared, n0) = VarintU32::common_read(src);
        let (not_shared, n1) = VarintU32::common_read(&src[n0 as usize..]);
        let (value_len, n2) = VarintU32::common_read(&src[(n1 + n0) as usize..]);
        let n = (n0 + n1 + n2) as u32;
        if offset + n + not_shared + value_len > self.restarts {
            self.corruption_error();
            return false;
        }
        self.key_offset = self.current + n;
        self.shared = shared;
        self.not_shared = not_shared;
        self.value_len = value_len;
        let total_key_len = (shared + not_shared) as usize;
        self.key.resize(total_key_len, 0);
        // de-compress key.
        let delta = &self.data[self.key_offset as usize..(self.key_offset + not_shared) as usize];
        for i in shared as usize..total_key_len {
            self.key[i] = delta[i - shared as usize];
        }

        // update restart index.
        while self.restart_index + 1 < self.restarts_len
            && self.get_restart_point(self.restart_index + 1) < self.current
        {
            self.restart_index += 1;
        }
        true
    }

    #[inline]
    fn corruption_error(&mut self) {
        self.err = Some(Error::Corruption("bad entry in block"));
        self.key.clear();
        self.current = self.restarts;
        self.restart_index = self.restarts_len;
    }

    #[inline]
    fn valid_or_panic(&self) -> bool {
        if !self.valid() {
            panic!("[Block Iterator] invalid the current data offset {}: overflows the restart {}",
                   self.current, self.restarts
            )
        }
        true
    }
}

impl<C: Comparator> Iter for BlockIterator<C> {
    fn valid(&self) -> bool {
        self.err.is_none() && self.current < self.restarts
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_block_entry();
    }

    fn seek_to_last(&mut self) {
        // seek to the last restart offset.
        self.seek_to_restart_point(self.restarts_len - 1);
        // keeping parsing block util the last.
        while self.parse_block_entry() && self.next_entry_offset() < self.restarts {
            self.current = self.next_entry_offset()
        }
    }

    /// Find the first entry in block with key >= target.
    fn seek(&mut self, target: &[u8]) {
        // Binary search in restart array to find the last restart point with a key < target.
        let mut left = 0;
        let mut right = self.restarts_len - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);
            let src = &self.data[region_offset as usize..];
            let (shared, n0) = VarintU32::common_read(src);
            let (not_shared, n1) = VarintU32::common_read(&src[n0 as usize..]);
            let (_, n2) = VarintU32::common_read(&src[(n1 + n0) as usize..]);
            if shared != 0 {
                // The first key from restart offset should be completely stored.
                self.corruption_error();
                return;
            }

            let key_offset = region_offset + (n0 + n1 + n2) as u32;
            let key_len = (shared + not_shared) as usize;
            let mid_key = &self.data[key_offset as usize..key_offset as usize + key_len];
            match self.comparator.compare(mid_key, target) {
                Ordering::Less => left = mid,
                _ => right = mid - 1,
            }
        }

        // linear search (with restart block) for first key >= target
        // if all the keys > target, we seek to the start
        // if all the keys < target, we seek to the last
        self.seek_to_restart_point(left);
        loop {
            if !self.parse_block_entry() {
                return;
            }

            match self.comparator.compare(self.key.as_slice(), target) {
                Ordering::Less => {}
                _ => return,
            }
            self.current = self.next_entry_offset();
        }
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.current = self.next_entry_offset();
        self.parse_block_entry();
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        todo!()
    }

    fn prev(&mut self) {
        let original = self.current;
        // Find the first restart point that just less than the current offset.
        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                // No more entries marked as invalid.
                self.current = self.restarts;
                self.restart_index = self.restarts_len;
                return;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart_point(self.restart_index);
        // Loop until end of current entry hits the start of original entry.
        while self.parse_block_entry() && self.next_entry_offset() < original {
            self.current = self.next_entry_offset();
        }
    }

    fn key(&self) -> &[u8] {
        self.valid_or_panic();
        &self.key
    }

    fn value(&self) -> &[u8] {
        self.valid_or_panic();
        let val_offset = self.next_entry_offset() - self.value_len;
        &self.data[val_offset as usize..(val_offset + self.value_len) as usize]
    }

    fn status(&mut self) -> IResult<()> {
        if let Some(_err) = &self.err {
            return Err(self.err.take().unwrap());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::iterator::Iter;
    use crate::opt::Options;
    use crate::sstable::block::{Block, BlockBuilder, BlockIterator};
    use crate::util::coding::{decode_fixed_32, put_fixed_32, VarintU32};
    use crate::util::comparator::BytewiseComparator;
    use std::str;

    fn new_test_block() -> Vec<u8> {
        let mut samples = vec!["1", "12", "123", "abc", "abd", "acd", "bbb"];
        let opt = Options {
            table_size: 0,
            block_size: 0,
            write_buffer_size: 0,
            block_restart_interval: 3,
        };
        let mut builder = BlockBuilder::new(opt, BytewiseComparator::default());
        for key in samples.drain(..) {
            builder.add(key.as_bytes(), key.as_bytes());
        }
        // restarts: [0, 18, 42]
        // entries data size: 51
        Vec::from(builder.finish())
    }

    #[test]
    fn test_corrupted_block_create() {
        // Invalid data size.
        let res = Block::new(vec![0, 0, 0]);
        assert!(res.is_err());

        let mut data = vec![];
        let mut test_restarts = vec![0, 10, 20];
        let length = test_restarts.len() as u32;
        for restart in test_restarts.drain(..) {
            put_fixed_32(&mut data, restart);
        }
        // Append invalid length of restarts
        put_fixed_32(&mut data, length + 1);
        let res = Block::new(data);
        assert!(res.is_err());
    }

    #[test]
    fn test_new_empty_block() {
        let ucmp = BytewiseComparator::default();
        let opt = Options {
            table_size: 0,
            block_size: 0,
            write_buffer_size: 0,
            block_restart_interval: 2,
        };
        let mut builder = BlockBuilder::new(opt, ucmp);
        let data = builder.finish();
        let length = data.len();
        let restarts_len = decode_fixed_32(&data[length - 4..length]);
        let restarts = &data[..length - 4];
        assert_eq!(restarts_len, 1);
        assert_eq!(restarts.len() as u32 / 4, restarts_len);
        assert_eq!(decode_fixed_32(restarts), 0);
        let block = Block::new(Vec::from(data)).unwrap();
        let iter = block.iter(ucmp);
        assert!(!iter.valid());
    }

    #[test]
    fn test_new_block_from_bytes() {
        let data = new_test_block();
        assert_eq!(Block::restarts_len(&data), 3);
        let block = Block::new(data).unwrap();
        assert_eq!(block.restart_offset, 51);
    }

    #[test]
    fn test_simple_empty_key() {
        let ucmp = BytewiseComparator::default();
        let opt = Options {
            table_size: 0,
            block_size: 0,
            write_buffer_size: 0,
            block_restart_interval: 2,
        };
        let mut builder = BlockBuilder::new(opt, ucmp);
        builder.add(b"", b"test");
        let data = builder.finish();
        let block = Block::new(Vec::from(data)).unwrap();
        let mut iter = block.iter(ucmp);
        iter.seek("".as_bytes());
        assert!(iter.valid());
        let k = iter.key();
        let v = iter.value();
        assert_eq!(str::from_utf8(k).unwrap(), "");
        assert_eq!(str::from_utf8(v).unwrap(), "test");
        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    #[should_panic]
    fn test_add_inconsistent_key() {
        let opt = Options {
            table_size: 0,
            block_size: 0,
            write_buffer_size: 0,
            block_restart_interval: 2,
        };
        let mut builder = BlockBuilder::new(opt, BytewiseComparator::default());
        builder.add(b"ffffff", b"");
        builder.add(b"a", b"");
    }

    #[test]
    fn test_write_entries() {
        let opt = Options {
            table_size: 0,
            block_size: 0,
            write_buffer_size: 0,
            block_restart_interval: 3,
        };
        let mut builder = BlockBuilder::new(opt, BytewiseComparator::default());
        assert!(builder.last_key.is_empty());
        // Basic key
        builder.add(b"1111", b"val1");
        assert_eq!(1, builder.counter);
        assert_eq!(&builder.last_key, b"1111");
        let (shared, n1) = VarintU32::common_read(&builder.buffer);
        assert_eq!(0, shared);
        assert_eq!(1, n1);
        let (non_shared, n2) = VarintU32::common_read(&builder.buffer[n1 as usize..]);
        assert_eq!(4, non_shared);
        assert_eq!(1, n2);
        let (value_len, n3) = VarintU32::common_read(&builder.buffer[(n1 + n2) as usize..]);
        assert_eq!(4, value_len);
        assert_eq!(1, n3);
        let key_len = shared + non_shared;
        let read = (n1 + n2 + n3) as usize;
        let key = &builder.buffer[read..read + non_shared as usize];
        assert_eq!(key, b"1111");
        let val_offset = read + key_len as usize;
        let val = &builder.buffer[val_offset..val_offset + value_len as usize];
        assert_eq!(val, b"val1");
        assert_eq!(&builder.last_key, b"1111");

        // Shared key
        let current = val_offset + value_len as usize;
        builder.add(b"11122", b"val2");
        let (shared, n1) = VarintU32::common_read(&builder.buffer[current..]);
        assert_eq!(shared, 3);
        let (non_shared, n2) = VarintU32::common_read(&builder.buffer[current + n1 as usize..]);
        assert_eq!(non_shared, 2);
        let (value_len, n3) =
            VarintU32::common_read(&builder.buffer[current + (n1 + n2) as usize..]);
        assert_eq!(value_len, 4);
        let key_offset = current + (n1 + n2 + n3) as usize;
        let key = &builder.buffer[key_offset..key_offset + non_shared as usize];
        assert_eq!(key, b"22"); // compressed
        let val_offset = key_offset + non_shared as usize;
        let val = &builder.buffer[val_offset..val_offset + value_len as usize];
        assert_eq!(val, b"val2");
        assert_eq!(&builder.last_key, b"11122");

        // Again shared key
        let current = val_offset + value_len as usize;
        builder.add(b"111222", b"val33");
        let (shared, n1) = VarintU32::common_read(&builder.buffer[current..]);
        assert_eq!(shared, 5);
        let (non_shared, n2) = VarintU32::common_read(&builder.buffer[current + n1 as usize..]);
        assert_eq!(non_shared, 1);
        let (value_len, n3) =
            VarintU32::common_read(&builder.buffer[current + (n1 + n2) as usize..]);
        assert_eq!(value_len, 5);
        let key_offset = current + (n1 + n2 + n3) as usize;
        let key = &builder.buffer[key_offset..key_offset + non_shared as usize];
        assert_eq!(key, b"2"); // compressed
        let val_offset = key_offset + non_shared as usize;
        let val = &builder.buffer[val_offset..val_offset + value_len as usize];
        assert_eq!(val, b"val33");
        assert_eq!(&builder.last_key, b"111222");
    }

    #[test]
    fn test_write_restarts() {
        let samples = vec!["1", "12", "123", "abc", "abd", "acd", "bbb"];
        let tests = vec![
            (1, vec![0, 4, 9, 15, 21, 27, 33], 39),
            (2, vec![0, 8, 20, 31], 37),
            (3, vec![0, 12, 27], 33),
        ];
        for (restarts_interval, expected, buffer_size) in tests {
            let opt = Options {
                table_size: 0,
                block_size: 0,
                write_buffer_size: 0,
                block_restart_interval: restarts_interval,
            };
            let mut builder = BlockBuilder::new(opt, BytewiseComparator::default());
            for key in samples.clone() {
                builder.add(key.as_bytes(), b"");
            }
            assert_eq!(builder.buffer.len(), buffer_size);
            assert_eq!(builder.restarts, expected);
        }
    }

    #[test]
    fn test_block_iter() {
        let ucmp = BytewiseComparator::default();
        // keys ["1", "12", "123", "abc", "abd", "acd", "bbb"]
        let data = new_test_block();
        let restarts_len = Block::restarts_len(&data);
        let block = Block::new(data).unwrap();
        let mut iter =
            BlockIterator::new(ucmp, block.data.clone(), block.restart_offset, restarts_len);
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_eq!(iter.current, 0);
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.next();
        assert_eq!(iter.current, 5); // shared 1 + non_shared 1 + value_len 1 + key 1 + value + 1
        assert_eq!(iter.key_offset, 8);
        assert_eq!(iter.key(), "12".as_bytes());
        assert_eq!(iter.value(), "12".as_bytes());
        iter.prev();
        assert_eq!(iter.current, 0);
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.seek_to_last();
        assert_eq!(iter.key(), "bbb".as_bytes());
        assert_eq!(iter.value(), "bbb".as_bytes());
        // Seek
        iter.seek("1".as_bytes());
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.seek("".as_bytes());
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.seek("abd".as_bytes());
        assert_eq!(iter.key(), "abd".as_bytes());
        assert_eq!(iter.value(), "abd".as_bytes());
        iter.seek("bbb".as_bytes());
        assert_eq!(iter.key(), "bbb".as_bytes());
        assert_eq!(iter.value(), "bbb".as_bytes());
        iter.seek("zzzzzzzzzzzzzzz".as_bytes());
        assert!(!iter.valid());
    }

    #[test]
    fn test_read_write() {
        let ucmp = BytewiseComparator::default();
        let opt = Options {
            table_size: 0,
            block_size: 0,
            write_buffer_size: 0,
            block_restart_interval: 2,
        };
        let mut builder = BlockBuilder::new(opt, ucmp);
        let tests = vec![
            ("", "empty"),
            ("1111", "val1"),
            ("1112", "val2"),
            ("1113", "val3"),
            ("abc", "1"),
            ("acd", "2"),
        ];
        for (key, val) in tests.clone() {
            builder.add(key.as_bytes(), val.as_bytes());
        }
        let data = builder.finish();
        let block = Block::new(Vec::from(data)).unwrap();
        let mut iter = block.iter(ucmp);
        assert!(!iter.valid());
        iter.seek_to_first();
        for (key, val) in tests {
            assert!(iter.valid());
            assert_eq!(iter.key(), key.as_bytes());
            assert_eq!(iter.value(), val.as_bytes());
            iter.next();
        }
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_big_entry_block() {
        let c = BytewiseComparator::default();
        let entries = vec![
            ("a", "a".repeat(10000)),
            ("b", "b".repeat(100000)),
            ("c", "c".repeat(1000000)),
        ];
        let mut blocks = vec![];
        for (k, v) in entries.clone() {
            let opt = Options {
                table_size: 0,
                block_size: 0,
                write_buffer_size: 0,
                block_restart_interval: 2,
            };
            let mut builder = BlockBuilder::new(opt, c);
            builder.add(k.as_bytes(), v.as_bytes());
            let data = builder.finish();
            blocks.push(Block::new(data.to_vec()).unwrap());
        }
        for (b, (k, v)) in blocks.into_iter().zip(entries) {
            let mut iter = b.iter(c);
            assert!(!iter.valid());
            iter.seek_to_first();
            assert_eq!(k.as_bytes(), iter.key());
            assert_eq!(v.as_bytes(), iter.value());
            iter.next();
            assert!(!iter.valid());
        }
    }
}
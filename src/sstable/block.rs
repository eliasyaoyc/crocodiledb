use std::cmp::{min, Ordering};
use std::detect::__is_feature_detected::sha;
use std::mem::size_of;
use std::sync::Arc;
use crate::IResult;
use crate::iterator::Iter;
use crate::opt::Options;
use crate::util::coding::{put_fixed_32, VarintU32};
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
///      restarts: vec[num_restarts]
///      num_restarts: u32
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
    data: Vec<u8>,
    size: usize,
    // Offset in data of restart array.
    restart_offset: u32,
    owned: bool,
}


struct BlockIterator {}

impl Iter for BlockIterator {
    fn valid(&self) -> bool {
        todo!()
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn status(&mut self) -> IResult<()> {
        todo!()
    }
}
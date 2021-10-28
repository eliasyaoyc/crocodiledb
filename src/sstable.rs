use std::cmp::Ordering;
use std::sync::Arc;
use snap::raw::max_compress_len;
use crate::cache::Cache;
use crate::error::Error;
use crate::IResult;
use crate::iterator::Iter;
use crate::opt::{CompressionType, Options, ReadOptions};
use crate::sstable::block::{Block, BlockBuilder, BlockIterator};
use crate::sstable::filter_block::{FilterBlockBuilder, FilterBlockReader};
use crate::sstable::format::{BlockHandle, Footer, K_BLOCK_TRAILER_SIZE, K_ENCODED_LENGTH, read_block};
use crate::storage::File;
use crate::util::coding::{decode_fixed_32, put_fixed_32};
use crate::util::comparator::Comparator;
use crate::util::crc32::{extend, hash, mask, unmask};

mod filter_block;
mod block;
pub mod format;

/// `SSTable` is stored in dist and divide to two parts that data-block(block.rs) and meta.
///
/// ```text
///                                                          + optional
///                                                         /
///     +--------------+--------------+--------------+------+-------+-----------------+-------------+--------+
///     | data block 1 |      ...     | data block n | filter block | metaindex block | index block | footer |
///     +--------------+--------------+--------------+--------------+-----------------+-------------+--------+
///
///     Each block followed by a 5-bytes trailer contains compression type and checksum.
///
/// ```
///
/// ## Common Table block trailer:
///
/// ```text
///
///     +---------------------------+-------------------+
///     | compression type (1-byte) | checksum (4-byte) |
///     +---------------------------+-------------------+
///
///     The checksum is a CRC-32 computed using Castagnoli's polynomial. Compression
///     type also included in the checksum.
///
/// ```
///
/// ## Table footer:
///
/// ```text
///
///       +------------------- 40-bytes -------------------+
///      /                                                  \
///     +------------------------+--------------------+------+-----------------+
///     | metaindex block handle / index block handle / ---- | magic (8-bytes) |
///     +------------------------+--------------------+------+-----------------+
///
///     The magic are first 64-bit of SHA-1 sum of "http://code.google.com/p/leveldb/".
///
/// ```
///
/// NOTE: All fixed-length integer are little-endian.
/// # Block
///
/// Block is consist of one or more key/value entries and a block trailer.
/// Block entry shares key prefix with its preceding key until a restart
/// point reached. A block should contains at least one restart point.
/// First restart point are always zero.
///
/// Block data structure:
///
/// ```text
///       + restart point                 + restart point (depends on restart interval)
///      /                               /
///     +---------------+---------------+---------------+---------------+------------------+----------------+
///     | block entry 1 | block entry 2 |      ...      | block entry n | restarts trailer | common trailer |
///     +---------------+---------------+---------------+---------------+------------------+----------------+
///
/// ```
/// Key/value entry:
///
/// ```text
///               +---- key len ----+
///              /                   \
///     +-------+---------+-----------+---------+--------------------+--------------+----------------+
///     | shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
///     +-----------------+---------------------+--------------------+--------------+----------------+
///
///     Block entry shares key prefix with its preceding key:
///     Conditions:
///         restart_interval=2
///         entry one  : key=deck,value=v1
///         entry two  : key=dock,value=v2
///         entry three: key=duck,value=v3
///     The entries will be encoded as follow:
///
///       + restart point (offset=0)                                                 + restart point (offset=16)
///      /                                                                          /
///     +-----+-----+-----+----------+--------+-----+-----+-----+---------+--------+-----+-----+-----+----------+--------+
///     |  0  |  4  |  2  |  "deck"  |  "v1"  |  1  |  3  |  2  |  "ock"  |  "v2"  |  0  |  4  |  2  |  "duck"  |  "v3"  |
///     +-----+-----+-----+----------+--------+-----+-----+-----+---------+--------+-----+-----+-----+----------+--------+
///      \                                   / \                                  / \                                   /
///       +----------- entry one -----------+   +----------- entry two ----------+   +---------- entry three ----------+
///
///     The block trailer will contains two restart points:
///
///     +------------+-----------+--------+
///     |     0      |    16     |   2    |
///     +------------+-----------+---+----+
///      \                      /     \
///       +-- restart points --+       + restart points length
///
/// ```
///
/// # Block restarts trailer
///
/// ```text
///
///       +-- 4-bytes --+
///      /               \
///     +-----------------+-----------------+-----------------+------------------------------+
///     | restart point 1 |       ....      | restart point n | restart points len (4-bytes) |
///     +-----------------+-----------------+-----------------+------------------------------+
///
/// ```
///
/// NOTE: All fixed-length integer are little-endian.
///
/// # Filter block
///
/// Filter block consist of one or more filter data and a filter block trailer.
/// The trailer contains filter data offsets, a trailer offset and a 1-byte base Lg.
///
/// Filter block data structure:
///
/// ```text
///
///       + offset 1      + offset 2      + offset n      + trailer offset
///      /               /               /               /
///     +---------------+---------------+---------------+---------+
///     | filter data 1 |      ...      | filter data n | trailer |
///     +---------------+---------------+---------------+---------+
///
/// ```
///
/// Filter block trailer:
///
/// ```text
///
///       +- 4-bytes -+
///      /             \
///     +---------------+---------------+---------------+-------------------------------+------------------+
///     | data 1 offset |      ....     | data n offset | data-offsets length (4-bytes) | base Lg (1-byte) |
///     +---------------+---------------+---------------+-------------------------------+------------------+
///
/// ```
///
/// NOTE: The filter block is not compressed
///
/// # Index block
///
/// Index block consist of one or more block handle data and a common block trailer.
/// The 'separator key' is the key just bigger than the last key in the data block which the 'block handle' pointed to
///
/// ```text
///
///     +---------------+--------------+
///     |      key      |    value     |
///     +---------------+--------------+
///     | separator key | block handle |---- a block handle points a data block starting offset and the its size
///     | ...           | ...          |
///     +---------------+--------------+
///
/// ```
///
/// NOTE: All fixed-length integer are little-endian.
///
/// # Meta block
///
/// This meta block contains a bunch of stats. The key is the name of the statistic. The value contains the statistic.
/// For the current implementation, the meta block only contains the filter meta data:
///
/// ```text
///
///     +-------------+---------------------+
///     |     key     |        value        |
///     +-------------+---------------------+
///     | filter name | filter block handle |
///     +-------------+---------------------+
///
/// ```
///
/// NOTE: All fixed-length integer are little-endian.

pub struct TableBuilder<F: File, C: Comparator> {
    c: C,
    options: Options,
    // Underlying sstable file.
    file: F,
    offset: u64,
    data_block: BlockBuilder<C>,
    // Similar fence-point.
    index_block: BlockBuilder<C>,
    // The last key in data-block.
    last_key: Vec<u8>,
    // The number of key/value pair in the file.
    num_entries: usize,
    closed: bool,
    filter_block: Option<FilterBlockBuilder>,
    // Indicates whether we have to add a index to index_block
    //
    // Iff true We do not emit the index entry for a block until we have seen
    // the first key for the next data block. This allows us to use shorter
    // keys in the index block.
    pending_index_entry: bool,
    pending_handle: BlockHandle,
}

impl<F: File, C: Comparator> TableBuilder<F, C> {
    pub fn new(file: F, c: C, options: Options) -> Self {
        Self {
            c: C,
            options,
            file,
            offset: 0,
            data_block: BlockBuilder::new(options, c.clone()),
            index_block: BlockBuilder::new(options, c.clone()),
            last_key: vec![],
            num_entries: 0,
            closed: false,
            filter_block: None,
            pending_index_entry: false,
            pending_handle: BlockHandle::new(0, 0),
        }
    }

    /// Adds a key/value pair to the table being constructed.
    /// If the data block reaches the limit ,it will be flushed
    /// If we just have flushed a new block before, add an index entry into the index block
    ///
    /// # Panics
    ///
    /// * If key is after any previously added key according to comparator.
    /// * TableBuilder is closed.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> IResult<()> {
        self.assert_not_closed();
        if self.num_entries > 0 {
            assert_eq!(
                self.c.compare(key, self.last_key.as_slice()),
                Ordering::Greater,
                "[TableBuilder] new key is inconsistent with the last key in sstable.");
        }

        // Check whether we need to create a new index entry.
        self.maybe_append_index_block(Some(key));
        // Update filter block.
        if let Some(fb) = self.filter_block.as_mut() {
            fb.add_key(key);
        }
        self.last_key.resize(key.len(), 0);
        self.last_key.copy_from_slice(key);
        self.num_entries += 1;
        self.data_block.add(key, value);

        if self.data_block.current_size_estimate() >= self.options.block_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flushes any buffered key/value pairs to file.
    /// Can be used to ensure that two adjacent entries never live in
    /// the same data block. Most clients should not need to use this method.
    ///
    /// # Panics
    ///
    /// * The table builder is closed.
    ///
    pub fn flush(&mut self) -> IResult<()> {
        self.assert_not_closed();
        if !self.data_block.empty() {
            // Make sure the pending_index_entry is false then `Add` operation is completion.
            assert!(!self.pending_index_entry, "[TableBuilder] the index for the previous data block should never remain when flushing current block data.")
            let data_block = self.data_block.finish();

            self.write_block(data_block, &mut self.pending_handle)?;

            self.data_block.reset();
            self.pending_index_entry = true;
            self.file.flush()?;
            if let Some(fb) = &mut self.filter_block {
                fb.start_block(self.offset)
            }
        }
        Ok(())
    }

    /// Finishes building the table and close the relative file.
    /// If `sync` is true, the `File::flush` will be called.
    ///
    /// # Panics
    ///
    /// * The table builder is closed.
    ///
    pub fn finish(&mut self, sync: bool) -> IResult<()> {
        // Write the last data block.
        self.flush()?;
        self.assert_not_closed();
        self.closed = true;
        // Write filter block.
        let mut filter_block_handler = BlockHandle::new(0, 0);
        let mut has_filter_block = false;
        if let Some(fb) = &mut self.filter_block {
            let data = fb.finish();
            write_raw_block(
                &mut self.file,
                data,
                CompressionType::KNoCompression,
                &mut filter_block_handler,
                &mut self.offset,
            )?;
            has_filter_block = true;
        }

        // Write metaindex block
        let mut meta_block_handle = BlockHandle::new(0, 0);
        let mut meta_block_builder = BlockBuilder::new(self.options, self.c.clone());
        let meta_block = {
            if has_filter_block {
                let filter_key = if let Some(fp) = &self.options.filter_policy {
                    "filter".to_owned() + fp.name()
                } else {
                    String::from("")
                };
                meta_block_builder.add(filter_key.as_bytes(), &filter_block_handler.encode());
            }
            meta_block_builder.finish()
        };
        self.write_block(meta_block, &mut meta_block_handle)?;

        // Write index block.
        self.maybe_append_index_block(None); // flush the last index first.
        let index_block = self.index_block.finish();
        let mut index_block_handle = BlockHandle::new(0, 0);
        self.write_block(index_block, &mut index_block_handle)?;
        self.index_block.reset();

        // Write footer.
        let footer = Footer::new(meta_block_handle, index_block_handle).encode();
        self.file.write(footer.as_slice())?;
        self.offset += footer.len() as u64;
        if sync {
            self.file.flush()?;
            self.file.close()?;
        }
        Ok(())
    }

    /// Mark this builder as closed.
    #[inline]
    pub fn close(&mut self) {
        assert!(!self.closed, "[TableBuilder] try to close a closed TableBuilder.");
        self.closed = true;
        let _ = self.file.close();
    }

    /// Returns the number of key/value added so far.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Returns size of the file generated so far. If invoked after a successful
    /// `Finish` call, returns the size of the final generated file.
    #[inline]
    pub fn file_size(&self) -> u64 {
        self.offset
    }

    #[inline]
    fn assert_not_closed(&self) {
        assert!(
            !self.closed,
            "[TableBuilder] try to handle a closed TableBuilder."
        );
    }

    /// Add a key into the index block if necessary.
    fn maybe_append_index_block(&mut self, key: Option<&[u8]>) -> bool {
        if self.pending_index_entry {
            // We've finished a data block to the file so adding an relate index entry into index block
            assert!(self.data_block.empty(), "[TableBuilder] the data block buffer is not empty after flushed, something is wrong.");
            let s = if let Some(k) = key {
                self.c.find_shortest_separator(&self.last_key, k)
            } else {
                self.c.find_short_successor(&self.last_key)
            };
            let mut handle_encoding = vec![];
            self.pending_handle.encode_to(&mut handle_encoding);
            self.index_block.add(&s, &handle_encoding);
            self.pending_index_entry = false;
            return true;
        }
        false
    }

    fn write_block(&mut self, raw_block: &[u8], handle: &mut BlockHandle) -> IResult<()> {
        let (data, compression) = compress_block(raw_block, self.options.compression)?;
        write_raw_block(&mut self.file, &data, compression, handle, &mut self.offset)?;
        Ok(())
    }
}

/// Compress the given raw block by configured compression algorithm.
/// Returns the compressed data and compression data.
fn compress_block(
    raw_block: &[u8],
    compression: CompressionType,
) -> IResult<(Vec<u8>, CompressionType)>
{
    match compression {
        CompressionType::KSnappyCompression => {
            let mut enc = snap::raw::Encoder::new();
            let mut buffer = vec![0; max_compress_len(raw_block.len())];
            match enc.compress(raw_block, buffer.as_mut_slice()) {
                Ok(size) => buffer.truncate(size),
                Err(e) => return Err(Error::CompressedFailed(e)),
            }
            Ok((buffer, CompressionType::KSnappyCompression))
        }
        CompressionType::KNoCompression | CompressionType::UnKnown => {
            Ok((Vec::from(raw_block), CompressionType::KNoCompression))
        }
    }
}

/// Write given block data into the file with block trailer
fn write_raw_block<F: File>(
    file: &mut F,
    data: &[u8],
    compression: CompressionType,
    handle: &mut BlockHandle,
    offset: &mut u64,
) -> IResult<()>
{
    // Write block data.
    file.write(data)?;
    // Update the block handle.
    handle.set_offset(*offset);
    handle.set_size(data.len() as u64);
    // Write trailer.
    let mut trailer = vec![compression as u8];
    let crc = mask(extend(hash(data), &[compression as u8]));
    put_fixed_32(&mut trailer, crc);
    assert_eq!(trailer.len(), K_BLOCK_TRAILER_SIZE);
    file.write(trailer.as_slice())?;
    // Update offset.
    *offset += (data.len() + K_BLOCK_TRAILER_SIZE) as u64;
    Ok(())
}

/// A `Table` is a sorted map from strings to strings, which must be immutable and persistent.
/// A `Table` may be safely accessed from multiple threads without external synchronization.
pub struct Table<F: File> {
    file: F,
    file_number: u64,
    filter_reader: Option<FilterBlockReader>,
    meta_block_handle: Option<BlockHandle>,
    index_block: Block,
    block_cache: Option<Arc<dyn Cache<Vec<u8>, Arc<Block>>>>,
}

impl<F: File> Table<F> {
    /// Attempt to open the table that is stored in bytes `[0..size)`
    /// of `file`, and read the metadata entries necessary to allow
    /// retrieving data from the table.
    ///
    /// NOTE: `UC` for user comparator and `TC` for table comparator.
    pub fn open<UC: Comparator, TC: Comparator>(
        file: F,
        file_number: u64,
        file_len: u64,
        options: Options,
        c: TC,
    ) -> IResult<Self>
    {
        if file_len < K_ENCODED_LENGTH as u64 {
            return Err(Error::Corruption("file is too short to be sstable"));
        }
        // Read footer.
        let mut footer_space = vec![0; K_ENCODED_LENGTH as usize];
        file.read_exact_at(footer_space.as_mut_slice(), file_len - K_ENCODED_LENGTH)?;
        let footer = Footer::decode_from(footer_space.as_slice())?;

        // Read the index block.
        let index_block_contents = read_block(&file, options.paranoid_checks, &footer.index_handle)?;
        let index_block = Block::new(index_block_contents)?;
        let mut t = Self {
            file,
            file_number,
            filter_reader: None,
            meta_block_handle: None,
            index_block,
            block_cache: None,
        };

        // Read MetaIndex block.
        if footer.metaindex_handle.size() > 0 && options.filter_policy.is_some() {
            // ignore the reading errors since meta info is not needed for operation.
            if let Ok(meta_block_contents) = read_block(&t.file, options.paranoid_checks, &footer.metaindex_handle) {
                if let Ok(meta_block) = Block::new(meta_block_contents) {
                    t.meta_block_handle = Some(footer.metaindex_handle);
                    let mut iter = meta_block.iter(c);
                    let filter_key = if let Some(fp) = &options.filter_policy {
                        "filter".to_owned() + fp.name()
                    } else {
                        String::from("")
                    };

                    // Read filter block.
                    iter.seek(filter_key.as_bytes());
                    if iter.valid() && iter.key() == filter_key.as_bytes() {
                        if let Ok((filter_handle, _)) = BlockHandle::decode_from(iter.value()) {
                            if let Ok(filter_block) = read_block(&t.file, options.paranoid_checks, &filter_handle) {
                                t.filter_reader = Some(FilterBlockReader::new(
                                    options.filter_policy.clone().unwrap(),
                                    filter_block,
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(t)
    }

    /// Convert an `BlockHandle` into an iterator over the contents of the corresponding block.
    fn block_reader<CC: Comparator>(
        &self,
        c: CC,
        data_block_handle: BlockHandle,
        options: ReadOptions,
    ) -> IResult<BlockIterator<CC>> {}

    /// Finds the first entry with the key equal or greater than target and
    /// returns the block iterator directly.
    ///
    /// The given `key` is an internal key so the `c` must be a `InternalKeyComparator`.
    pub fn internal_get<TC: Comparator>(
        &self,
        options: ReadOptions,
        c: TC,
        key: &[u8],
    ) -> IResult<Option<BlockIterator<TC>>>
    {
        Ok(None)
    }

    /// Given a key, return an approximate byte offset in the file where
    /// the data for that key begins (or would begin if the key were
    /// present in the file).  The returned value is in terms of file
    /// bytes, and so includes effects like compression of the underlying data.
    /// E.g., the approximate offset of the last key in the table will
    /// be close to the file length.
    pub fn approximate_offset_of<TC: Comparator>(&self, c: TC, key: &[u8]) -> u64 {
        0
    }
}
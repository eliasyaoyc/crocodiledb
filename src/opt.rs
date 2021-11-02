use std::sync::Arc;
use crate::cache::Cache;
use crate::filter::FilterPolicy;
use crate::snapshot::Snapshot;
use crate::sstable::block::Block;
use crate::storage::File;
use crate::util::comparator::{BytewiseComparator, Comparator};

/// DB contents are stored in a set of blocks, each of which holds a
/// sequence of key,value pairs. Each block may be compressed before
/// being stored in a file. The following enum describes which
/// compression method(if any) is used to compress a block.
#[derive(Clone, Copy, Debug, FromPrimitive)]
pub enum CompressionType {
    KNoCompression = 0x0,
    KSnappyCompression = 0x1,
    UnKnown,
}

impl From<u8> for CompressionType {
    fn from(t: u8) -> Self {
        num_traits::FromPrimitive::from_u8(t).unwrap()
    }
}

#[derive(Clone)]
pub struct Options<C: Comparator> {
    pub comparator: C,
    /// If true, the database will be created if it is missing.
    pub create_if_missing: bool,

    /// If true, an error is raised if the database already exists.
    pub error_if_exists: bool,

    /// If true, the implementation will do aggressive checking of the
    /// data it is processing and will stop early if it detects any
    /// errors.  This may have unforeseen ramifications: for example, a
    /// corruption of one DB entry may cause a large number of entries to
    /// become unreadable or for the entire DB to become unopenable.
    pub paranoid_checks: bool,

    // -------------------
    // Parameters that affect compaction:
    /// The max number of levels except L0
    pub max_levels: usize,

    /// The number of files necessary to trigger an L0 compaction.
    pub l0_compaction_threshold: usize,

    /// Soft limit on the number of L0 files. Writes are slowed down when this
    /// threshold is reached.
    pub l0_slowdown_writes_threshold: usize,

    /// Hard limit on the number of L0 files. Writes are stopped when this
    /// threshold is reached.
    pub l0_stop_writes_threshold: usize,

    /// The maximum number of bytes for L1. The maximum number of bytes for other
    /// levels is computed dynamically based on this value. When the maximum
    /// number of bytes for a level is exceeded, compaction is requested.
    pub l1_max_bytes: u64,

    /// Maximum level to which a new compacted memtable is pushed if it
    /// does not create overlap.  We try to push to level 2 to avoid the
    /// relatively expensive level 0=>1 compactions and to avoid some
    /// expensive manifest file operations.  We do not push all the way to
    /// the largest level since that can generate a lot of wasted disk
    /// space if the same key space is being repeatedly overwritten.
    pub max_mem_compact_level: usize,

    /// Approximate gap in bytes between samples of data read during iteration
    pub read_bytes_period: u64,

    // -------------------
    // Parameters that affect performance:
    /// Amount of data to build up in memory (backed by an unsorted log
    /// on disk) before converting to a sorted on-disk file.
    ///
    /// Larger values increase performance, especially during bulk loads.
    /// Up to two write buffers may be held in memory at the same time,
    /// so you may wish to adjust this parameter to control memory usage.
    /// Also, a larger write buffer will result in a longer recovery time
    /// the next time the database is opened.
    pub write_buffer_size: usize,

    /// Number of open files that can be used by the DB.  You may need to
    /// increase this if your database has a large working set (budget
    /// one open file per 2MB of working set).
    pub max_open_files: usize,

    // -------------------
    // Control over blocks (user data is stored in a set of blocks, and
    // a block is the unit of reading from disk).
    /// If non-null, use the specified cache for blocks.
    /// If null, we will automatically create and use an 8MB internal cache.
    pub block_cache: Option<Arc<dyn Cache<Vec<u8>, Arc<Block>>>>,

    /// Number of sstables that remains out of table cache
    pub non_table_cache_files: usize,

    /// size of each block in bytes in SST.
    pub block_size: usize,

    /// Number of keys between restart points for delta encoding of keys.
    /// This parameter can be changed dynamically. Most clients should
    /// leave this parameter alone.l
    pub block_restart_interval: u32,

    pub max_file_size: usize,

    pub compression: CompressionType,

    pub reuse_logs: bool,

    pub filter_policy: Option<Arc<dyn FilterPolicy>>,
}

impl<C: Comparator> Options<C> {
    /// Maximum number of bytes in all compacted files.  We avoid expanding
    /// the lower level file set of a compaction if it would make the
    /// total compaction cover more than this many bytes.
    pub fn expanded_compaction_byte_size_limit(&self) -> u64 {
        (25 * self.max_file_size) as u64
    }

    /// Maximum bytes of overlaps in grandparents (i.e., level + 2) before we
    /// stop building a single file in a level -> level + 1 compaction.
    pub fn max_grandparent_overlap_bytes(&self) -> u64 {
        (10 * self.max_file_size) as u64
    }

    /// Maximum bytes of total files in a given level
    pub fn max_bytes_for_level(&self, mut level: usize) -> u64 {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1.
        let mut result = self.l1_max_bytes;
        while level > 1 {
            result *= 10;
            level -= 1;
        }
        result
    }
}

impl<C: Comparator> Default for Options<C> {
    fn default() -> Self {
        Options {
            comparator: C::default(),
            create_if_missing: false,
            error_if_exists: false,
            paranoid_checks: false,
            max_levels: 7,
            l0_compaction_threshold: 4,
            l0_slowdown_writes_threshold: 8,
            l0_stop_writes_threshold: 12,
            l1_max_bytes: 64 * 1024 * 1024,
            max_mem_compact_level: 2,
            block_size: 4 * 1024,
            write_buffer_size: 4 * 1024 * 1024,
            max_open_files: 0,
            block_cache: None,
            block_restart_interval: 16,
            max_file_size: 2 * 1024 * 1024,
            compression: CompressionType::KSnappyCompression,
            reuse_logs: false,
            filter_policy: None,
            read_bytes_period: 0,
            non_table_cache_files: 0,
        }
    }
}

/// Options that control read operations.
#[derive(Clone, Copy)]
pub struct ReadOptions {
    // If true, all data read from underlying storage will be
    // verity against corresponding checksums.
    pub verify_checksums: bool,

    // Should the data read for this iteration be cached in memory?
    // Callers may wish to set this field to false for bulk scans.
    pub fill_cache: bool,

    // If `snapshot` is non-null, read as of the supplied snapshot
    // (which must belong to the DB that is being read and which must
    // not have been released). if `snapshot` is null, use an implicit
    // snapshot of the stat at the beginning of this read operation.
    pub snapshot: Option<Snapshot>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            verify_checksums: false,
            fill_cache: false,
            snapshot: None,
        }
    }
}
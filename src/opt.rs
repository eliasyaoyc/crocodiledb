use crate::snapshot::Snapshot;
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

#[derive(Debug, Clone)]
pub struct Options {
    /// size of each block inside SST.
    pub table_size: usize,
    /// size of each block in bytes in SST.
    pub block_size: usize,
    /// size of memtable, if arrived it convert to immemtable.
    pub write_buffer_size: usize,

    /// Number of keys between restart points for delta encoding of keys.
    /// This parameter can be changed dynamically. Most clients should
    /// leave this parameter alone.l
    pub block_restart_interval: u32,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            table_size: 0,
            block_size: 4 * 1024,
            write_buffer_size: 4 * 1024 * 1024,
            block_restart_interval: 16,
        }
    }
}

/// Options that control read operations.
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
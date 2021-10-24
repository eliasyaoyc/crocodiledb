use crate::util::comparator::{BytewiseComparator, Comparator};

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
#[derive(Debug, Clone)]
pub struct Options {
    /// size of each block inside SST.
    pub table_size: usize,
    /// size of each block in bytes in SST.
    pub block_size: usize,
    /// size of memtable, if arrived it convert to immemtable.
    pub write_buffer_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            table_size: 0,
            block_size: 4 * 1024,
            write_buffer_size: 4 * 1024 * 1024,
        }
    }
}
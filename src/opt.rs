#[derive(Debug, Clone)]
pub struct Options {
    /// size of each block inside SST.
    pub table_size: usize,
    /// size of each block in bytes in SST.
    pub block_size: usize,
    /// size of memtable.
    pub mem_table_size: usize,
}
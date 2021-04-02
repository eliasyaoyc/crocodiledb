mod builder;
mod concat_iterator;
mod iterator;
mod merge_iterator;

use bytes::{Buf, Bytes};

use memmap::{Mmap, MmapOptions};
use std::sync::Arc;
use std::path::PathBuf;
use crate::storage::engine::slsm::fence::FencePointer;
use crate::storage::engine::slsm::bloom::Bloom;

/// MmapFile stores SST data. `File` refers to a file on disk.
/// and `Memory` refers to data in memory.
enum MmapFile {
    File {
        name: PathBuf,
        file: std::fs::File,
        mmap: Mmap,
    },
    Memory {
        data: Bytes,
    },
}
impl MmapFile {
    /// Returns if data is in memory.
    pub fn is_in_memory(&self) -> bool {
        match self {
            MmapFile::File {..} => false,
            MmapFile::Memory {..} => true,
        }
    }
}

/// TableInner stores data of an SST.
/// It is immutable once created and initialized.
pub struct TableInner {
    /// file struct of SST.
    file: MmapFile,
    /// size of SST.
    table_size: usize,
    /// fence-pointer of SST.
    fence_pointer: FencePointer,

}

/// Table is empty an Arc to its internal TableInner structure.
#[derive(Clone)]
pub struct Table {
    inner: Arc<TableInner>,
}

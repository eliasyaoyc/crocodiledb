use crate::storage::{
    config::StorageConfig,
    engine::slsm::fence::FencePointer,
    error::{Error, Result},
    Range, Scan,
};
use bytes::{Buf, Bytes};
use futures::future::err;
use memmap::{Mmap, MmapOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    #[inline]
    pub fn is_in_memory(&self) -> bool {
        match self {
            MmapFile::File { .. } => false,
            MmapFile::Memory { .. } => true,
        }
    }

    pub fn open(path: &Path, file: std::fs::File) -> Result<Self> {
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        Ok(MmapFile::File {
            name: path.to_path_buf(), // full-path
            file,
            mmap,
        })
    }
}

/// TableInner stores data of an SST.
/// It is immutable once created and initialized.
pub struct TableInner {
    id: u64,
    /// file struct of SST.
    file: MmapFile,
    /// size of SST.
    table_size: usize,
    /// fence-pointer of SST.
    fence_pointer: FencePointer,
    checksum: Bytes,
    /// only used on encryption or compression
    estimated_size: usize,
    /// index of SST.
    index: TableIndex,
    conf: StorageConfig,
}

/// Table is empty an Arc to its internal TableInner structure.
#[derive(Clone)]
pub struct Table {
    inner: Arc<TableInner>,
}

impl TableInner {
    pub fn create(path: &Path, data: Bytes, conf: StorageConfig) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.write_all(&data).unwrap();
        // TODO: pass file object directly to open and sync write
        drop(file);
        Self::open(path, conf)
    }

    /// Open an existing SST on disk.
    pub fn open(path: &Path, conf: StorageConfig) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)
            .unwrap();
        let file_name = path.file_name().unwrap().to_str().unwrap();
        let id = parse_file_id(file_name).unwrap();
        let meta = file.metadata().unwrap();
        let table_siz = meta.len();
        let mut inner = TableInner {
            id,
            file: MmapFile::open(path, file).unwrap(),
            table_size: table_siz as usize,
            fence_pointer: FencePointer::new(),
            checksum: Default::default(),
            estimated_size: 0,
            index: TableIndex::new(),
            conf,
        };
        inner.init_fence_pointer();

        Ok(inner)
    }

    fn init_fence_pointer(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Table {
    /// Create an SST from bytes data generated with table builder.
    pub fn create(path: &Path, data: Bytes, conf: StorageConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(TableInner::create(path, data, conf)?),
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }

    fn scan(&self, range: Range) -> Scan {
        todo!()
    }
}

/// Block contains several entries. It can be obtained from an SST.
#[derive(Debug, Clone)]
pub struct Block {
    offset: usize,
    data: Bytes,
    entries_index_start: usize,
    entry_offset: Vec<u32>,
    checksum: Bytes,
    checksum_len: usize,
}

impl Block {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableIndex {}

impl TableIndex {
    pub fn new() -> Self {
        Self {}
    }
}

fn parse_file_id(file_name: &str) -> Result<u64> {
    if !file_name.ends_with(".sst") {
        return Err(Error::InvalidFilename(file_name.to_string()));
    }
    match file_name[..file_name.len() - 4].parse() {
        Ok(id) => Ok(id),
        Err(_) => Err(Error::InvalidFilename(file_name.to_string())),
    }
}

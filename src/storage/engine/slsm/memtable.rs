use crate::storage::engine::slsm::skl::{slist::SkipList, FixedLengthSuffixComparator, KeyComparator};
use crate::storage::error::{Error, Result};
use crate::storage::{Range, Scan, Storage};
use crate::storage::config::StorageConfig;
use crate::storage::engine::slsm::fence::FencePointer;
use bytes::Bytes;
use priority_queue::PriorityQueue;

/// MemTable represent the active table so that all modified operations
/// will happened on.
pub struct MemTable<T: KeyComparator> {
    config: StorageConfig,
    runs: PriorityQueue<u64, Run<T>>,
}

struct Run<T> {
    id: u64,
    siz: usize,
    checksum: Bytes,
    fence_pointer: FencePointer,
    // true if there's bloom filter in run.
    has_bloom_filter: bool,
    skl: SkipList<T>,
}

impl MemTable<T> {
    pub(crate) fn create(config: &StorageConfig) -> Result<MemTable<T>> {
        let comp = FixedLengthSuffixComparator::new(8);
        Ok(Self {
            config: config.clone(),
            // skl: SkipList::with_capacity(comp, 1 << 20),
            runs: PriorityQueue::new(),
        })
    }

    /// create_run that create a active run to storage data.
    pub(crate) fn create_run(&self) -> Result<()> {
        if !self.can_create_run() {
            return Err(Error::InternalErr("Create run in memory failed.".to_string()));
        }

        Ok(())
    }

    /// is_run_full that determine whether the current run is full.
    pub(crate) fn is_run_full(&self) -> bool {
        true
    }

    /// is_full that determine whether the whole memtable is full.
    pub(crate) fn is_full(&self) -> bool {
        true
    }

    pub(crate) fn can_create_run(&self) -> bool {
        true
    }

    pub(crate) fn compression(&self) -> Result<()> {
        // todo whether to sync the table to disk, or aysnc?
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub(crate) fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        todo!()
    }

    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<()> {
        todo!()
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        todo!()
    }

    pub(crate) fn scan(&self, range: Range) -> Scan {
        todo!()
    }
}
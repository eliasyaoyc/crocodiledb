use crate::storage::config::StorageConfig;
use crate::storage::engine::slsm::bloom::Bloom;
use crate::storage::engine::slsm::fence::FencePointer;
use crate::storage::engine::slsm::skl::{
    slist::SkipList, FixedLengthSuffixComparator, KeyComparator,
};
use crate::storage::error::{Error, Result};
use crate::storage::util::priority_queue::PriorityQueue;
use crate::storage::{Range, Scan, Storage};
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use std::cmp::Ordering;
use std::thread;
use std::time::Duration;

/// MemTable represent the active table so that all modified operations
/// will happened on.
pub struct MemTable<C: KeyComparator> {
    config: StorageConfig,
    runs: PriorityQueue<usize, Run<C>>,
    used_memory: u64,
}

struct Run<C: KeyComparator> {
    id: u64,
    active: bool,
    siz: usize,
    checksum: Bytes,
    minimum: Bytes,
    maximum: Bytes,
    // true if there's bloom filter in run
    skl: SkipList<C>,
}

impl<C: KeyComparator> PartialEq for Run<C> {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

impl<C: KeyComparator> Eq for Run<C> {}

impl<C: KeyComparator> PartialOrd for Run<C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!()
    }
}

impl<C: KeyComparator> Ord for Run<C> {
    fn cmp(&self, other: &Self) -> Ordering {
        todo!()
    }
}

impl<C: KeyComparator> MemTable<C> {
    pub(crate) fn start(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn create(config: &StorageConfig, recv: Receiver<()>) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            runs: PriorityQueue::new(),
            used_memory: 0,
        })
    }

    /// create_run that create a active run to storage data.
    pub(crate) fn create_run(&self) -> Result<()> {
        if !self.can_create_run() {
            return Err(Error::InternalErr(
                "Create run in memory failed.".to_string(),
            ));
        }

        Ok(())
    }

    /// is_run_full that determine whether the current run is full.
    pub(crate) fn is_run_full(&self) -> bool {
        true
    }

    /// is_full that determine whether the whole memtable is full.
    pub(crate) fn is_full(&self) -> bool {
        self.used_memory >= self.config.max_mem_table_siz
    }

    /// can_create_run true if `mem_table_siz` + the current used space
    /// less than and equal to `max_men_table_siz`, otherwise false.
    pub(crate) fn can_create_run(&self) -> bool {
        if self.used_memory + self.config.mem_table_siz <= self.config.max_mem_table_siz {
            return true;
        }
        false
    }

    pub(crate) fn compression(&self, sender: Sender<()>) {
        // todo whether to sync the table to disk, or aysnc?
        thread::Builder::new()
            .name("manual-tale-compaction".to_owned())
            .spawn(move || {
                let mut done_compaction = false;

                if done_compaction {}
            })
            .unwrap();
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<&Bytes>> {
        Ok(None)
    }

    pub(crate) fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        todo!()
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        todo!()
    }

    pub(crate) fn scan(&self, range: Range) -> Scan {
        todo!()
    }
}

impl<C: KeyComparator> Run<C> {
    fn new(comparor: C) -> Self {
        Self {
            id: 0,
            active: false,
            siz: 0,
            checksum: Default::default(),
            minimum: Default::default(),
            maximum: Default::default(),
            skl: SkipList::with_capacity(comparor, 16),
        }
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&Bytes> {
        self.skl.get(key)
    }
}

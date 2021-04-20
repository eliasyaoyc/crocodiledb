//! Instructions in advance, if you need to written a introduction about your code what you should create a file named `README.md` and written in.
//! Strangely, i written in `mod.rs` but you don't ask me why i do that.
//! LSM the full-name is Log Structure Merge Tree that has good write performance greater than B+Tree, because LSM written in direct order,
//! so what is the basic performance cost for behavior but it's a waste of space serious, so we need to compress regularly and the related code in `compressor.rs`.
//! In this i will not impl write ahead logging(WAL) to ensure the security of the data because at the high-level all requests go through the RAFT protocol that it will
//! written in disk first.
//!
//! The most important thing is i'm not implementation the pure-lsm, i reference a paper named 『The SkipList-Based LSM Tree』that techniques such as cache-aware skiplist、
//! bloom filter、fence pointer、fast k-way merging algorithm and multi-thread. **It has the two important components built-in：**
//! * In-Memory: mainly used fot quick inserts and queries for buffered data，achieved by skiplist.
//! * In-Disk: mainly stores data in a hierarchical way and provides the operation of merge.
//! * In-Memory data structures support queries for cached data and queries for recent data.
//! * The storage on disk is hierarchical and immediate.
//!
//! **The related techniques designs:**
//! * Memory Buffer:
//! > The memory buffer contains of R runs, indexed by r. In the sLSM，one run corresponds to one skiplist. Only one run is active at any one time, denoted by index ra. Each
//! > run can contain up to Rn elements.
//! >
//! > An insert occurs as followers:
//! > 1. If the current run is full, make the current run a new,empty one
//! > 2. If there is no space for a new run:
//! >     * merge a proportion m of the existing runs in the buffer to secondary storage(disk).
//! >     * set the active run to a new,empty one.
//! >     * insert the k-v pair into the active run.
//! >
//! > A lookup occus as followers:
//! > 1. starting from the newest run and moving towards the oldest,search the skiplist for the given key.
//! > 2. return the first one found(as it is the newest). If not found, then search disk storage.
//! * SkipLists:
//! > * Fast Random Levels: p setup 0.5 in practice.
//! > * Vertical Arrays, Horizontal Pointer: traversal optimization. In skiplist, each node contains an array, which mainly stores the pointer to the next node
//! > of the node at each level.This design can save a lot of memory.
//! * Bloom Filter
//! > mainly provides the read performance optimization. if queries key not hit in bloom filter then return.
//! * Fence Pointer:
//! > used for disk-storage, mainly record the maximum and minimum values built in each SST. Since the key ranges of multiple SSTs in each layer do not overlap
//! > each other, it is possible to narrow the search scope for a single key to singe SST.
//! * Merge:
//! > merge a proportion m of the existing runs in the buffer to disk when memory-buffer is full. use the heap to optimize the merge operation,even with a minimum heap for the merge operation.
mod bloom;
mod checksum;
mod compaction;
mod fence;
mod memtable;
pub mod skl;
pub mod table;

use crate::config::StorageConfig;
use crate::slsm::compaction::Compaction;
use crate::slsm::memtable::MemTable;
use crate::slsm::skl::{FixedLengthSuffixComparator, KeyComparator};
use crate::slsm::table::Table;
use crate::{Range, Scan};
use bytes::Bytes;
use crocodiledb_components_error::error::{Error, Result};
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::{Condvar, Mutex};
pub use tracing::{debug, error, info, warn};

/// The slsm outermost layer.
pub struct SLSM<C: KeyComparator> {
    config: StorageConfig,
    memtable: MemTable<C>,
    table: Table,
    // The queue for ManualCompaction that all the compaction will be executed one by one once compaction is triggered.
    manual_compaction_queue: Mutex<VecDeque<Compaction>>,
    // Signal whether the compaction finished.
    background_work_finished_signal: Condvar,
    // Whether we have scheduled and running a compaction.
    background_compaction_scheduled: AtomicBool,
    // Signal of schedule a compaction.
    do_compaction: (Sender<()>, Receiver<()>),
    // Whether the db is closing.
    is_shutting_down: AtomicBool,
    compact: Compaction,
}

impl<C: KeyComparator> Display for SLSM<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "slsm")
    }
}

impl<C: KeyComparator> SLSM<C> {
    /// Init the slsm, notice that it not be start, so if you want start, you can call `start`.
    pub fn create(config: &StorageConfig) -> Self {
        let compact_channel: (Sender<()>, Receiver<()>) = crossbeam_channel::unbounded();
        let compact = Compaction::new(compact_channel.0);
        Self {
            config: Default::default(),
            memtable: MemTable::create(config, compact_channel.1.clone()).unwrap(),
            table: Table::create(config, compact_channel.1).unwrap(),
            manual_compaction_queue: Mutex::new(VecDeque::new()),
            background_work_finished_signal: Condvar::new(),
            background_compaction_scheduled: AtomicBool::new(false),
            do_compaction: crossbeam_channel::unbounded(),
            is_shutting_down: AtomicBool::new(false),
            compact,
        }
    }

    /// start that real-start SLSM engine.
    pub fn start(&self) -> Result<()> {
        self.memtable.start()?;
        self.table.start()?;
        Ok(())
    }

    #[inline]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        if !self.memtable.is_run_full() {
            let res = self.memtable.set(key, value);
            if self.maybe_need_compaction() {
                info!("SLSM set operation triggered compaction.")
            }
            return res;
        }

        if !self.memtable.can_create_run() {
            // merge memtable and storage it to disk and become sstable.
            // Notice: we should try to avoid that occurred the behaviour of compression
            // because this behaviour is very slowly that it will sync to scanned the all inactive table in memtable
            // and compress it since than storage it to disk. Although, i want to do it asynchronous but it's so complex.
            // // so in order to avoid compress that will launch a compress thread on memtable instance init.

            // let signal: (Sender<()>, Receiver<()>) = unbounded();
            // self.compact.process_compaction(signal.0);
            //
            // match signal.1.recv() {
            //     Err(e) => {
            //         error!("Received compression err:{}", e)
            //     }
            //     Ok(_) => {
            //         info!("Received compression finished signal.");
            //         // check again whether it can write.
            //         if !self.memtable.read()?.can_create_run() {
            //             return Err(Error::InternalErr("After Compression, there's still no space, Please check memory and disk.".to_string()));
            //         }
            //     }
            // }
        }

        // can write, do it.
        return match self.memtable.create_run() {
            Ok(_) => {
                self.memtable.set(key, value);

                if self.maybe_need_compaction() {
                    info!("SLSM set operation triggered compaction.")
                }

                Ok(())
            }
            Err(e) => Err(e),
        };
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let res = match self.memtable.get(key)? {
            None => self.table.get(key),
            Some(v) => Ok(Some(v.to_vec())),
        };
        if self.maybe_need_compaction() {
            info!("SLSM get operation triggered compaction.");
            // self.compact.process_compaction();
        }
        res
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.set(key, Vec::from("Deleted"))
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }

    fn scan(&self, range: Range) -> Scan {
        todo!()
    }

    /// Send compaction signal to channel, if memtable or sstable need to compaction,
    /// this method will be called after all slsm operations.
    fn maybe_need_compaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_work() {
        assert!(1 == 1)
    }
}

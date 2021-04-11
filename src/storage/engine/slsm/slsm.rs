//! The slsm outermost layer.
use crate::storage::engine::slsm::compaction::Compaction;
use crate::storage::engine::slsm::skl::{FixedLengthSuffixComparator, KeyComparator};
use crate::storage::engine::slsm::Deleted;
use crate::storage::{
    config::StorageConfig,
    engine::slsm::{memtable::MemTable, table::Table},
    error::{Error, Result},
    Range, Scan, Storage,
};
use bytes::Bytes;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::{Condvar, Mutex, RwLock};
use tokio::sync::mpsc;
pub use tracing::{debug, error, info, warn};

pub struct SLSM<C: KeyComparator> {
    pub(crate) config: StorageConfig,
    pub(crate) memtable: RwLock<MemTable<C>>,
    pub(crate) table: RwLock<Table>,
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
    pub(crate) compact: Compaction,
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
            memtable: RwLock::new(MemTable::create(config, compact_channel.1.clone()).unwrap()),
            table: RwLock::new(Table::create(config, compact_channel.1).unwrap()),
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
        self.memtable.read()?.start()?;
        self.table.read()?.start()?;
        Ok(())
    }

    #[inline]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        if !self.memtable.read()?.is_run_full() {
            let res = self.memtable.write()?.set(key, value);
            if self.maybe_need_compaction() {
                info!("SLSM set operation triggered compaction.")
            }
            return res;
        }

        if !self.memtable.read()?.can_create_run() {
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
        return match self.memtable.read()?.create_run() {
            Ok(_) => {
                self.memtable.write()?.set(key, value);

                if self.maybe_need_compaction() {
                    info!("SLSM set operation triggered compaction.")
                }

                Ok(())
            }
            Err(e) => Err(e),
        };
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let res = match self.memtable.read()?.get(key)? {
            None => self.table.read()?.get(key),
            Some(v) => Ok(Some(v.to_vec())),
        };
        if self.maybe_need_compaction() {
            info!("SLSM get operation triggered compaction.");
            // self.compact.process_compaction();
        }
        res
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.set(key, Vec::from(Deleted))
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

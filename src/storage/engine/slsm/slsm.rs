//! The slsm outermost layer.
use crate::storage::{
    config::StorageConfig,
    error::{Error, Result},
    Range, Scan, Storage,
    engine::slsm::{
        memtable::MemTable,
        table::Table,
    },
};
use std::fmt::{Display, Formatter};
use tokio::sync::mpsc;
use bytes::Bytes;
use std::sync::RwLock;
use crate::storage::engine::slsm::Deleted;
use crate::storage::engine::slsm::skl::{FixedLengthSuffixComparator, KeyComparator};
use std::fmt;

pub struct SLSM<C: KeyComparator> {
    pub(crate) config: StorageConfig,
    pub(crate) memtable: RwLock<MemTable<C>>,
    pub(crate) table: RwLock<Table>,
}

impl<C: KeyComparator> Display for SLSM<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "slsm")
    }
}

impl<C: KeyComparator> SLSM<C> {
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
            return self.memtable.write()?.set(key, value);
        }

        if !self.memtable.read()?.can_create_run() {
            // merge memtable and storage it to disk and become sstable.
            // Notice: we should try to avoid that occurred the behaviour of compression
            // because this behaviour is very slowly that it will sync to scanned the all inactive table in memtable
            // and compress it since than storage it to disk. Although, i want to do it asynchronous but it's so complex.
            // so in order to avoid compress that will launch a compress thread on memtable instance init.
            self.memtable.write()?.compression()?;
        }

        if !self.memtable.read()?.can_create_run() {
            return Err(Error::InternalErr("After Compression, there's still no space, Please check memory and disk.".to_string()));
        }

        return match self.memtable.read()?.create_run() {
            Ok(_) => {
                self.memtable.write()?.set(key, value);
                Ok(())
            }
            Err(e) => {
                Err(e)
            }
        };
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.read()?.get(key)? {
            None => {}
            Some(v) => {
                return Ok(Some(v.to_vec()));
            }
        }
        self.table.read()?.get(key)
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
}
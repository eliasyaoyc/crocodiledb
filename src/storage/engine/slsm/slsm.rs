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
use crate::storage::engine::slsm::skl::FixedLengthSuffixComparator;

pub struct SLSM {
    pub(crate) config: StorageConfig,
    pub(crate) memtable: RwLock<MemTable>,
    pub(crate) table: RwLock<Table>,
}

impl Display for SLSM {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "slsm")
    }
}

pub struct SLSMBuilder {
    pub config: StorageConfig,
}

impl SLSMBuilder {
    fn default() -> Self {
        Self {
            config: StorageConfig::default(),
        }
    }

    fn set_config(&mut self, config: StorageConfig) -> &Self {
        self.config = config;
        self
    }

    /// builder is not start a SLSM that just builtin initialization memtable and table,
    /// so if you wan't real-start SLSM, you must manual call start fn.
    fn build(self) -> Result<SLSM> {
        let memtable = MemTable::create(&self.config)?;
        let table = Table::create(&self.config)?;
        Ok(SLSM {
            config: self.config,
            memtable: RwLock::new(memtable),
            table: RwLock::new(table),
        })
    }
}

impl SLSM {
    /// start that real-start SLSM engine.
    pub fn start(&self) -> Result<()> {
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }

    #[inline]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }
}

impl Storage for SLSM {
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
        let res = self.memtable.read()?.get(key)?;
        if res.is_some() {
            return Ok(res);
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
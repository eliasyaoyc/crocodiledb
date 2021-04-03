//! The slsm outermost layer.
use crate::storage::{
    config::StorageConfig,
    error::{Error, Result},
    Range, Scan, Storage,
};
use std::fmt::{Display, Formatter};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct SLSM {
    pub(crate) config: StorageConfig,
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
    fn default(shutdown_rx: mpsc::UnboundedReceiver<()>) -> Self {
        Self {
            config: StorageConfig::default(),
        }
    }

    fn set_config(&mut self, config: StorageConfig) -> &Self {
        self.config = config;
        self
    }

    fn build(self) -> SLSM {
        SLSM {
            config: self.config,
        }
    }
}

impl Storage for SLSM {
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

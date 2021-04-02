use crate::storage::error::{Error, Result};
use crate::storage::engine::slsm::skl::{
    FixedLengthSuffixComparator,
    slist::SkipList,
};
use crate::storage::{Scan, Range};

/// MemTable represent the active table so that all modified operations
/// will happened on.
#[derive(Clone)]
pub struct MemTable {
    skl: SkipList<FixedLengthSuffixComparator>
}

impl MemTable {
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
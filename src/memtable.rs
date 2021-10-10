use crate::db::format::{LookupKey, ValueType};
use crate::IResult;
use crate::memtable::key::KeyComparator;
use crate::memtable::skiplist::Skiplist;
use crate::iterator::Iterator;

mod arena;
mod key;
pub mod skiplist;

pub struct MemTable<C> {
    table: Skiplist<C>,
}

impl<C: KeyComparator> MemTable<C> {
    pub fn with_capacity(cmp: C, capacity: u32) -> Self {
        Self {
            table: Skiplist::with_capacity(cmp, capacity),
        }
    }

    /// Insert record to memtable.
    ///
    /// ```test
    /// Format of an entry is concatenation of:
    ///
    /// internal_key : user_key + sequence number + type
    /// key_size     : varint32 of internal_key.size()
    /// key bytes    : &[internal_key.size()]
    /// value_size   : varint32 of value.size()
    /// value bytes  : &[value.size()]
    /// ```
    pub fn add(&mut self, sequence_number: u64, typ: ValueType, key: &[u8], value: &[u8]) {}

    pub fn get(&self, key: LookupKey) -> Option<IResult<Vec<u8>>> {
        None
    }
}


impl<C: KeyComparator> Iterator for MemTable<C> {
    fn valid(&self) -> bool {
        todo!()
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn status(&mut self) -> IResult<()> {
        todo!()
    }
}
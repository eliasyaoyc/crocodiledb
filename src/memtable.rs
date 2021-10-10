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
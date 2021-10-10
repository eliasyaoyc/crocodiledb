use crate::memtable::key::KeyComparator;
use crate::memtable::skiplist::Skiplist;

mod arena;
mod key;
pub mod skiplist;

pub struct MemTable<C> {
    table: Skiplist<C>,
}

impl<C> MemTable<C>
    where C: KeyComparator
{
    pub fn with_capacity(cmp: C, capacity: u32) -> Self {
        Self {
            table: Skiplist::with_capacity(cmp, capacity),
        }
    }
}
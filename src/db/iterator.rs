use std::sync::Arc;
use crate::db::DBImpl;
use crate::error::Error;
use crate::iterator::Iter;
use crate::storage::Storage;
use crate::util::comparator::Comparator;

#[derive(Eq, PartialEq)]
enum Direction {
    // When moving forward, the internal iterator is positioned at
    // the exact entry that yields `inner.key()`, `inner.value()`.
    Forward,
    // When moving backwards, the internal iterator is positioned
    // just before all entries whose user key == `inner.key()`
    Reverse,
}

/// Memtables and sstables that make the DB representation contain
/// (userkey,sequence_number,type) => uservalue entries.
/// `DBIterator` combines multiple entries for the same userkey found in the DB
/// representation into a single entry while accounting for sequence
/// numbers, deletion markers, overwrites,etc.
pub struct DBIterator<I: Iter, S: Storage + Clone + 'static, C: Comparator> {
    valid: bool,
    db: Arc<DBImpl<S, C>>,
    ucmp: C,
    sequence: u64,
    err: Option<Error>,
    inner: I,
    direction: Direction,
    // Used fro randomly picking a yielded key to record read stats.
    bytes_util_read_sampling: u64,

    // Current key when direction is reserve.
    saved_key: Vec<u8>,
    // Current value when direction is reserve.
    saved_value: Vec<u8>,
}

/// Iterating from memtable iterators to table iterators.
pub struct DBIteratorCore<C: Comparator, M: Iter, T: Iter> {
    cmp: C,
    mem_iters: Vec<M>,
    table_iters: Vec<T>,
}

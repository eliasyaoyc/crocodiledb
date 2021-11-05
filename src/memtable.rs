use crate::db::format::{INTERNAL_KEY_TAIL, LookupKey, ValueType};
use crate::error::Error;
use crate::IResult;
use crate::memtable::key::KeyComparator;
use crate::memtable::skiplist::Skiplist;
use crate::iterator::Iter;
use crate::skiplist::SkiplistIterator;
use crate::util::coding::{decode_fixed_64, put_fixed_64, VarintU32};
use crate::util::comparator::Comparator;

mod arena;
mod key;
pub mod skiplist;

pub struct MemTable<C> {
    table: Skiplist<C>,
}

impl<C: Comparator> MemTable<C> {
    pub fn with_capacity(cmp: C, capacity: usize) -> Self {
        Self {
            table: Skiplist::with_capacity(cmp, capacity),
        }
    }

    #[inline]
    pub fn iter(&self) -> MemTableIterator<C> {
        let skiplist = SkiplistIterator::new(self.table.clone());
        MemTableIterator::new(skiplist)
    }

    /// Add an entry into memtable that maps key to value at the
    /// specified sequence number and with the specified type.
    /// Typically value will be empty if type==kTypeDeletion.
    ///
    /// ```text
    /// Format of an entry is concatenation of:
    ///
    /// internal_key : user_key + sequence number + type
    /// key_size     : varint32 of internal_key.size()
    /// key bytes    : &[internal_key.size()]
    /// value_size   : varint32 of value.size()
    /// value bytes  : &[value.size()]
    /// ```
    /// TODO remove value, now will insert the empty arr to skiplist and it will cost 24bits.
    pub fn add(&self, sequence_number: u64, value_type: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let internal_key_size = key_size + 8;
        let mut buf: Vec<u8> = vec![];
        VarintU32::put_varint(&mut buf, internal_key_size as u32);
        buf.extend_from_slice(key);
        put_fixed_64(&mut buf, (sequence_number << 8) | value_type as u64);
        // notice `put_varint_prefixed_slice` will add value size first then add data.
        VarintU32::put_varint_prefixed_slice(&mut buf, value);
        self.table.put(buf, vec![]);
    }

    /// If memtable contains a value for key, store it in value and return `Some(Ok())`.
    /// If memtable contains a deletion for key, returns `Some(Err(Status::NotFound))`
    /// in status and return true, otherwise return `None`.
    pub fn get(&self, key: &LookupKey) -> Option<IResult<Vec<u8>>> {
        let mem_key = key.memtable_key();
        let mut iter = SkiplistIterator::new(self.table.clone());
        iter.seek(mem_key);
        if iter.valid() {
            let mut k = iter.key();
            let ikey = extract_varint32_encoded_slice(&mut k);
            let key_size = ikey.len();
            match self.table.c.compare(&ikey[..key_size - INTERNAL_KEY_TAIL], key.user_key()) {
                std::cmp::Ordering::Equal => {
                    let tag = decode_fixed_64(&ikey[key_size - INTERNAL_KEY_TAIL..]);
                    match ValueType::from(tag & 0xff_u64) {
                        ValueType::KTypeValue => {
                            return Some(Ok(extract_varint32_encoded_slice(&mut k).to_vec()));
                        }
                        ValueType::KTypeDeletion => {
                            return Some(Err(Error::NotFound));
                        }
                        _ => {}
                    }
                }
                _ => return None,
            }
        }
        None
    }

    /// Returns an estimate of the number of bytes of data in use by this
    /// data structure. It is safe to call when MemTable is being modified.
    pub fn memory_usage(&self) -> usize {
        self.table.memory_usage()
    }
}

/// Decodes the length (varint u32) from `src` and advances it.
fn extract_varint32_encoded_slice<'a>(src: &mut &'a [u8]) -> &'a [u8] {
    if src.is_empty() {
        return src;
    }
    VarintU32::get_varint_prefixed_slice(src).unwrap_or(src)
}

pub struct MemTableIterator<C: Comparator> {
    iter: SkiplistIterator<C>,
}

impl<C: Comparator> MemTableIterator<C> {
    pub fn new(iter: SkiplistIterator<C>) -> Self {
        Self { iter }
    }
}

impl<C: Comparator> Iter for MemTableIterator<C> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn seek(&mut self, target: &[u8]) {
        self.iter.seek(&target)
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.iter.seek_for_prev(&target)
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    fn key(&self) -> &[u8] {
        let mut key = self.iter.key();
        extract_varint32_encoded_slice(&mut key)
    }

    fn value(&self) -> &[u8] {
        let mut value = self.iter.value();
        extract_varint32_encoded_slice(&mut value);
        extract_varint32_encoded_slice(&mut value)
    }

    fn status(&mut self) -> IResult<()> {
        self.iter.status()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::format::{LookupKey, ValueType};
    use crate::iterator::Iter;
    use crate::memtable::key::FixedLengthSuffixComparator;
    use crate::memtable::MemTable;
    use crate::util::comparator::BytewiseComparator;

    #[test]
    fn test_add_and_get() {
        let cmp = BytewiseComparator::default();;
        let mut mem = MemTable::with_capacity(cmp, 1 << 20);
        mem.add(1, ValueType::KTypeValue, b"foo", b"val1");
        mem.add(2, ValueType::KTypeValue, b"foo", b"val2");

        let v = mem.get(&LookupKey::new(b"foo", 1));
        assert_eq!(b"val1", v.unwrap().unwrap().as_slice());
        let v = mem.get(&LookupKey::new(b"foo", 2));
        assert_eq!(b"val2", v.unwrap().unwrap().as_slice());
    }

    #[test]
    fn test_memtable_scan() {
        let cmp = BytewiseComparator::default();
        let mut mem = MemTable::with_capacity(cmp, 1 << 20);
        let mut iter = mem.iter();
        let tests = vec![
            (2, ValueType::KTypeValue, "boo", "boo"),
            (4, ValueType::KTypeValue, "foo", "val3"),
            (3, ValueType::KTypeDeletion, "foo", ""),
            (2, ValueType::KTypeValue, "foo", "val2"),
            (1, ValueType::KTypeValue, "foo", "val1"),
        ];
        for (seq, t, key, value) in tests.clone().drain(..) {
            mem.add(seq, t, key.as_bytes(), value.as_bytes());
        }
        iter.seek_to_first();
        assert!(iter.valid());
        let key = iter.key();
    }
}
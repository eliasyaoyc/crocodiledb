use crate::db::format::{INTERNAL_KEY_TAIL, LookupKey, ValueType};
use crate::error::Error;
use crate::IResult;
use crate::memtable::key::KeyComparator;
use crate::memtable::skiplist::Skiplist;
use crate::iterator::Iterator;
use crate::util::coding::{decode_fixed_64, put_fixed_64, VarintU32};

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
    pub fn add(&mut self, sequence_number: u64, value_type: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let value_size = value.len();
        let internal_key_size = key_size + 8;
        let mut buf: Vec<u8> = vec![];
        VarintU32::put_varint(&mut buf, internal_key_size as u32);
        buf.extend_from_slice(key);
        put_fixed_64(&mut buf, (sequence_number << 8) | value_type as u64);
        VarintU32::put_varint_prefixed_slice(&mut buf, value);
        self.table.put(buf, vec![]);
    }

    /// If memtable contains a value for key, store it in value and return `Some(Ok())`.
    /// If memtable contains a deletion for key, returns `Some(Err(Status::NotFound))`
    /// in status and return true, otherwise return `None`.
    pub fn get(&self, key: &LookupKey) -> Option<IResult<Vec<u8>>> {
        let mem_key = key.memtable_key();
        let mut iter = self.table.iter();
        iter.seek(mem_key);
        if iter.valid() {
            let mut k = iter.key().to_vec();
            let mut k: &[u8] = &k;
            let ikey = extract_varint32_encoded_slice(&mut k);
            let key_size = ikey.len();
            match self.table.c.compare_key(&ikey[..key_size - INTERNAL_KEY_TAIL], key.user_key()) {
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
use crate::db::format::ValueType::{KTypeDeletion, KTypeValue, UnKnown};
use crate::util::coding::{put_fixed_64, VarintU32};

/// The max key sequence number. The value is 2^56 -1 because the sequence number
/// only task 56 bits when is serialized to `InternalKey`.
pub const MAX_KEY_SEQUENCE: u64 = (1u64 << 56) - 1;

/// The tail bytes length of an internal key
/// 7bytes sequence number + 1byte type number
pub const INTERNAL_KEY_TAIL: usize = 8;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ValueType {
    KTypeDeletion = 0x0,
    KTypeValue = 0x1,
    UnKnown,
}

impl From<u64> for ValueType {
    fn from(v: u64) -> Self {
        match v {
            0 => KTypeDeletion,
            1 => KTypeValue,
            _ => UnKnown,
        }
    }
}

/// A `LookupKey` represents a 'Get' request from the user by the give key with a
/// specific sequence number to perform a MVCC style query.
///
/// The format of a `LookupKey`:
///
/// ```text
///
///   +---------------------------------+
///   | varint32 of internal key length |
///   +---------------------------------+ --------------- user key start
///   | user key bytes                  |
///   +---------------------------------+   internal key
///   | sequence (7)        |  type (1) |
///   +---------------------------------+ ---------------
///
/// ```
pub struct LookupKey {
    data: Vec<u8>,
    user_key_start: usize,
}

impl LookupKey {
    pub fn new(user_key: &[u8], sequence_number: u64) -> Self {
        let mut data = vec![];
        let user_key_start = VarintU32::put_varint(&mut data, (user_key.len() + INTERNAL_KEY_TAIL) as u32);
        data.extend_from_slice(user_key);
        put_fixed_64(&mut data, compose_seq_number_and_type(sequence_number, ValueType::KTypeValue));
        LookupKey { data, user_key_start }
    }

    /// Return a key suitable for lookup in a MemTable.
    pub fn memtable_key(&self) -> &[u8] {
        &self.data
    }

    /// Return a internal key(suitable for passing to an internal iterator).
    pub fn internal_key(&self) -> &[u8] {
        &self.data[self.user_key_start..]
    }

    /// Return the user key.
    pub fn user_key(&self) -> &[u8] {
        &self.data[self.user_key_start..self.data.len() - INTERNAL_KEY_TAIL]
    }
}

/// Compose the sequence number and value type into a single u64.
fn compose_seq_number_and_type(seq_number: u64, value_type: ValueType) -> u64 {
    assert!(
        seq_number <= MAX_KEY_SEQUENCE,
        "key sequence number should be less than {}, but goy {}",
        MAX_KEY_SEQUENCE,
        seq_number
    );
    seq_number << 8 | value_type as u64
}

pub struct InternalKey {}
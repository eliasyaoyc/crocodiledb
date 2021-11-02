use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::BufRead;
use std::sync::Arc;
use crate::db::format::ValueType::{KTypeDeletion, KTypeValue, UnKnown};
use crate::filter::FilterPolicy;
use crate::util::coding::{decode_fixed_64, put_fixed_64, VarintU32};
use crate::util::comparator::Comparator;

/// The maximum level of SSTable.
pub const NUM_LEVELS: u32 = 7;

/// Level-0 compaction is started when we hit this many files.
pub const L0_COMPACTION_TRIGGER: u32 = 4;

/// Soft limit on number of level-0 files. We slow down writes at this point. (Sleep 1ms).
pub const L0_SLOWDOWN_WRITES_TRIGGER: u32 = 8;

/// Maximum number if level-0 files. We stop writes at this point.
pub const L0_STOP_WRITES_TRIGGER: u32 = 12;

/// Maximum level to which a new compacted memtable is pushed if it
/// dose not create overlap. We try to push to level 2 to avoid the
/// relatively expensive level 0=> 1 compactions and to avoid some
/// expensive manifest file operations. We do not push all the way to
/// the largest level since that can generate a lot of wasted dist
/// space if the same key space is being repeatedly overwritten.
pub const MAX_MEM_COMPACT_LEVEL: u32 = 2;

/// Approximate gap in bytes between samples of data read during iteration.
pub const READ_BYTES_PERIOD:u32 = 1047576;

/// The max key sequence number. The value is 2^56 -1 because the sequence number
/// only task 56 bits when is serialized to `InternalKey`.
pub const MAX_KEY_SEQUENCE: u64 = (1u64 << 56) - 1;

/// `VALUE_TYPE_FOR_SEEK` defines the `ValueType` that should be passed when
/// constructing a `ParsedInternalKey` object for seeking to a particular
/// sequence number (since we sort sequence numbers in decreasing order and
/// the value type is embedded as the low 8 bits in the sequence
/// number in internal keys, we need to use the highest-numbered ValueType
/// ,not the lowest).
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::KTypeValue;

/// The tail bytes length of an internal key
/// 7bytes sequence number + 1byte type number
pub const INTERNAL_KEY_TAIL: usize = 8;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

/// Represent a internal key that can be encoded into a `InternalKey` by `encode()`.
pub struct ParsedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub sequence_number: u64,
    pub value_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    pub fn new(user_key: &'a [u8], sequence_number: u64, value_type: ValueType) -> Self {
        ParsedInternalKey { user_key, sequence_number, value_type }
    }

    /// Returns a `InternalKey` encoded from the `ParsedInternalKey` using
    /// the format described in the below comment of `InternalKey`.
    pub fn encode(&self) -> InternalKey {
        InternalKey::new(self.user_key, self.sequence_number, self.value_type)
    }

    /// Try to extract a `ParsedInternalKey` from the given bytes.
    /// Returns `None` if data length is less than 8 or getting an unknown value type.
    pub fn parse_internal_key(internal_key: &'a [u8]) -> Option<ParsedInternalKey<'_>> {
        let size = internal_key.len();
        if size < INTERNAL_KEY_TAIL {
            return None;
        }

        let num = decode_fixed_64(&internal_key[size - INTERNAL_KEY_TAIL..]);
        let t = ValueType::from(num & 0xff);
        if t == ValueType::UnKnown {
            return None;
        }
        let sequence = num >> INTERNAL_KEY_TAIL;
        Some(ParsedInternalKey {
            user_key: &internal_key[..size - INTERNAL_KEY_TAIL],
            sequence_number: sequence,
            value_type: t,
        })
    }

    /// Return the inner user key as a &str.
    pub fn extract_user_key_str(&self) -> &'a str {
        std::str::from_utf8(self.user_key).unwrap()
    }

    /// Return the length of the encoding of "key".
    #[inline]
    pub fn length(&self) -> usize {
        self.user_key.len() + INTERNAL_KEY_TAIL
    }
}

impl<'a> Debug for ParsedInternalKey<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} @ {} : {:?}", self.user_key, self.sequence_number, self.value_type)
    }
}

#[derive(Default,Clone, PartialEq, Eq)]
pub struct InternalKey {
    data: Vec<u8>,
}

impl InternalKey {
    pub fn new(key: &[u8], seq: u64, t: ValueType) -> Self {
        let mut v = Vec::from(key);
        put_fixed_64(&mut v, pack_sequence_and_type(seq, t));
        InternalKey {
            data: v
        }
    }

    #[inline]
    pub fn decoded_from(src: &[u8]) -> Self {
        // TODO: avoid copy here
        Self {
            data: Vec::from(src),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn user_key(&self) -> &[u8] {
        let length = self.data.len();
        &self.data[..length - INTERNAL_KEY_TAIL]
    }

    /// Returns a `ParsedInternalKey`
    pub fn parsed(&self) -> Option<ParsedInternalKey<'_>> {
        let size = self.data.len();
        let user_key = &(self.data.as_slice())[..size - INTERNAL_KEY_TAIL];
        let num = decode_fixed_64(&(self.data.as_slice())[size - INTERNAL_KEY_TAIL..]);
        let t = ValueType::from(num & 0xff_u64);
        match t {
            ValueType::UnKnown => None,
            _ => Some(ParsedInternalKey {
                user_key,
                sequence_number: num >> 8,
                value_type: t,
            }),
        }
    }
}

impl Debug for InternalKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(parsed) = self.parsed() {
            write!(f, "{:?}", parsed)
        } else {
            let s = unsafe { std::str::from_utf8_unchecked(self.data.as_slice()) };
            write!(f, "(bad){}", s)
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
        put_fixed_64(&mut data, pack_sequence_and_type(sequence_number, ValueType::KTypeValue));
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

/// A comparator for internal keys that uses a specified comparator for
/// the user key portion and breaks ties by decreasing sequence number.
#[derive(Clone,Default)]
pub struct InternalKeyComparator<C: Comparator> {
    pub comparator: C,
}

impl<C: Comparator> InternalKeyComparator<C> {
    pub fn new(c: C) -> Self {
        InternalKeyComparator { comparator: c }
    }
}

impl<C: Comparator> Comparator for InternalKeyComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        match self.comparator.compare(extract_user_key(a), extract_user_key(b)) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let a_num = decode_fixed_64(&a[a.len() - INTERNAL_KEY_TAIL..]) >> INTERNAL_KEY_TAIL;
                let b_num = decode_fixed_64(&b[b.len() - INTERNAL_KEY_TAIL..]) >> INTERNAL_KEY_TAIL;
                if a_num > b_num{
                    Ordering::Less
                } else if a_num == b_num {
                    Ordering::Equal
                }else {
                    Ordering::Greater
                }
            }
        }
    }

    fn name(&self) -> &str {
        "InternalKeyComparator"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        // Attempt to shorten the user portion of the key.
        let user_start = extract_user_key(start);
        let user_limit = extract_user_key(limit);
        let mut separator = self.comparator.find_shortest_separator(user_start, user_limit);
        if separator.len() < user_start.len()
            && self.comparator.compare(user_start, &separator) == Ordering::Less {
            // User key has become shorter physically,but larger logically
            // Tack on the earliest possible number to the shortened user key.
            put_fixed_64(&mut separator, pack_sequence_and_type(MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK));
            separator
        } else {
            start.to_owned()
        }
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let user_key = extract_user_key(key);
        let mut successor = self.comparator.find_short_successor(user_key);
        if successor.len() < user_key.len() &&
            self.comparator.compare(user_key, &successor) == Ordering::Less {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            put_fixed_64(&mut successor, pack_sequence_and_type(MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK));
            successor
        } else {
            key.to_owned()
        }
    }
}

/// Filter policy wrapper that converts from internal keys to user keys.
pub struct InternalFilterPolicy {
    pub user_policy: Arc<dyn FilterPolicy>,
}

impl InternalFilterPolicy {
    pub fn new(user_policy: Arc<dyn FilterPolicy>) -> Self {
        InternalFilterPolicy { user_policy }
    }
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &str {
        self.user_policy.name()
    }

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        let mut user_keys = vec![];
        for key in keys {
            let user_key = extract_user_key(key.as_slice());
            user_keys.push(Vec::from(user_key));
        }
        self.user_policy.create_filter(user_keys.as_slice())
    }

    fn key_may_match(&self, filter: &[u8], key: &[u8]) -> bool {
        let user_key = extract_user_key(key);
        self.user_policy.key_may_match(filter, user_key)
    }
}


/// Compose the sequence number and value type into a single u64.
fn pack_sequence_and_type(seq_number: u64, value_type: ValueType) -> u64 {
    assert!(
        seq_number <= MAX_KEY_SEQUENCE,
        "key sequence number should be less than {}, but goy {}",
        MAX_KEY_SEQUENCE,
        seq_number
    );
    seq_number << 8 | value_type as u64
}

/// Returns the user key portion of an internal key.
fn extract_user_key(internal_key: &[u8]) -> &[u8] {
    let len = internal_key.len();
    assert!(len >= INTERNAL_KEY_TAIL,
            "the size of internal key must be greater than {}, but got {}",
            INTERNAL_KEY_TAIL,
            len);
    &internal_key[..len - INTERNAL_KEY_TAIL]
}

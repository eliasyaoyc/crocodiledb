/// The tail bytes length of an internal key
/// 7bytes sequence number + 1byte type number
pub const INTERNAL_KEY_TAIL: usize = 8;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ValueType {
    KTypeDeletion = 0x0,
    KTypeValue = 0x1,
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

}

pub struct InternalKey {}
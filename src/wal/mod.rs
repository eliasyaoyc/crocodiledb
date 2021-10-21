mod writer;
mod reader;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RecordType {
    // Zero is reserved for preallocated files.
    KZeroType = 0,
    KFullType = 1,

    // For fragments.
    KFirstType = 2,
    KMiddleType = 3,
    KLastType = 4,
}

impl From<usize> for RecordType {
    fn from(v: usize) -> Self {
        match v {
            0 => RecordType::KZeroType,
            1 => RecordType::KFullType,
            2 => RecordType::KFirstType,
            3 => RecordType::KMiddleType,
            4 => RecordType::KLastType,
            _ => panic!("non corresponding type.")
        }
    }
}

pub const MAX_RECORD_TYPE: RecordType = RecordType::KLastType;

pub const BLOCK_SIZE: usize = 32768;

/// Header is checksum(4 bytes)  + length(2 bytes) + type(1 byte).
pub const HEADER_SIZE: usize = 4 + 2 + 1;
mod log_writer;
mod log_reader;


pub trait Log {}

pub enum RecordType {
    KZeroType = 0,
    KFullType = 1,
    KFirstType = 2,
    KMiddleType = 3,
    KLastType = 4,
}

impl From<u32> for RecordType {
    fn from(v: u32) -> Self {
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

/// Header is checksum(4 bytes)  + length(2 bytes) + type(1 byte).
const HEADER: usize = 4 + 2 + 1;
use crate::error::Result;
use std::ops::Bound;
use crate::storage::types::{Key, Value};

pub trait Storage: Send + Sync {
    /// Gets a value for a key, if it exists.
    fn get(&self, key: Key) -> Result<Option<Value>>;
    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: Key, value: Value) -> Result<()>;
    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: Key) -> Result<()>;
    /// Flushes any buffered data to the underlying storage engine.
    fn flush(&mut self) -> Result<()>;

    fn scan(&self, range: Range) -> Scan;
}

/// Scan range including start bound and end bound.
pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

/// Iterator over a key/value range.
pub type Scan = Box<dyn DoubleEndedIterator<Item=std::result::Result<Key, Value>> + Send>;
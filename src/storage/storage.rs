use crate::storage::error::Result;
use std::ops::Bound;

pub trait Storage: Send + Sync {
    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;
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
pub type Scan = Box<dyn DoubleEndedIterator<Item = std::result::Result<Vec<u8>, Vec<u8>>> + Send>;

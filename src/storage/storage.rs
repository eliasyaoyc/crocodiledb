use crate::storage::error::Result;
use crossbeam_channel::bounded;
use std::ops::{Bound, RangeBounds};

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
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl Range {
    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }

    /// Checks if the given value is contained in the range.
    pub fn contains(&self, v: &[u8]) -> bool {
        (match &self.start {
            Bound::Included(start) => &**start <= v,
            Bound::Excluded(start) => &**start < v,
            Bound::Unbounded => true,
        }) && (match &self.end {
            Bound::Included(end) => &**end >= v,
            Bound::Excluded(end) => &**end > v,
            Bound::Unbounded => true,
        })
    }
}

impl RangeBounds<Vec<u8>> for Range {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Vec<u8>> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// Iterator over a key/value range.
pub type Scan = Box<dyn DoubleEndedIterator<Item = std::result::Result<Vec<u8>, Vec<u8>>> + Send>;

#[cfg(test)]
pub trait TestSuite<S: Storage> {
    fn setup() -> Result<S>;

    fn test() -> Result<()> {
        Self::t_delete()?;
        Self::t_get()?;
        Self::t_scan()?;
        Self::t_set()?;
        Self::t_random()?;
        Ok(())
    }

    fn t_get() -> Result<()> {
        Ok(())
    }

    fn t_delete() -> Result<()> {
        Ok(())
    }

    fn t_random() -> Result<()> {
        Ok(())
    }

    fn t_scan() -> Result<()> {
        Ok(())
    }

    fn t_set() -> Result<()> {
        Ok(())
    }
}

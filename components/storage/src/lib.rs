//! The current version that just support two kinds of engines, e.g memory and lsm
//! * Memory : all operation what action on the DRAM. The advantage is that it is very fast, but the data is easy to lose and  underlying data structure is B+Tree,
//!            as is known to all that the B+Tree write method is more serious, but it has better read performance(Ologn).
//! * SLSM : Log Structured Merged Tree given up a part of read performance be compared to B+Tree，but SLSM has very good write performance
//!         because it's going to written in direct order.
//! * WISCLSM : Not yet support in the short term.
mod btree;
mod config;
pub mod encoding;
mod factory;
mod memory;
mod slsm;
mod wisclsm;

use crocodiledb_components_error::error::{Error, Result};
use crossbeam_channel::bounded;
use serde_derive::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::{Bound, RangeBounds};

/// A specific value of a data type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

pub trait Storage: Display + Send + Sync {
    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to the underlying storage engine.
    fn flush(&mut self) -> Result<()>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&self, range: Range) -> Scan;
}

/// Scan range including start bound and end bound.
#[derive(Debug, Clone)]
pub struct Range {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl Range {
    /// Creates a new range from the given Rust range. We can't use the RangeBounds directly in
    /// scan() since that prevents us from using Store as a trait object. Also, we can't take
    /// AsRef<[u8]> or other convenient types, since that won't work with e.g. .. ranges.
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
            Bound::Included(end) => v <= &**end,
            Bound::Excluded(end) => v < &**end,
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
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

pub trait TestSuite<S: Storage> {
    fn setup() -> Result<S>;

    fn test() -> Result<()> {
        Self::test_delete()?;
        Self::test_get()?;
        Self::test_scan()?;
        Self::test_set()?;
        Self::test_random()?;
        Ok(())
    }

    fn test_get() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        assert_eq!(None, s.get(b"b")?);
        Ok(())
    }

    fn test_delete() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.delete(b"a")?;
        assert_eq!(None, s.get(b"a")?);
        s.delete(b"b")?;
        Ok(())
    }

    fn test_random() -> Result<()> {
        use rand::Rng;
        let mut s = Self::setup()?;
        let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(397_427_893);

        // Create a bunch of random items and insert them
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..1000_u64 {
            items.push((rng.gen::<[u8; 32]>().to_vec(), i.to_be_bytes().to_vec()))
        }
        for (key, value) in items.iter() {
            s.set(key, value.clone())?;
        }

        // Fetch the random items, both via get() and scan()
        for (key, value) in items.iter() {
            assert_eq!(s.get(key)?, Some(value.clone()))
        }
        let mut expect = items.clone();
        expect.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(expect, s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?);
        expect.reverse();
        assert_eq!(
            expect,
            s.scan(Range::from(..)).rev().collect::<Result<Vec<_>>>()?
        );

        // Remove the items
        for (key, _) in items {
            s.delete(&key)?;
            assert_eq!(None, s.get(&key)?);
        }
        assert!(s
            .scan(Range::from(..))
            .collect::<Result<Vec<_>>>()?
            .is_empty());

        Ok(())
    }

    fn test_scan() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        s.set(b"b", vec![0x02])?;
        s.set(b"ba", vec![0x02, 0x01])?;
        s.set(b"bb", vec![0x02, 0x02])?;
        s.set(b"c", vec![0x03])?;

        // Forward/backward ranges
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(Range::from(b"b".to_vec()..b"bz".to_vec()))
                .collect::<Result<Vec<_>>>()?
        );
        assert_eq!(
            vec![
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"b".to_vec(), vec![0x02]),
            ],
            s.scan(Range::from(b"b".to_vec()..b"bz".to_vec()))
                .rev()
                .collect::<Result<Vec<_>>>()?
        );

        // Inclusive/exclusive ranges
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
            ],
            s.scan(Range::from(b"b".to_vec()..b"bb".to_vec()))
                .collect::<Result<Vec<_>>>()?
        );
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(Range::from(b"b".to_vec()..=b"bb".to_vec()))
                .collect::<Result<Vec<_>>>()?
        );

        // Open ranges
        assert_eq!(
            vec![
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"c".to_vec(), vec![0x03]),
            ],
            s.scan(Range::from(b"bb".to_vec()..))
                .collect::<Result<Vec<_>>>()?
        );
        assert_eq!(
            vec![(b"a".to_vec(), vec![0x01]), (b"b".to_vec(), vec![0x02]),],
            s.scan(Range::from(..=b"b".to_vec()))
                .collect::<Result<Vec<_>>>()?
        );

        // Full range
        assert_eq!(
            vec![
                (b"a".to_vec(), vec![0x01]),
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"c".to_vec(), vec![0x03]),
            ],
            s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
        );
        Ok(())
    }

    fn test_set() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.set(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), s.get(b"a")?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

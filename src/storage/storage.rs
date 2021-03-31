use crate::storage::error::Result;
use crossbeam_channel::bounded;
use std::fmt::Display;
use std::ops::{Bound, RangeBounds};

pub trait Storage: Display + Send + Sync {
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
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

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
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        assert_eq!(None, s.get(b"b")?);
        Ok(())
    }

    fn t_delete() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.delete(b"a")?;
        assert_eq!(None, s.get(b"a")?);
        s.delete(b"b")?;
        Ok(())
    }

    fn t_random() -> Result<()> {
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

    fn t_scan() -> Result<()> {
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

    fn t_set() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.set(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), s.get(b"a")?);
        Ok(())
    }
}

mod test {
    use super::*;
    use crate::storage::engine::memory::Memory;
    use std::fmt::Display;
    use std::sync::{Arc, RwLock};
    use tokio::io::AsyncReadExt;

    /// Key-value storage backend for testing. Protects an inner Memory backend using a mutex, so it can
    /// be cloned and inspected.
    #[derive(Clone)]
    pub struct Test {
        kv: Arc<RwLock<Memory>>,
    }

    impl Test {
        /// Creates a new Test key-value storage engine.
        pub fn new() -> Self {
            Self {
                kv: Arc::new(RwLock::new(Memory::new())),
            }
        }
    }

    impl Display for Test {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test")
        }
    }

    impl Storage for Test {
        fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.kv.write()?.delete(key)
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.kv.read()?.get(key)
        }

        fn scan(&self, range: Range) -> Scan {
            // Since the mutex guard is scoped to this method, we simply buffer the result.
            Box::new(
                self.kv
                    .read()
                    .unwrap()
                    .scan(range)
                    .collect::<Vec<Result<_>>>()
                    .into_iter(),
            )
        }

        fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
            self.kv.write()?.set(key, value)
        }
    }

    #[cfg(test)]
    impl super::TestSuite<Test> for Test {
        fn setup() -> Result<Self> {
            Ok(Test::new())
        }
    }

    #[test]
    fn tests() -> Result<()> {
        use super::TestSuite;
        Test::test()
    }
}

use std::cmp::Ordering;

/// A Comparator object provides a total order access vec that are
/// used as keys in an sstable or a database. A Comaprator implementation
/// must be thread-safe since it may invoke its methods concurrently
/// from multiple threads.
pub trait Comparator: Sync + Send {
    /// Three-way  comparison. Returns value:
    /// Ordering::Less iff a < b
    /// Ordering::Equal iff a == b
    /// Ordering::Greater iff a > b
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// The name of comparator. Used to check for comparator
    ///mismatches (i.e., a DB created with one comparator is
    /// accessed using a different comparator.)
    fn name(&self) -> &str;

    /// Advance functions: these are used to reduce the space requirements
    /// for internal data structures like index blocks.
    ///
    /// if start > limit, returns a vec in [start,limit).
    /// Simple comparator implementations may return with start unchanged.
    /// i.e., an implementation of this method that dose noting is correct.
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8>;

    /// Returns a short string >= key.
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8>;
}

/// `ByteWiseComparator` uses lexicographic byte-wise ordering.
#[derive(Default)]
pub struct BytewiseComparator {}

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &str {
        "BytewiseComparator"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        // Find length of common prefix.
        let min_length = std::cmp::min(start.len(), limit.len());
        let mut diff_index = 0;
        while diff_index < min_length && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }
        if diff_index >= min_length {
            // Do not shorten if one string is a prefix of the other.
        } else {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff &&
                diff_byte + 1 < limit[diff_index] {
                let mut res = vec![0; diff_index + 1];
                res[0..=diff_index].copy_from_slice(&start[0..=diff_index]);
                *(res.last_mut().unwrap()) += 1;
                return res;
            }
        }
        start.to_owned()
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        // Find first character that can be incremented.
        let n = key.len();
        for i in 0..n {
            let byte = key[i];
            if byte != 0xff {
                let mut res = vec![0; i + 1];
                res[0..=i].copy_from_slice(&key[0..=i]);
                *(res.last_mut().unwrap()) += 1;
                return res;
            }
        }
        key.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use crate::util::comparator::{BytewiseComparator, Comparator};

    #[test]
    fn test_bytewise_comparator_separator() {
        let mut tests = vec![
            ("", "1111", ""),
            ("1111", "", "1111"),
            ("1111", "111", "1111"),
            ("123", "1234", "123"),
            ("1234", "1234", "1234"),
            ("1", "2", "1"),
            ("1357", "2", "1357"),
            ("1111", "12345", "1111"),
            ("1111", "13345", "12"),
        ];
        let c = BytewiseComparator::default();
        for (a, b, expect) in tests.drain(..) {
            let res = c.find_shortest_separator(a.as_bytes(), b.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        let a: Vec<u8> = vec![48, 255];
        let b: Vec<u8> = vec![48, 49, 50, 51];
        let res = c.find_shortest_separator(a.as_slice(), b.as_slice());
        assert_eq!(res, a);
    }

    #[test]
    fn test_bytewise_comparator_successor() {
        let mut tests = vec![
            ("", ""),
            ("111", "2"),
            ("222", "3"),
        ];
        let c = BytewiseComparator::default();
        for (v, expect) in tests.drain(..) {
            let res = c.find_short_successor(v.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }

        let mut corner_tests = vec![
            (vec![0xff, 0xff, 1], vec![255u8, 255u8, 2]),
            (vec![0xff, 0xff, 0xff], vec![255u8, 255u8, 255u8]),
        ];
        for (input, expect) in corner_tests.drain(..) {
            let res = c.find_short_successor(input.as_slice());
            assert_eq!(res, expect)
        }
    }
}
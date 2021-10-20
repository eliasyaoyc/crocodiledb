mod bloom;
mod cuckoo;

pub trait FilterPolicy {
    /// Returns the name of this policy. Note that if the filter encoding
    /// changes in an incompatible way, the name returned by this method
    /// must be changed. Otherwise, old incompatible filters may be
    /// pass to methods of this type.
    fn name(&self) -> &str;
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
    fn key_may_match(&self, filter: &[u8], key: &[u8]) -> bool;
}
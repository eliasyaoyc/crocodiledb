pub trait FilterPolicy {
    fn name(&self) -> &str;
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
    fn key_may_match(&self,filter: &[u8], key: &[u8]) -> bool;
}
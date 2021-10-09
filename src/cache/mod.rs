mod lru_cache;

pub trait Cache<K, V>: Sync + Send
    where
        K: Sync + Send,
        V: Sync + Send + Clone,
{
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V>;
    fn get(&self, key: &K) -> Option<V>;
    fn erase(&self, key: &K);
    fn total_charge(&self) -> usize;
}
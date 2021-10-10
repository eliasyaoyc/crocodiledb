use std::collections::hash_map::DefaultHasher;
use std::fmt::Pointer;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

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

pub struct SharedCache<C, K, V>
    where
        C: Cache<K, V>,
        K: Sync + Send,
        V: Sync + Send + Clone,
{
    shards: Vec<C>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<C, K, V> SharedCache<C, K, V>
    where
        C: Cache<K, V>,
        K: Sync + Send + Hash + Eq,
        V: Sync + Send + Clone,
{
    pub fn new(shards: Vec<C>) -> Self {
        Self {
            shards: shards,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    fn find_shard(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        let len = self.shards.len();
        key.hash(&mut hasher);
        hasher.finish() as usize % len
    }
}

impl<C, K, V> Cache<K, V> for SharedCache<C, K, V>
    where
        C: Cache<K, V>,
        K: Sync + Send + Hash + Eq,
        V: Sync + Send + Clone
{
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V> {
        let index = self.find_shard(&key);
        self.shards[index].insert(key, value, charge)
    }

    fn get(&self, key: &K) -> Option<V> {
        let index = self.find_shard(&key);
        self.shards[index].get(key)
    }

    fn erase(&self, key: &K) {
        let index = self.find_shard(&key);
        self.shards[index].erase(key)
    }

    fn total_charge(&self) -> usize {
        self.shards.iter().fold(0, |acc, cache| {
            acc + cache.total_charge()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use crate::cache::lru_cache::LRUCache;

    fn new_test_lru_shards(n: usize) -> Vec<LRUCache<String, String>> {
        (0..n).into_iter().fold(vec![], |mut acc, _| {
            acc.push(LRUCache::new(1 << 20));
            acc
        })
    }

    #[test]
    fn test_concurrent_insert() {
        let cache = Arc::new(SharedCache::new(new_test_lru_shards(8)));
        let n = 4; // use 4 thread
        let repeated = 10;
        let mut hs = vec![];
        let kv: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(vec![]));
        let total_size = Arc::new(AtomicU64::new(0));
        for i in 0..n {
            let cache = cache.clone();
            let kv = kv.clone();
            let total_size = total_size.clone();
            let h = thread::spawn(move || {
                for x in 1..=repeated {
                    let k = i.to_string().repeat(x);
                    let v = k.clone();
                    {
                        let mut kv = kv.lock().unwrap();
                        (*kv).push((k.clone(), v.clone()));
                    }
                    total_size.fetch_add(x as u64, Ordering::SeqCst);
                    assert_eq!(cache.insert(k, v, x), None);
                }
            });
            hs.push(h);
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(
            total_size.load(Ordering::Relaxed) as usize,
            cache.total_charge()
        );
        for (k, v) in kv.lock().unwrap().clone() {
            assert_eq!(cache.get(&k), Some(v));
        }
    }
}

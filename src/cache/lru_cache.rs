use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::{mem, ptr};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::cache::Cache;

#[derive(Clone)]
struct Key<K> {
    key: *const K,
}

impl<K: Hash> Hash for Key<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            (*self.key).hash(state)
        }
    }
}

impl<K: PartialEq> PartialEq<Self> for Key<K> {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            (*self.key).eq(&*other.key)
        }
    }
}

impl<K: Eq> Eq for Key<K> {}

impl<K> Default for Key<K> {
    fn default() -> Self {
        Self {
            key: ptr::null(),
        }
    }
}


struct LRUEntry<K, V> {
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    prev: *mut LRUEntry<K, V>,
    next: *mut LRUEntry<K, V>,
    charge: usize,
}

impl<K, V> LRUEntry<K, V> {
    fn new(key: K, value: V, charge: usize) -> Self {
        LRUEntry {
            key: MaybeUninit::new(key),
            value: MaybeUninit::new(value),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            charge,
        }
    }

    fn new_empty() -> Self {
        LRUEntry {
            key: MaybeUninit::uninit(),
            value: MaybeUninit::uninit(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            charge: 0,
        }
    }
}

pub struct LRUCache<K, V: Clone> {
    // The capacity of LRU
    capacity: usize,
    inner: Arc<Mutex<LRUInner<K, V>>>,
    // The size of space with have been allocated.
    usage: Arc<AtomicUsize>,
    evict_hook: Option<Box<dyn Fn(&K, &V)>>,
}

struct LRUInner<K, V> {
    table: HashMap<Key<K>, Box<LRUEntry<K, V>>>,
    head: *mut LRUEntry<K, V>,
    tail: *mut LRUEntry<K, V>,
}

impl<K, V> LRUInner<K, V> {
    fn remove(&mut self, entry: *mut LRUEntry<K, V>) {
        unsafe {
            (*(*entry).next).prev = (*entry).prev;
            (*(*entry).prev).next = (*entry).next;
        }
    }

    fn append(&mut self, entry: *mut LRUEntry<K, V>) {
        unsafe {
            (*entry).next = (*self.head).next;
            (*entry).prev = self.head;
            (*self.head).next = entry;
            (*(*entry).next).prev = entry;
        }
    }

    fn move_to_head(&mut self, entry: *mut LRUEntry<K, V>) {
        self.remove(entry);
        self.append(entry);
    }
}

impl<K: Hash + Eq, V: Clone> LRUCache<K, V> {
    pub fn new(cap: usize) -> Self {
        let mut inner = LRUInner {
            table: HashMap::default(),
            head: Box::into_raw(Box::new(LRUEntry::new_empty())),
            tail: Box::into_raw(Box::new(LRUEntry::new_empty())),
        };

        unsafe {
            (*inner.head).next = inner.tail;
            (*inner.tail).prev = inner.head;
        }

        LRUCache {
            capacity: cap,
            inner: Arc::new(Mutex::new(inner)),
            usage: Arc::new(AtomicUsize::new(0)),
            evict_hook: None,
        }
    }
}

impl<K, V> Cache<K, V> for LRUCache<K, V>
    where
        K: Sync + Send + Hash + Eq,
        V: Sync + Send + Clone,
{
    fn insert(&self, key: K, mut value: V, charge: usize) -> Option<V> {
        let mut inner = self.inner.lock().unwrap();
        if self.capacity > 0 {
            let k = Key { key: &key as *const K };
            match inner.table.get_mut(&k) {
                // Replace entry.
                Some(entry) => {
                    let old = entry as &mut Box<LRUEntry<K, V>>;
                    unsafe { mem::swap(&mut value, &mut (*(*old).value.as_mut_ptr())) };
                    let p = entry.as_mut() as *mut LRUEntry<K, V>;
                    inner.move_to_head(p);
                    if let Some(hk) = &self.evict_hook {
                        hk(&key, &value);
                    }
                    Some(value)
                }
                // Insert entry.
                None => {
                    let mut node = {
                        if self.usage.load(Ordering::Acquire) >= self.capacity {
                            let tail_key = Key {
                                key: unsafe { (*(*inner.tail).prev).key.as_ptr() },
                            };
                            let mut tail_entry = inner.table.remove(&tail_key).unwrap();
                            self.usage.fetch_sub(tail_entry.charge, Ordering::Relaxed);
                            if let Some(hk) = &self.evict_hook {
                                unsafe {
                                    hk(&(*tail_entry.key.as_ptr()), &(*tail_entry.value.as_ptr()));
                                }
                            }
                            unsafe {
                                ptr::drop_in_place(tail_entry.key.as_mut_ptr());
                                ptr::drop_in_place(tail_entry.value.as_mut_ptr());
                            }
                            // Replace new key and value.
                            tail_entry.key = MaybeUninit::new(key);
                            tail_entry.value = MaybeUninit::new(value);

                            inner.remove(tail_entry.as_mut());

                            tail_entry
                        } else {
                            Box::new(LRUEntry::new(key, value, charge))
                        }
                    };

                    self.usage.fetch_add(charge, Ordering::Relaxed);
                    inner.append(node.as_mut());

                    inner.table.insert(Key { key: node.key.as_ptr()}, node);
                    None
                }
            }
        } else {
            None
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        let key = Key { key: key as *const K };
        let mut inner = self.inner.lock().unwrap();
        if let Some(mut entry) = inner.table.get_mut(&key) {
            let p = entry.as_mut() as *mut LRUEntry<K, V>;
            inner.move_to_head(p);
            let v = unsafe {
                (*(*p).value.as_ptr()).clone()
            };
            Some(v)
        } else {
            None
        }
    }

    fn erase(&self, key: &K) {
        let k = Key { key: key as *const K };
        let mut inner = self.inner.lock().unwrap();
        if let Some(mut entry) = inner.table.remove(&k) {
            self.usage.fetch_sub(entry.charge, Ordering::SeqCst);
            inner.remove(entry.as_mut() as *mut LRUEntry<K, V>);
            unsafe {
                if let Some(cb) = &self.evict_hook {
                    cb(key, &(*entry.value.as_ptr()))
                }
            }
        }
    }

    #[inline]
    fn total_charge(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }
}

impl<K, V: Clone> Drop for LRUCache<K, V> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        (*inner).table.values_mut().for_each(|e| unsafe {
            ptr::drop_in_place(e.key.as_mut_ptr());
            ptr::drop_in_place(e.value.as_mut_ptr());
        });
        unsafe {
            let _head = *Box::from_raw(inner.head);
            let _tail = *Box::from_raw(inner.tail);
        }
    }
}

unsafe impl<K: Sync, V: Sync + Clone> Sync for LRUCache<K, V> {}

unsafe impl<K: Send, V: Send + Clone> Send for LRUCache<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    const CACHE_SIZE: usize = 100;

    struct CacheTest {
        cache: LRUCache<u32, u32>,
        deleted_kv: Rc<RefCell<Vec<(u32, u32)>>>,
    }

    impl CacheTest {
        fn new(cap: usize) -> Self {
            let deleted_kv = Rc::new(RefCell::new(vec![]));
            let cloned = deleted_kv.clone();
            let mut cache = LRUCache::<u32, u32>::new(cap);
            cache.evict_hook = Some(Box::new(move |k, v| {
                cloned.borrow_mut().push((*k, *v));
            }));
            Self { cache, deleted_kv }
        }

        fn get(&self, key: u32) -> Option<u32> {
            self.cache.get(&key)
        }

        fn insert(&self, key: u32, value: u32) {
            self.cache.insert(key, value, 1);
        }

        fn insert_with_charge(&self, key: u32, value: u32, charge: usize) {
            self.cache.insert(key, value, charge);
        }

        fn erase(&self, key: u32) {
            self.cache.erase(&key);
        }

        fn assert_deleted_kv(&self, index: usize, (key, val): (u32, u32)) {
            assert_eq!((key, val), self.deleted_kv.borrow()[index]);
        }

        fn assert_get(&self, key: u32, want: u32) -> u32 {
            let h = self.cache.get(&key).unwrap();
            assert_eq!(want, h);
            h
        }
    }

    #[test]
    fn test_hit_and_miss() {
        let cache = CacheTest::new(CACHE_SIZE);
        assert_eq!(None, cache.get(100));
        cache.insert(100, 101);
        assert_eq!(Some(101), cache.get(100));
        assert_eq!(None, cache.get(200));
        assert_eq!(None, cache.get(300));

        cache.insert(200, 201);
        assert_eq!(Some(101), cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(None, cache.get(300));

        cache.insert(100, 102);
        assert_eq!(Some(102), cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(None, cache.get(300));

        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));
    }

    #[test]
    fn test_erase() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.erase(200);
        assert_eq!(0, cache.deleted_kv.borrow().len());

        cache.insert(100, 101);
        cache.insert(200, 201);
        cache.erase(100);

        assert_eq!(None, cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));

        cache.erase(100);
        assert_eq!(None, cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(1, cache.deleted_kv.borrow().len());
    }

    #[test]
    fn test_entries_are_pinned() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        let v1 = cache.assert_get(100, 101);
        assert_eq!(v1, 101);
        cache.insert(100, 102);
        let v2 = cache.assert_get(100, 102);
        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));
        assert_eq!(v1, 101);
        assert_eq!(v2, 102);

        cache.erase(100);
        assert_eq!(v1, 101);
        assert_eq!(v2, 102);
        assert_eq!(None, cache.get(100));
        assert_eq!(
            vec![(100, 101), (100, 102)],
            cache.deleted_kv.borrow().clone()
        );
    }

    #[test]
    fn test_eviction_policy() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        cache.insert(200, 201);
        cache.insert(300, 301);

        // frequently used entry must be kept around
        for i in 0..(CACHE_SIZE + 100) as u32 {
            cache.insert(1000 + i, 2000 + i);
            assert_eq!(Some(2000 + i), cache.get(1000 + i));
            assert_eq!(Some(101), cache.get(100));
        }
        assert_eq!(cache.cache.inner.lock().unwrap().table.len(), CACHE_SIZE);
        assert_eq!(Some(101), cache.get(100));
        assert_eq!(None, cache.get(200));
        assert_eq!(None, cache.get(300));
    }

    #[test]
    fn test_use_exceeds_cache_size() {
        let cache = CacheTest::new(CACHE_SIZE);
        let extra = 100;
        let total = CACHE_SIZE + extra;
        // overfill the cache, keeping handles on all inserted entries
        for i in 0..total as u32 {
            cache.insert(1000 + i, 2000 + i)
        }

        // check that all the entries can be found in the cache
        for i in 0..total as u32 {
            if i < extra as u32 {
                assert_eq!(None, cache.get(1000 + i))
            } else {
                assert_eq!(Some(2000 + i), cache.get(1000 + i))
            }
        }
    }

    #[test]
    fn test_heavy_entries() {
        let cache = CacheTest::new(CACHE_SIZE);
        let light = 1;
        let heavy = 10;
        let mut added = 0;
        let mut index = 0;
        while added < 2 * CACHE_SIZE {
            let weight = if index & 1 == 0 { light } else { heavy };
            cache.insert_with_charge(index, 1000 + index, weight);
            added += weight;
            index += 1;
        }
        let mut cache_weight = 0;
        for i in 0..index {
            let weight = if index & 1 == 0 { light } else { heavy };
            if let Some(val) = cache.get(i) {
                cache_weight += weight;
                assert_eq!(1000 + i, val);
            }
        }
        assert!(cache_weight < CACHE_SIZE);
    }

    #[test]
    fn test_zero_size_cache() {
        let cache = CacheTest::new(0);
        cache.insert(100, 101);
        assert_eq!(None, cache.get(100));
    }
}

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use bytes::Bytes;
use crate::iterator::Iter;
use rand::Rng;
use crate::IResult;
use crate::memtable::arena::Arena;
use crate::util::comparator::Comparator;

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Bytes,
    value: Bytes,
    height: usize,
    // The actual size will vary depending on the height that a node was allocated with.
    next_nodes: [AtomicU32; MAX_HEIGHT],
}

impl Node {
    fn alloc(key: Bytes, value: Bytes, height: usize, arena: &Arena) -> u32 {
        let align = std::mem::align_of::<Node>();
        let size = std::mem::size_of::<Node>();

        let not_used = (MAX_HEIGHT as usize - height as usize - 1) * std::mem::size_of::<AtomicU32>();
        let node_offset = arena.alloc_align(align, size - not_used);
        unsafe {
            let node_ptr: *mut Node = arena.get_ptr(node_offset);
            let node = &mut *node_ptr;
            std::ptr::write(&mut node.key, key);
            std::ptr::write(&mut node.value, value);
            node.height = height;
            std::ptr::write_bytes(node.next_nodes.as_mut_ptr(), 0, height + 1);
        }
        node_offset
    }

    /// Returns a raw pointer to this node at the specified level.
    fn next_offset(&self, height: usize) -> u32 {
        self.next_nodes[height].load(Ordering::SeqCst)
    }

    #[inline]
    fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    fn value(&self) -> &[u8] {
        &self.value
    }
}

struct SkiplistCore {
    height: AtomicUsize,
    head: NonNull<Node>,
    arena: Arena,
}

#[derive(Clone)]
pub struct Skiplist<C> {
    core: Arc<SkiplistCore>,
    pub c: C,
}

impl<C> Skiplist<C> {
    pub fn with_capacity(c: C, arena_size: usize) -> Skiplist<C> {
        let arena = Arena::with_capacity(arena_size);
        let head_offset = Node::alloc(Bytes::new(), Bytes::new(), MAX_HEIGHT - 1, &arena);
        let head = unsafe {
            NonNull::new_unchecked(arena.get_ptr::<Node>(head_offset))
        };
        Skiplist {
            core: Arc::new(SkiplistCore {
                height: AtomicUsize::new(0),
                head,
                arena,
            }),
            c,
        }
    }

    fn random_height(&self) -> usize {
        let mut rng = rand::thread_rng();
        for h in 0..(MAX_HEIGHT - 1) {
            if !rng.gen_ratio(HEIGHT_INCREASE, u32::MAX) {
                return h;
            }
        }
        MAX_HEIGHT - 1
    }

    #[inline]
    fn height(&self) -> usize {
        self.core.height.load(Ordering::SeqCst)
    }
}

impl<C: Comparator> Skiplist<C> {
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        if let Some((_, value)) = self.get_with_key(key) {
            Some(value)
        } else { None }
    }

    /// Returns the correspond value and key through the given key,
    /// false if not found.
    pub fn get_with_key(&self, key: &[u8]) -> Option<(&Bytes, &Bytes)> {
        let node = unsafe { self.find_near(key, false, true) };
        if !node.is_null() && self.c.compare(&unsafe { &*node }.key, key) == std::cmp::Ordering::Equal {
            return Some(unsafe {
                (&(*node).key, &(*node).value)
            });
        }
        None
    }

    /// Puts the key and value to skiplist.
    /// if the same key already exists, it will not be added again. Consider the follows situation:
    /// a).the value is same too, then directly return none.
    /// b).the value is difference, then return (key,value).
    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Option<(Bytes, Bytes)> {
        let (key, value) = (key.into(), value.into());
        let mut list_height = self.height();
        let mut prev = [std::ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [std::ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.core.head.as_ptr();
        next[list_height + 1] = std::ptr::null_mut();
        for i in (0..=list_height).rev() {
            // p is prev_node, n is next_node.
            let (p, n) = unsafe {
                self.find_splice_for_level(&key, prev[i + 1], i)
            };
            prev[i] = p;
            next[i] = n;
            if p == n {
                unsafe {
                    if (*p).value != value {
                        // todo whether logs a record that duplicate put the same key with different value.
                        return Some((key, value));
                    }
                }
                return None;
            }
        }

        let height = self.random_height();
        let node_offset = Node::alloc(key, value, height, &self.core.arena);
        while height > list_height {
            match self.core.height.compare_exchange_weak(
                list_height,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(h) => list_height = h,
            }
        }
        let x = unsafe { &mut *self.core.arena.get_ptr::<Node>(node_offset) };

        // We always insert from the base level and up. After you add a node in base level, we cannot
        // create a node in the level above because it would have discovered the node in the base level.
        for i in 0..=height {
            loop {
                if prev[i].is_null() {
                    assert!(i > 1);
                    // We haven't computed prev, next for this level because height exceeds old listHeight.
                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    let (p, n) = unsafe { self.find_splice_for_level(&x.key, self.core.head.as_ptr(), i) };

                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }

                let next_offset = self.core.arena.offset(next[i]);
                x.next_nodes[i].store(next_offset, Ordering::SeqCst);
                match unsafe { &*prev[i] }.next_nodes[i].compare_exchange(
                    next_offset,
                    node_offset,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(_) => {
                        // CAS failed. We need to recompute prev and next.
                        // It is unlikely to be helpful to try to use a different level as we redo the search,
                        // because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
                        let (p, n) = unsafe { self.find_splice_for_level(&x.key, prev[i], i) };
                        if p == n {
                            assert_eq!(i, 0);
                            if unsafe { &*p }.value != x.value {
                                let key = std::mem::replace(&mut x.key, Bytes::new());
                                let value = std::mem::replace(&mut x.value, Bytes::new());
                                return Some((key, value));
                            }
                            unsafe {
                                std::ptr::drop_in_place(x);
                            }
                            return None;
                        }
                        prev[i] = p;
                        next[i] = n;
                    }
                }
            }
        }
        None
    }

    /// Returns memory usage.
    #[inline]
    pub fn memory_usage(&self) -> usize {
        self.core.arena.memory_usage()
    }

    /// Returns true if skiplist is empty, otherwise return false.
    pub fn is_empty(&self) -> bool {
        let node = self.core.head.as_ptr();
        let next_offset = unsafe {
            (&*node).next_offset(0)
        };
        next_offset == 0
    }

    /// Returns the node count in skiplist.
    pub fn len(&self) -> usize {
        let mut node = self.core.head.as_ptr();
        let mut count = 0;
        loop {
            let next = unsafe {
                (&*node).next_offset(0)
            };
            if next != 0 {
                count += 1;
                node = unsafe { self.core.arena.get_ptr::<Node>(next) };
                continue;
            }
            return count;
        }
    }

    /// find_near that finds the node near to key.
    /// If less = true, it find rightmost node such that node.key < key (if allow_equal = false) or
    /// node.key <= key(if allow_equal = true).
    /// If less = false, it finds leftmost node such that node.key > key (if allow_equal = false) or
    /// node.key >= key(if allow_equal = true).
    unsafe fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> *const Node {
        let mut cursor: *const Node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            // Assume x.key < key
            let next_offset = (&*cursor).next_offset(level);
            if next_offset == 0 {
                // x.key < key < END OF LIST
                if level > 0 {
                    level -= 1;
                    continue;
                }

                // height = 0 or it's a head node.
                // head node because this level has no real node, so it's a head node.
                if !less || cursor == self.core.head.as_ptr() {
                    return std::ptr::null();
                }
                return cursor;
            }
            // This level has node, so we need to comparator node.key and input.key.

            // Notice `get_ptr` will advance the pointer
            let next_ptr = self.core.arena.get_ptr::<Node>(next_offset);
            let next_node = &*next_ptr;
            match self.c.compare(key, &next_node.key) {
                std::cmp::Ordering::Greater => {
                    // cursor.key < next.key < key. We can continue to move right.
                    cursor = next_ptr;
                    continue;
                }
                std::cmp::Ordering::Equal => {
                    // cursor.key < key == next.key.
                    if allow_equal {
                        return next_node;
                    }
                    // We want key < next.key, so go to base level to grab the next bigger note.
                    if !less {
                        let offset = next_node.next_offset(0);
                        if offset != 0 {
                            // The bigger one.
                            return self.core.arena.get_ptr::<Node>(offset);
                        } else {
                            return std::ptr::null();
                        }
                    }

                    // We want key > next.key. If not base level, we should go closer in the next level.
                    if level > 0 {
                        level -= 1;
                        continue;
                    }

                    // On base level. Return cursor.
                    // Try to return cursor. Make sure it is not a head node.
                    if cursor == self.core.head.as_ptr() {
                        return std::ptr::null();
                    }
                    return cursor;
                }
                std::cmp::Ordering::Less => {
                    // cursor.key < key < next.key.
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    // At base level. Need to return
                    if !less {
                        return next_node;
                    }

                    if cursor == self.core.head.as_ptr() {
                        return std::ptr::null();
                    }

                    return cursor;
                }
            }
        }
    }

    unsafe fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: usize,
    ) -> (*mut Node, *mut Node)
    {
        loop {
            let next_offset = (&*before).next_offset(level);
            if next_offset == 0 {
                return (before, std::ptr::null_mut());
            }
            let next_ptr = self.core.arena.get_ptr::<Node>(next_offset);
            let next_node = &*next_ptr;
            match self.c.compare(key, &next_node.key) {
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                std::cmp::Ordering::Less => return (before, next_ptr),
                _ => before = next_ptr,
            }
        }
    }

    fn find_last(&self) -> *const Node {
        let mut node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            let next = unsafe { (&*node).next_offset(level) };
            if next != 0 {
                node = unsafe { self.core.arena.get_ptr::<Node>(next) };
                continue;
            }
            if level == 0 {
                if node == self.core.head.as_ptr() {
                    return std::ptr::null();
                }
                return node;
            }
            level -= 1;
        }
    }
}

impl Drop for SkiplistCore {
    fn drop(&mut self) {
        let mut node = self.head.as_ptr();
        loop {
            let next = unsafe { (&*node).next_offset(0) };
            if next != 0 {
                let next_ptr = unsafe { self.arena.get_ptr::<Node>(next) };
                unsafe {
                    std::ptr::drop_in_place(node);
                }
                node = next_ptr;
                continue;
            }
            unsafe { std::ptr::drop_in_place(node) };
            return;
        }
    }
}

unsafe impl<C: Send> Send for Skiplist<C> {}

unsafe impl<C: Sync> Sync for Skiplist<C> {}

pub struct SkiplistIterator<C: Comparator> {
    list: Skiplist<C>,
    cursor: *const Node,
}

impl<C: Comparator> SkiplistIterator<C> {
    pub fn new(list: Skiplist<C>) -> Self {
        Self {
            list,
            cursor: std::ptr::null(),
        }
    }
}

impl<C: Comparator> Iter for SkiplistIterator<C> {
    fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { &(*self.cursor).key() }
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { &(*self.cursor).value() }
    }

    fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            let cursor_offset = (&*self.cursor).next_offset(0);
            self.cursor = self.list.core.arena.get_ptr::<Node>(cursor_offset);
        }
    }

    fn prev(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = self.list.find_near(self.key(), true, false);
        }
    }

    fn seek(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.find_near(target, false, true);
        }
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.find_near(target, true, true);
        }
    }

    fn seek_to_first(&mut self) {
        unsafe {
            let cursor_offset = (&*self.list.core.head.as_ptr()).next_offset(0);
            self.cursor = self.list.core.arena.get_ptr::<Node>(cursor_offset);
        }
    }

    fn seek_to_last(&mut self) {
        self.cursor = self.list.find_last();
    }

    // TODO Calculates whether the current memory is healthy.
    fn status(&mut self) -> IResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::time::Duration;
    use super::*;
    use std::thread;
    use crate::memtable::key::FixedLengthSuffixComparator;
    use crate::util::comparator::BytewiseComparator;

    fn new_value(v: usize) -> Bytes {
        Bytes::from(format!("{:05}", v))
    }

    fn key_with_ts(key: &str, ts: u64) -> Bytes {
        Bytes::from(format!("{}{:08}", key, ts))
    }

    #[test]
    fn test_find_near() {
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        for i in 0..1000 {
            let key = Bytes::from(format!("{:05}{:08}", i * 10 + 5, 0));
            let value = Bytes::from(format!("{:05}", i));
            list.put(key, value);
        }
        let mut cases = vec![
            ("00001", false, false, Some("00005")),
            ("00001", false, true, Some("00005")),
            ("00001", true, false, None),
            ("00001", true, true, None),
            ("00005", false, false, Some("00015")),
            ("00005", false, true, Some("00005")),
            ("00005", true, false, None),
            ("00005", true, true, Some("00005")),
            ("05555", false, false, Some("05565")),
            ("05555", false, true, Some("05555")),
            ("05555", true, false, Some("05545")),
            ("05555", true, true, Some("05555")),
            ("05558", false, false, Some("05565")),
            ("05558", false, true, Some("05565")),
            ("05558", true, false, Some("05555")),
            ("05558", true, true, Some("05555")),
            ("09995", false, false, None),
            ("09995", false, true, Some("09995")),
            ("09995", true, false, Some("09985")),
            ("09995", true, true, Some("09995")),
            ("59995", false, false, None),
            ("59995", false, true, None),
            ("59995", true, false, Some("09995")),
            ("59995", true, true, Some("09995")),
        ];
        for (i, (key, less, allow_equal, exp)) in cases.drain(..).enumerate() {
            let seek_key = Bytes::from(format!("{}{:08}", key, 0));
            let res = unsafe { list.find_near(&seek_key, less, allow_equal) };
            if exp.is_none() {
                assert!(res.is_null(), "{}", i);
                continue;
            }
            let e = format!("{}{:08}", exp.unwrap(), 0);
            assert_eq!(&unsafe { &*res }.key, e.as_bytes(), "{}", i);
        }
    }

    #[test]
    fn test_empty() {
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        let key = b"aaa";
        for less in &[false, true] {
            for allow_equal in &[false, true] {
                let node = unsafe { list.find_near(key, *less, *allow_equal) };
                assert!(node.is_null());
            }
        }

        let mut iter = SkiplistIterator::new(list.clone());
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
        iter.seek(key);
        assert!(!iter.valid());
    }

    #[test]
    fn test_basic() {
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        let table = vec![
            ("key1", new_value(42)),
            ("key2", new_value(52)),
            ("key3", new_value(62)),
            ("key5", Bytes::from(format!("{:0102400}", 1))),
            ("key4", new_value(72)),
        ];

        for (key, value) in &table {
            list.put(key_with_ts(*key, 0), value.clone());
        }

        assert_eq!(list.get(&key_with_ts("key", 0)), None);
        assert_eq!(list.len(), 5);
        assert!(!list.is_empty());
        for (key, value) in &table {
            let get_key = key_with_ts(*key, 0);
            assert_eq!(list.get(&get_key), Some(value), "{}", key);
        }
    }

    fn test_concurrent_basic(n: usize, cap: usize, value_len: usize) {
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, cap);
        let kvs: Vec<_> = (0..n)
            .map(|i| {
                (
                    key_with_ts(format!("{:05}", i).as_str(), 0),
                    Bytes::from(format!("{1:00$}", value_len, i)),
                )
            })
            .collect();
        let (tx, rx) = mpsc::channel();
        for (k, v) in kvs.clone() {
            let tx = tx.clone();
            let list = list.clone();
            thread::spawn(move || {
                list.put(k, v);
                tx.send(()).unwrap();
            });
        }
        for _ in 0..n {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        for (k, v) in kvs {
            let tx = tx.clone();
            let list = list.clone();
            thread::spawn(move || {
                let val = list.get(&k);
                assert_eq!(val, Some(&v), "{:?}", k);
                tx.send(()).unwrap();
            });
        }
        for _ in 0..n {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        assert_eq!(list.len(), n);
    }

    #[test]
    fn test_concurrent_basic_small_value() {
        test_concurrent_basic(1000, 1 << 20, 5);
    }

    #[test]
    fn test_concurrent_basic_big_value() {
        test_concurrent_basic(100, 120 << 20, 1048576);
    }

    #[test]
    fn test_one_key() {
        let n = 10000;
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        let key = key_with_ts("thekey", 0);
        let (tx, rx) = mpsc::channel();
        list.put(key.clone(), new_value(0));
        for i in 0..n {
            let tx = tx.clone();
            let list = list.clone();
            let key = key.clone();
            let value = new_value(i);
            thread::spawn(move || {
                list.put(key, value);
                tx.send("w").unwrap();
            });
        }
        let mark = Arc::new(AtomicBool::new(false));
        for _ in 0..n {
            let tx = tx.clone();
            let list = list.clone();
            let mark = mark.clone();
            let key = key.clone();
            thread::spawn(move || {
                let val = list.get(&key);
                if val.is_none() {
                    return;
                }
                let s = unsafe { std::str::from_utf8_unchecked(val.unwrap()) };
                let val: usize = s.parse().unwrap();
                assert!(val < n);
                mark.store(true, Ordering::SeqCst);
                tx.send("r").unwrap();
            });
        }
        let mut r = 0;
        let mut w = 0;
        for _ in 0..(n * 2) {
            match rx.recv_timeout(Duration::from_secs(3)) {
                Ok("w") => w += 1,
                Ok("r") => r += 1,
                Err(err) => panic!("timeout on receiving r{} w{} msg {:?}", r, w, err),
                _ => panic!("unexpected value"),
            }
        }
        assert_eq!(list.len(), 1);
        assert!(mark.load(Ordering::SeqCst));
    }

    #[test]
    fn test_iterator_next() {
        let n = 100;
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        let mut iter_ref = SkiplistIterator::new(list.clone());
        assert!(!iter_ref.valid());
        iter_ref.seek_to_first();
        assert!(!iter_ref.valid());
        for i in (0..n).rev() {
            let key = key_with_ts(format!("{:05}", i).as_str(), 0);
            list.put(key, new_value(i));
        }
        iter_ref.seek_to_first();
        for i in 0..n {
            assert!(iter_ref.valid());
            let v = iter_ref.value();
            assert_eq!(*v, new_value(i));
            iter_ref.next();
        }
        assert!(!iter_ref.valid());
    }

    #[test]
    fn test_iterator_prev() {
        let n = 100;
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        let mut iter_ref = SkiplistIterator::new(list.clone());
        assert!(!iter_ref.valid());
        iter_ref.seek_to_last();
        assert!(!iter_ref.valid());
        for i in (0..n).rev() {
            let key = key_with_ts(format!("{:05}", i).as_str(), 0);
            list.put(key, new_value(i));
        }
        iter_ref.seek_to_last();
        for i in (0..n).rev() {
            assert!(iter_ref.valid());
            let v = iter_ref.value();
            assert_eq!(*v, new_value(i));
            iter_ref.prev();
        }
        assert!(!iter_ref.valid());
    }

    #[test]
    fn test_iterator_seek() {
        let n = 100;
        let comp = BytewiseComparator::default();
        let list = Skiplist::with_capacity(comp, 1 << 20);
        let mut iter_ref = SkiplistIterator::new(list.clone());
        assert!(!iter_ref.valid());
        iter_ref.seek_to_first();
        assert!(!iter_ref.valid());
        for i in (0..n).rev() {
            let v = i * 10 + 1000;
            let key = key_with_ts(format!("{:05}", v).as_str(), 0);
            list.put(key, new_value(v));
        }
        iter_ref.seek_to_first();
        assert!(iter_ref.valid());
        assert_eq!(iter_ref.value(), b"01000" as &[u8]);

        let cases = vec![
            ("00000", Some(b"01000"), None),
            ("01000", Some(b"01000"), Some(b"01000")),
            ("01005", Some(b"01010"), Some(b"01000")),
            ("01010", Some(b"01010"), Some(b"01010")),
            ("99999", None, Some(b"01990")),
        ];
        for (key, seek_expect, for_prev_expect) in cases {
            let key = key_with_ts(key, 0);
            iter_ref.seek(&key);
            assert_eq!(iter_ref.valid(), seek_expect.is_some());
            if let Some(v) = seek_expect {
                assert_eq!(iter_ref.value(), &v[..]);
            }
            iter_ref.seek_for_prev(&key);
            assert_eq!(iter_ref.valid(), for_prev_expect.is_some());
            if let Some(v) = for_prev_expect {
                assert_eq!(iter_ref.value(), &v[..]);
            }
        }
    }
}



















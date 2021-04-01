use super::arena::Arena;
use crate::storage::engine::lsm::skl::{FixedLengthSuffixComparator, KeyComparator, MAX_HEIGHT};
use bytes::Bytes;
use rand::Rng;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

const HEIGHT_INCREASE: u32 = u32::MAX / 3;

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Bytes,
    value: Bytes,
    height: usize,
    tower: [AtomicU32; MAX_HEIGHT as usize],
}

impl Node {
    fn alloc(arena: &Arena, key: Bytes, value: Bytes, height: usize) -> u32 {
        let align = std::mem::align_of::<Node>();
        let size = std::mem::size_of::<Node>();
        let not_used =
            (MAX_HEIGHT as usize - height as usize - 1) * std::mem::size_of::<AtomicU32>();
        let node_offset = arena.alloc(align, size - not_used);
        unsafe {
            let node_ptr: *mut Node = arena.get_mut(node_offset);
            let node = &mut *node_ptr;
            std::ptr::write(&mut node.key, key);
            std::ptr::write(&mut node.value, value);
            node.height = height;
            std::ptr::write_bytes(node.tower.as_mut_ptr(), 0, height + 1);
        }
        node_offset
    }

    fn next_offset(&self, height: usize) -> u32 {
        self.tower[height].load(Ordering::SeqCst)
    }
}

struct SkipListCore {
    height: AtomicUsize,
    head: NonNull<Node>,
    arena: Arena,
}

#[derive(Clone)]
pub struct SkipList<C> {
    core: Arc<SkipListCore>,
    c: C,
}

impl<C> SkipList<C> {
    pub fn with_capacity(c: C, arena_size: u32) -> SkipList<C> {
        let arena = Arena::with_capacity(arena_size);
        let head_offset = Node::alloc(&arena, Bytes::new(), Bytes::new(), MAX_HEIGHT - 1);
        let head = unsafe { NonNull::new_unchecked(arena.get_mut(head_offset)) };
        SkipList {
            core: Arc::new(SkipListCore {
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

    fn height(&self) -> usize {
        self.core.height.load(Ordering::SeqCst)
    }
}

impl<C: KeyComparator> SkipList<C> {
    /// Finds the `node` near to key.
    /// If less = true, it finds rightmost node such that `node.key` < key (if allowEqual = false) or
    /// `node.key` <= key (if allowEqual = true).
    /// If less = false, it finds leftmost node such that `node.key` > key (if allowEqual = false) or
    /// `node.key` >= key (if allowEqual = true).
    unsafe fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> *const Node {
        let mut cursor: *const Node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            // Assume cursor.key < key.
            let next_offset = (&*cursor).next_offset(level);
            if next_offset == 0 {
                // cursor.key < key < END of list.
                if level > 0 {
                    // can descend further to iterate closer to the end.
                    level -= 1;
                    continue;
                }

                // level = 0, can't descend further, Make sure it is not a head node.
                if !less || cursor == self.core.head.as_ptr() {
                    return std::ptr::null();
                }
                return cursor;
            }

            let next_ptr: *mut Node = self.core.arena.get_mut(next_offset);
            let next = &*next_ptr;
            let res = self.c.compare_key(key, &next.key);
            if res == std::cmp::Ordering::Greater {
                // cursor.key < next.key < key. We can continue to move right.
                cursor = next_ptr;
                continue;
            }
            if res == std::cmp::Ordering::Equal {
                // cursor.key < key == next.key.
                if allow_equal {
                    return next;
                }
                if !less {
                    // We want > , so go to base level to grab the next bigger one.
                    let offset = next.next_offset(0);
                    if offset != 0 {
                        return self.core.arena.get_mut(offset);
                    } else {
                        return std::ptr::null();
                    }
                }

                // We want <. If not base level, we should go closer in the next level.
                if level > 0 {
                    level -= 1;
                    continue;
                }

                // On base level, return cursor
                if cursor == self.core.head.as_ptr() {
                    return std::ptr::null();
                }
                return cursor;
            }

            // cmp < 0. In other words, cursor.key < key < next.
            if level > 0 {
                level -= 1;
                continue;
            }

            // At base level, Need to return something.
            if !less {
                return next;
            }

            // Try to return cursor, Make sure it is not a head node.
            if cursor == self.core.head.as_ptr() {
                return std::ptr::null();
            }
            return cursor;
        }
    }

    /// Returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
    /// The input "before" tells us where to start looking
    /// If we found a `node` with the same key, then we return outBefore = outAfter.
    /// Otherwise, outBefore.key < key < outAfter.key
    unsafe fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: usize,
    ) -> (*mut Node, *mut Node) {
        loop {
            // Assume before.key < key
            let next_offset = (&*before).next_offset(level);
            if next_offset == 0 {
                return (before, std::ptr::null_mut());
            }
            let next_ptr: *mut Node = self.core.arena.get_mut(next_offset);
            let next_node = &*next_ptr;
            match self.c.compare_key(&key, &next_node.key) {
                // Equality case.
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                // before.key < key < next.key. We are done for this level
                std::cmp::Ordering::Less => return (before, next_ptr),
                // Keep moving right on this level.
                _ => before = next_ptr,
            }
        }
    }

    /// Inserts the k-v pair.
    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Option<(Bytes, Bytes)> {
        // Since we allow overwrite, we may not need to create a new node. We might not even need to
        // increase the height. Let's defer these actions.

        let (key, value) = (key.into(), value.into());
        let mut list_height = self.height();
        let mut prev = [std::ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [std::ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.core.head.as_ptr();
        next[list_height + 1] = std::ptr::null_mut();
        for i in (0..=list_height).rev() {
            // Use higher level to speed up for current level.
            let (p, n) = unsafe { self.find_splice_for_level(&key, prev[i + 1], i) };
            prev[i] = p;
            next[i] = n;
            if p == n {
                unsafe {
                    if (*p).value != value {
                        return Some((key, value));
                    }
                }
                return None;
            }
        }

        // We do need to create a new node.
        let height = self.random_height();
        let node_offset = Node::alloc(&self.core.arena, key, value, height);
        while height > list_height {
            // Try to increase s.height via CAS.
            match self.core.height.compare_exchange_weak(
                list_height,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                // Successfully increased skiplist.height.
                Ok(_) => break,
                Err(h) => list_height = h,
            }
        }
        let x: &mut Node = unsafe { &mut *self.core.arena.get_mut(node_offset) };
        // We always insert from the base level and up. After you add a node in base level, we cannot
        // create a node in the level above because it would have discovered the node in the base level.
        for i in 0..=height {
            loop {
                if prev[i].is_null() {
                    assert!(i > 1); // This cannot happen in base level.

                    // We haven't computed prev, next for this level because height exceeds old listHeight.
                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    let (p, n) =
                        unsafe { self.find_splice_for_level(&x.key, self.core.head.as_ptr(), i) };
                    prev[i] = p;
                    next[i] = n;
                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    assert_ne!(p, n);
                }
                let next_offset = self.core.arena.offset(next[i]);
                x.tower[i].store(next_offset, Ordering::SeqCst);
                // Managed to insert x between prev[i] and next[i]. Go to the next level.
                match unsafe { &*prev[i] }.tower[i].compare_exchange(
                    next_offset,
                    node_offset,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    // CAS failed. We need to recompute prev and next.
                    // It is unlikely to be helpful to try to use a different level as we redo the search,
                    // because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
                    Err(_) => {
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

    pub fn is_empty(&self) -> bool {
        let node = self.core.head.as_ptr();
        let next_offset = unsafe { (&*node).next_offset(0) };
        next_offset == 0
    }

    pub fn len(&self) -> usize {
        let mut node = self.core.head.as_ptr();
        let mut count = 0;
        loop {
            let next = unsafe { (&*node).next_offset(0) };
            if next != 0 {
                count += 1;
                node = unsafe { self.core.arena.get_mut(next) };
                continue;
            }
            return count;
        }
    }

    /// Returns the last element. If head (empty list), we return null. All the find functions
    /// will NEVER return the head nodes.
    fn find_last(&self) -> *const Node {
        let mut node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            let next = unsafe { (&*node).next_offset(level) };
            if next != 0 {
                node = unsafe { self.core.arena.get_mut(next) };
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

    /// Gets the value associated with the key. It returns a valid value if it finds equal or earlier
    /// version of the same key.
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        if let Some((_, value)) = self.get_with_key(key) {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_with_key(&self, key: &[u8]) -> Option<(&Bytes, &Bytes)> {
        let node = unsafe { self.find_near(key, false, true) };
        if node.is_null() {
            return None;
        }
        if self.c.same_key(&unsafe { &*node }.key, key) {
            return Some(unsafe { (&(*node).key, &(*node).value) });
        }
        None
    }

    pub fn iter_ref<'a>(&'a self) -> IterRef<&'a SkipList<C>, C> {
        IterRef {
            list: self,
            cursor: std::ptr::null(),
            _key_cmp: std::marker::PhantomData,
        }
    }

    pub fn iter(&self) -> IterRef<SkipList<C>, C> {
        IterRef {
            list: self.clone(),
            cursor: std::ptr::null(),
            _key_cmp: std::marker::PhantomData,
        }
    }

    pub fn mem_size(&self) -> u32 {
        self.core.arena.len()
    }
}

impl<C> AsRef<SkipList<C>> for SkipList<C> {
    fn as_ref(&self) -> &SkipList<C> {
        self
    }
}

impl Drop for SkipListCore {
    fn drop(&mut self) {
        let mut node = self.head.as_ptr();
        loop {
            let next = unsafe { (&*node).next_offset(0) };
            if next != 0 {
                let next_ptr = unsafe { self.arena.get_mut(next) };
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

unsafe impl<C: Send> Send for SkipList<C> {}

unsafe impl<C: Sync> Sync for SkipList<C> {}

pub struct IterRef<T, C>
where
    T: AsRef<SkipList<C>>,
{
    list: T,
    cursor: *const Node,
    _key_cmp: std::marker::PhantomData<C>,
}

impl<T: AsRef<SkipList<C>>, C: KeyComparator> IterRef<T, C> {
    #[inline]
    pub fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    #[inline]
    pub fn key(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.cursor).key }
    }

    #[inline]
    pub fn value(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.cursor).value }
    }

    #[inline]
    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            let cursor_offset = (&*self.cursor).next_offset(0);
            self.cursor = self.list.as_ref().core.arena.get_mut(cursor_offset);
        }
    }

    #[inline]
    pub fn prev(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = self.list.as_ref().find_near(self.key(), true, false);
        }
    }

    #[inline]
    pub fn seek(&mut self, target: &[u8]) {
        unsafe { self.cursor = self.list.as_ref().find_near(target, false, true) }
    }

    #[inline]
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.as_ref().find_near(target, true, true);
        }
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        unsafe {
            let cursor_offset = (&*self.list.as_ref().core.head.as_ptr()).next_offset(0);
            self.cursor = self.list.as_ref().core.arena.get_mut(cursor_offset);
        }
    }

    #[inline]
    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.as_ref().find_last();
    }
}

#[test]
fn test_find_near() {
    let comp = FixedLengthSuffixComparator::new(8);
    let list = SkipList::with_capacity(comp, 1 << 20);
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

use core::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

static BLOCK_SIZE: usize = 4096;

pub struct Arena {
    core: Arc<ArenaCore>,
}

struct ArenaCore {
    ptr: *mut u8,
    len: AtomicUsize,
    cap: usize,
}

impl Drop for ArenaCore {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.ptr as *mut u64;
            let cap = self.cap / 8;
            Vec::from_raw_parts(ptr, 0, cap);
        }
    }
}

impl Arena {
    pub fn with_capacity(capacity: u32) -> Arena {
        let mut buf: Vec<u64> = Vec::with_capacity(capacity as usize / 8);
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * 8;
        mem::forget(buf);
        Arena {
            core: Arc::new(ArenaCore {
                len: AtomicUsize::new(1),
                cap,
                ptr,
            }),
        }
    }

    pub fn alloc(&self, size: usize) -> u32 {
        assert!(size > 0);
        let offset = self.core.len.fetch_add(size, Ordering::SeqCst);
        assert!(offset as usize <= self.core.cap);
        offset as u32
    }

    /// Allocates `size` bytes aligned with `align`
    pub fn alloc_align(&self, align: usize, size: usize) -> u32 {
        let mut align = align;
        if align >= 8 {
            align = 8;
        }
        let align_mark = align - 1;

        let size = size + align_mark;
        let offset = self.core.len.fetch_add(size, Ordering::SeqCst);
        // Calculate the bit to fill.  19 & -8 = 24;
        // (offset + align_mark) / align * align
        let ptr_offset = (offset + align_mark) & !align_mark;
        assert!(offset as usize + size <= self.core.cap);
        ptr_offset as u32
    }

    /// Returns a raw pointer with given arena offset
    pub unsafe fn get_ptr<N>(&self, offset: u32) -> *mut N {
        if offset == 0 {
            return std::ptr::null_mut();
        }
        self.core.ptr.add(offset as usize) as _
    }

    #[inline]
    pub fn memory_used(&self) -> usize {
        self.core.len.load(Ordering::SeqCst)
    }

    pub fn offset<N>(&self, ptr: *const N) -> u32 {
        let ptr_addr = ptr as usize;
        let self_addr = self.core.ptr as usize;
        if ptr_addr > self_addr && ptr_addr < self_addr + self.core.cap {
            (ptr_addr - self_addr) as u32
        } else {
            0
        }
    }
}

#[cfg(test)]
mod arena_test {
    use std::sync::atomic::{AtomicU32, Ordering};
    use crate::memtable::arena::Arena;

    struct Node {
        v: u64,
        p: u64,
    }

    #[test]
    fn alloc() {
        let v = std::mem::size_of::<Node>();
        let arena = Arena::with_capacity(1 << 20);
        let s = arena.alloc(v);
        assert_eq!(s, 1);
        assert_eq!(arena.memory_used(), 1 + 16);
        let s = arena.alloc(v);
        assert_eq!(s, 1 + 16);
        assert_eq!(arena.memory_used(), 1 + 16 + 16);
    }

    #[test]
    fn alloc_align() {
        // 0001 00111  30

        // 0000 1000   8
        // 1111 0111   8 的补码
        // 1111 1000   8 的反码

        println!("{}", (23 + 7) & -8);
        println!("{}", 30 / 8 * 8);
        let align = std::mem::align_of::<Node>();
        let v = std::mem::size_of::<Node>();
        let arena = Arena::with_capacity(1 << 20);
        let s = arena.alloc_align(align, v);
        assert_eq!(s, 8);
        assert_eq!(arena.memory_used(), 1 + 7 + 16);
        let s = arena.alloc_align(align, v);
        assert_eq!(s, 24);
        assert_eq!(arena.memory_used(), 1 + 7 + 16 + 7 + 16);
    }
}
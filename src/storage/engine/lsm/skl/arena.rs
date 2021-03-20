use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

struct ArenaCore {
    len: AtomicU32,
    cap: usize,
    ptr: *mut u8,
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

pub struct Arena {
    core: Arc<ArenaCore>,
}

impl Arena {
    pub fn with_capacity(cap: u32) -> Self {
        let mut buf: Vec<u64> = Vec::with_capacity(cap as usize / 8);
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * 8;
        std::mem::forget(buf);
        Self {
            core: Arc::new(ArenaCore {
                len: AtomicU32::new(1);
                cap,
                ptr,
            })
        }
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.core.len.load(Ordering::SeqCst)
    }

    pub unsafe fn get_mut<N>(&self, offset: u32) -> *mut N {
        if offset == 0 {
            return std::ptr::null_mut();
        }
        self.core.ptr.add(offset as usize) as _
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
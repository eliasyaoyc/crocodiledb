use std::sync::atomic::{AtomicU32, Ordering};
use std::marker::PhantomData;
use std::sync::Arc;

struct ArenaInner {
    len: AtomicU32,
    cap: usize,
    ptr: PhantomData<u8>,
}

impl Drop for ArenaInner {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.ptr as *mut u64;
            let cap = self.cap / 8;
            Vec::from_raw_parts(ptr, 0, cap);
        }
    }
}

pub struct Arena {
    inner: Arc<ArenaInner>,
}

impl Arena {
    pub fn new() -> Arena {
        Arena::with_capacity(0)
    }

    pub fn with_capacity(cap: u32) -> Arena {}


    pub fn alloc() -> u32 {}

    pub unsafe fn get_mut<N>(&self, offset: u32) -> *mut N {}

    pub fn offset<N>(&self, ptr: *const N) -> u32 {}

    pub fn len(&self) -> u32 {
        self.inner.len.load(Ordering::SeqCst)
    }
}
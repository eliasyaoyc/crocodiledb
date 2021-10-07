use std::cmp::Ordering;
use std::sync::Arc;

pub trait KeyComparator: Clone {}


pub struct Skiplist {
    inner: Arc<SkiplistInner>,
}

struct SkiplistInner {}

struct Node {}


#[cfg(test)]
mod skiplist_test {

}
mod builder;
mod concat_iterator;
mod iterator;
mod merge_iterator;

use std::sync::Arc;

pub struct TableInner {}

pub struct Table {
    inner: Arc<TableInner>,
}

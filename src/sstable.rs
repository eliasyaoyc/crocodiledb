use std::sync::Arc;

mod builder;

pub struct Table {
    inner: Arc<TableInner>,
}

struct TableInner {}
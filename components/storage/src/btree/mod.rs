use crate::{Range, Scan, Storage};
use crocodiledb_components_error::error::{Error, Result};
use std::fmt::{Display, Formatter};
pub use tracing::{debug, error, info, warn};

pub enum NodeType {
    RootNode,
    InternalNode,
    LeafNode,
}

pub struct BTree {}

impl Display for BTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "btree")
    }
}

impl Storage for BTree {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }

    fn scan(&self, range: Range) -> Scan {
        todo!()
    }
}

impl BTree {
    fn merge(&self) -> Result<()> {
        todo!()
    }
    fn split(&self) -> Result<()> {
        todo!()
    }

    fn rebalance(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_work() {
        println!("{}", 123);
    }
}

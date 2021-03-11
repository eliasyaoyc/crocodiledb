use crate::storage::Storage;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// The status of mvcc
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

pub struct MVCC {
    storage: Arc<RwLock<Box<dyn Storage>>>,
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl MVCC {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(storage: Arc<RwLock<Box<dyn Storage>>>) -> Self {
        MVCC { storage }
    }
}

use crate::config::Config;
use crate::storage::{Storage, StorageFactory};

pub struct KV {
    pub storage: Box<dyn Storage>,
}

impl KV {
    pub fn new(config: Config) -> Self {
        let storage = StorageFactory::get_storage(config);
        Self { storage }
    }

    async fn open() {}

    async fn get(key: &[u8]) -> Vec<u8> {
        vec![]
    }

    async fn put( key: &[u8], value: &[u8]) {}

    async fn delete(key: &[u8]) -> Vec<u8> {
        vec![]
    }
}
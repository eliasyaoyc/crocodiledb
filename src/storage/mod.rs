use crate::config::Config;
use crate::storage::lsm_storage::LSM;

mod lsm_storage;
mod bloom_filter;

pub trait Storage {
    fn open(&self) {}

    fn get(&self, key: &[u8]) -> Vec<u8> {
        vec![]
    }

    fn put(&self, key: &[u8], value: &[u8]) {}

    fn delete(&self, key: &[u8]) -> Vec<u8> {
        vec![]
    }
}


pub struct StorageFactory {}

impl StorageFactory {
    pub fn get_storage(config: Config) -> Box<dyn Storage> {
        Box::new(LSM::new())
    }
}
use crate::storage::Storage;

pub struct LSM {}

impl Storage for LSM {
    fn open(&self) {}

    fn get(&self, key: &[u8]) -> Vec<u8> {
        vec![]
    }

    fn put(&self, key: &[u8], value: &[u8]) {}

    fn delete(&self, key: &[u8]) -> Vec<u8> {
        vec![]
    }
}

impl LSM {
    pub fn new() -> Self {
        Self {}
    }
}
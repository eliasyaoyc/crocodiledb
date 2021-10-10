pub mod format;

use crate::opt::Options;

pub struct KV {
    // pub storage: Box<dyn Storage>,
}

impl KV {
    pub fn new(opt: Options) -> Self {
        Self {}
    }

    async fn open() {}

    async fn get(key: &[u8]) -> Vec<u8> {
        vec![]
    }

    async fn put(key: &[u8], value: &[u8]) {}

    async fn delete(key: &[u8]) -> Vec<u8> {
        vec![]
    }
}
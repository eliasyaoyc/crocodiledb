use crate::filter::FilterPolicy;

pub struct CuckooFilter {}

impl CuckooFilter {
    pub fn new() -> Self {
        CuckooFilter {}
    }
}

impl FilterPolicy for CuckooFilter {
    fn name(&self) -> &str {
        "CuckooFilter"
    }

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        todo!()
    }

    fn key_may_match(&self, filter: &[u8], key: &[u8]) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
use std::sync::Arc;
use crate::filter::FilterPolicy;

pub struct FilterBlockBuilder {
    policy: Arc<dyn FilterPolicy>,
    keys: Vec<u8>,
    start: Vec<usize>,

}

pub struct FilterBlock{

}
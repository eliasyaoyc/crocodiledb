use crossbeam_channel::Sender;
use crate::storage::error::{Error, Result};

/// Information for a manual compaction
#[derive(Clone)]
pub struct ManualCompaction {
    pub level: usize,
    pub done: Sender<Result<()>>,
    pub begin: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}
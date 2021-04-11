use crate::storage::error::{Error, Result};
use crossbeam_channel::{Receiver, Sender};
use std::sync::Condvar;
use std::thread;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub enum CompactionTyp {
    manual,
    auto,
}

/// Information for a manual compaction
#[derive(Clone)]
pub struct Compaction {
    pub level: usize,
    pub compact: Sender<()>,
    pub begin: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub typ: CompactionTyp,
    // todo metrics.
}

impl Compaction {
    pub fn new(compact: Sender<()>) -> Self {
        Self {
            level: 0,
            compact,
            begin: None,
            end: None,
            typ: CompactionTyp::manual, // default is manual.
        }
    }

    pub fn process_compaction(&self, done_tx: Sender<()>) {
        thread::Builder::new()
            .name("main_compaction".to_owned())
            .spawn(move || {
                let cond = Condvar::new();
            })
            .unwrap();
    }
}

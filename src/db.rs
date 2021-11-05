use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, Sender};
use crate::batch::WriteBatch;
use crate::db::filename::{FileType, generate_filename};
use crate::db::format::{InternalKey, InternalKeyComparator};
use crate::IResult;
use crate::iterator::Iter;
use crate::opt::{Options, ReadOptions, WriteOptions};
use crate::sstable::TableBuilder;
use crate::storage::Storage;
use crate::table_cache::TableCache;
use crate::util::comparator::Comparator;
use crate::version::version_edit::FileMetaData;

pub mod format;
pub mod filename;
pub mod iterator;

/// A `DB` is a persistent ordered map from keys to values.
/// A `DB` is safe for concurrent access from multiple threads without
/// any external synchronization.
pub trait DB {
    /// The iterator that can yield all the kv pairs in `DB`.
    type Iter;

    /// Sets the value for the given key. It overwrites any previous value
    /// for that key; a DB is not a multi-map
    fn put(&self, write_opt: WriteOptions, key: &[u8], value: &[u8]) -> IResult<()>;
}

/// The wrapper of `DBImpl` for concurrency control.
/// `CrocodileDB` is thread safe and is able to be shared by `clone()` in different thread.
pub struct CrocodileDB<S: Storage + Clone + 'static, C: Comparator> {
    inner: Arc<DBImpl<S, C>>,
    shutdown_batch_processing_thread: (Sender<()>, Receiver<()>),
    shutdown_compaction_thread: (Sender<()>, Receiver<()>),
}

pub struct DBImpl<S: Storage + Clone, C: Comparator> {
    env: S,
    internal_compactor: InternalKeyComparator<C>,
    options: Arc<Options<C>>,
    db_path: String,
    db_lock: Option<S::F>,

    // Write batch scheduling.
    batch_queue: Mutex<VecDeque<BatchTask>>,

    // Whether the db is closing.
    is_shutting_down: AtomicBool,
}

/// A wrapper struct for scheduling `WriteBatch`.
struct BatchTask {
    // Flag for shutdown the batch processing thread gracefully.
    stop_process: bool,
    force_mem_compaction: bool,
    batch: WriteBatch,
    signal: Sender<IResult<()>>,
    options: WriteOptions,
}

// Build a Table file from the contents of `iter`.  The generated file
// will be named according to `meta.number`.  On success, the rest of
// meta will be filled with metadata about the generated table.
// If no data is present in iter, `meta.file_size` will be set to
// zero, and no Table file will be produced.
pub(crate) fn build_table<S: Storage + Clone, C: Comparator + 'static>(
    options: Arc<Options<C>>,
    storage: &S,
    db_path: &str,
    table_cache: &TableCache<S, C>,
    iter: &mut dyn Iter,
    meta: &mut FileMetaData,
) -> IResult<()> {
    meta.file_size = 0;
    iter.seek_to_first();
    let file_name = generate_filename(db_path, FileType::Table, meta.number);
    let mut status = Ok(());
    if iter.valid() {
        let file = storage.create(file_name.as_str())?;
        let icmp = InternalKeyComparator::new(options.comparator.clone());
        let mut builder = TableBuilder::new(file, icmp.clone(), &options);
        let mut prev_key = vec![];
        meta.smallest = InternalKey::decoded_from(iter.key());
        while iter.valid() {
            let key = iter.key().to_vec();
            let s = builder.add(&key, iter.value());
            if s.is_err() {
                status = s;
                break;
            }
            prev_key = key;
            iter.next();
        }
        if !prev_key.is_empty() {
            meta.largest = InternalKey::decoded_from(&prev_key);
        }
        if status.is_ok() {
            status = builder.finish(true).and_then(|_| {
                meta.file_size = builder.file_size();
                assert!(meta.file_size > 0);
                // make sure that the new file is in the cache
                let mut it = table_cache.new_iter(
                    icmp,
                    ReadOptions::default(),
                    meta.number,
                    meta.file_size,
                )?;
                it.status()
            });
        }
    }
    let iter_status = iter.status();
    if iter_status.is_err() {
        status = iter_status;
    };
    if status.is_err() || meta.file_size == 0 {
        storage.remove(file_name.as_str())?;
        status
    } else {
        Ok(())
    }
}
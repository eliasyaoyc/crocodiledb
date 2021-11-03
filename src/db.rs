use std::sync::Arc;
use crate::db::filename::{FileType, generate_filename};
use crate::db::format::{InternalKey, InternalKeyComparator};
use crate::IResult;
use crate::iterator::Iter;
use crate::opt::{Options, ReadOptions};
use crate::sstable::TableBuilder;
use crate::storage::Storage;
use crate::table_cache::TableCache;
use crate::util::comparator::Comparator;
use crate::version::version_edit::FileMetaData;

pub mod format;
pub mod filename;
pub mod iterator;


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
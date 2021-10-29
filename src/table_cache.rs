use std::sync::Arc;
use crate::cache::Cache;
use crate::cache::lru_cache::LRUCache;
use crate::db::filename::{FileType, generate_filename};
use crate::IResult;
use crate::opt::{Options, ReadOptions};
use crate::sstable::block::BlockIterator;
use crate::sstable::{new_table_iterator, Table, TableIterator};
use crate::storage::Storage;
use crate::util::comparator::Comparator;

/// A `TableCache` is the cache for the sst files and the sstable in them.
pub struct TableCache<S: Storage + Clone, C: Comparator> {
    storage: S,
    cmp: C,
    db_name: String,
    options: Options,
    cache: Arc<dyn Cache<u64, Arc<Table<S::F>>>>,
}

impl<S: Storage + Clone, C: Comparator + 'static> TableCache<S, C> {
    pub fn new(db_name: String, cmp: C, options: Options, size: usize, storage: S) -> Self {
        let cache = Arc::new(LRUCache::<u64, Arc<Table<S::F>>>::new(size));
        Self {
            storage,
            cmp,
            db_name,
            options,
            cache,
        }
    }

    /// Try to find the sst file from cache. If not found, try to find the file from storage and insert it into the cache.
    pub fn find_table<TC: Comparator>(
        &self,
        cmp: TC,
        file_number: u64,
        file_size: u64,
    ) -> IResult<Arc<Table<S::F>>> {
        match self.cache.get(&file_number) {
            Some(v) => Ok(v),
            None => {
                let filename = generate_filename(&self.db_name, FileType::Table, file_number);
                let table_file = self.storage.open(&filename)?;
                let table = Table::open(
                    table_file,
                    file_number,
                    file_size,
                    self.options.clone(),
                    cmp,
                )?;
                let value = Arc::new(table);
                let _ = self.cache.insert(file_number, value.clone(), 1);
                Ok(value)
            }
        }
    }

    /// Evict any entry for the specified file number.
    pub fn evict(&self, file_number: u64) {
        self.cache.erase(&file_number);
    }

    /// Returns the result of a seek to internal key `key` in specified file.
    pub fn get<TC: Comparator>(
        &self,
        cmp: TC,
        options: ReadOptions,
        key: &[u8],
        file_number: u64,
        file_size: u64,
    ) -> IResult<Option<BlockIterator<TC>>>
    {
        let table = self.find_table(cmp.clone(), file_number, file_size)?;
        table.internal_get(options, cmp, key)
    }
    /// Create an iterator for the specified `file_number` (the corresponding
    /// file length must be exactly `file_size` bytes).
    /// The table referenced by returning Iterator will be released after the Iterator is dropped.
    ///
    /// Entry format:
    ///     key: internal key
    ///     value: value of user key
    pub fn new_iter<TC: Comparator>(
        &self,
        cmp: TC,
        options: ReadOptions,
        file_number: u64,
        file_size: u64,
    ) -> IResult<TableIterator<TC, S::F>> {
        let t = self.find_table(cmp.clone(), file_number, file_size)?;
        let iter = new_table_iterator(cmp, t, options);
        Ok(iter)
    }
}

impl<S: Storage + Clone, C: Comparator> Clone for TableCache<S, C> {
    fn clone(&self) -> Self {
        TableCache {
            storage: self.storage.clone(),
            cmp: self.cmp.clone(),
            db_name: self.db_name.clone(),
            options: self.options.clone(),
            cache: self.cache.clone(),
        }
    }
}
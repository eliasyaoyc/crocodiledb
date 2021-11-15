use std::any::TypeId;
use std::collections::VecDeque;
use std::fs::read;
use std::net::Shutdown::Write;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;
use log::Level::Error;
use test::RunIgnored::No;
use crate::batch::{HEADER_SIZE, WriteBatch};
use crate::compaction::{Compaction, ManualCompaction};
use crate::db::filename::{FileType, generate_filename, parse_filename, update_current};
use crate::db::format::{InternalKey, InternalKeyComparator, LookupKey, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK, ValueType};
use crate::db::iterator::{DBIterator, DBIteratorCore};
use crate::error::Error;
use crate::IResult;
use crate::iterator::{Iter, KMergeIter};
use crate::memtable::{MemTable, MemTableIterator};
use crate::opt::{Options, ReadOptions, WriteOptions};
use crate::snapshot::Snapshot;
use crate::sstable::{SSTableIters, TableBuilder};
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::comparator::Comparator;
use crate::util::reporter::LogReporter;
use crate::version::Version;
use crate::version::version_edit::{FileMetaData, VersionEdit};
use crate::version::version_set::VersionSet;
use crate::wal::reader::Reader;
use crate::wal::writer::Writer;

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

    /// Gets the value for the given key. It returns `None` if the DB dose not contain
    /// the key.
    fn get(&self, write_opt: WriteOptions, key: &[u8], value: &[u8]) -> IResult<()>;

    /// Deletes the value for the given key. It returns `Error::NotFound` if
    /// the DB dose not contain the key.
    fn delete(&self, write_opt: WriteOptions, key: &[u8]) -> IResult<()>;

    /// Applies the operations contained in the `WriteBatch` to the DB atomically.
    /// the DB dose not contain the key.
    fn write(&self, write_opt: WriteOptions, batch: WriteBatch) -> IResult<()>;

    /// Shutdown the current DB by waiting util all the background tasks are complete
    /// and then release the file lock. A closed db should never be used again and is able to
    /// be dropped safely.
    fn close(&self) -> IResult<()>;

    /// Shutdown the current DB and delete all relative file and the db directory.
    fn destroy(&self) -> IResult<()>;

    /// Acquire a snapshot for reading DB.
    fn snapshot(&self) -> Arc<Snapshot>;

    /// Return an iterator over the contents of the database.
    fn iter(&self, read_opt: ReadOptions) -> IResult<Self::iter()>;
}

/// The wrapper of `DBImpl` for concurrency control.
/// `CrocodileDB` is thread safe and is able to be shared by `clone()` in different thread.
pub struct CrocodileDB<S: Storage + Clone + 'static, C: Comparator> {
    inner: Arc<DBImpl<S, C>>,
    shutdown_batch_processing_thread: (Sender<()>, Receiver<()>),
    shutdown_compaction_thread: (Sender<()>, Receiver<()>),
}

/// The iterator yields all the user keys and user values in db.
pub type CrocodileDBIterator<S, C> = DBIterator<InternalIterator<S, C>, S, C>;

/// The iterator yields all the internal keys and internal values in db.
type InternalIterator<S, C> = KMergeIter<
    DBIteratorCore<InternalKeyComparator<C>, MemTableIterator<C>, KMergeIter<SSTableIters<S, C>>>
>;

impl<S: Storage + Clone, C: Comparator + 'static> DB for CrocodileDB<S, C> {
    type Iter = CrocodileDBIterator<S, C>;

    fn put(&self, write_opt: WriteOptions, key: &[u8], value: &[u8]) -> IResult<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.write(write_opt, batch)
    }

    fn get(&self, write_opt: WriteOptions, key: &[u8], value: &[u8]) -> IResult<()> {
        todo!()
    }

    fn delete(&self, write_opt: WriteOptions, key: &[u8]) -> IResult<()> {
        todo!()
    }

    fn write(&self, write_opt: WriteOptions, batch: WriteBatch) -> IResult<()> {
        todo!()
    }

    fn close(&self) -> IResult<()> {
        todo!()
    }

    fn destroy(&self) -> IResult<()> {
        todo!()
    }

    fn snapshot(&self) -> Arc<Snapshot> {
        todo!()
    }

    fn iter(&self, read_opt: ReadOptions) -> IResult<Self::iter()> {
        todo!()
    }
}

impl<S: Storage + Clone, C: Comparator + 'static> CrocodileDB<S, C> {}

pub struct DBImpl<S: Storage + Clone, C: Comparator> {
    env: S,
    internal_compactor: InternalKeyComparator<C>,
    options: Arc<Options<C>>,
    db_path: String,
    db_lock: Option<S::F>,

    // Write batch scheduling.
    batch_queue: Mutex<VecDeque<BatchTask>>,
    process_batch_sem: Condvar,

    // The table cache.
    table_cache: TableCache<S, C>,

    // The version set.
    versions: Mutex<VersionSet<S, C>>,

    // The queue for manual compaction.
    // All the compaction will be executed one by one once compaction is triggered.
    manual_compaction_queue: Mutex<VecDeque<ManualCompaction>>,

    // Signal whether the compaction finished.
    background_work_finished_signal: Condvar,

    // Whether we have scheduled and running a compaction.
    background_compaction_scheduled: AtomicBool,

    // Signal of schedule a compaction.
    do_compaction: (Sender<()>, Receiver<()>),

    // Though memtable is thread safe with multiple readers and single writers
    // and all relative methods are using immutable borrowing,
    // we still need to mutate the field `mem` and `im_mem` in some situations.
    mem: ShardedLock<MemTable<C>>,

    // There is a compacted immutable table or not.
    im_mem: ShardedLock<Option<MemTable<C>>>,

    // Have we encountered a background error in paranoid mode.
    bg_error: RwLock<Option<Error>>,

    // Whether the db is closing.
    is_shutting_down: AtomicBool,
}

impl<S: Storage + Clone, C: Comparator> Drop for DBImpl<S, C> {
    fn drop(&mut self) {
        if !self.is_shutting_down.load(Ordering::Acquire) {
            let _ = self.close();
        }
    }
}

impl<S: Storage + Clone, C: Comparator> DBImpl<S, C> {
    fn close(&self) -> IResult<()> {
        self.is_shutting_down.store(true, Ordering::Release);
        match &self.db_lock {
            Some(lock) => lock.unlock_file(),
            None => Ok(()),
        }
    }
}

impl<S: Storage + Clone + 'static, C: Comparator + 'static> DBImpl<S, C> {
    fn new(options: Options<C>, db_path: String, storage: S) -> Self {
        let o = Arc::new(options);
        let icmp = InternalKeyComparator::new(o.comparator.clone());
        Self {
            env: storage.clone(),
            internal_compactor: icmp.clone(),
            options: o.clone(),
            db_path: db_path.clone(),
            db_lock: None,
            batch_queue: Mutex::new(VecDeque::default()),
            process_batch_sem: Condvar::default(),
            table_cache: TableCache::new(
                db_path.clone(),
                o.clone(),
                o.clone(),
                o.table_cache_size(),
                storage.clone(),
            ),
            versions: Mutex::new(VersionSet::new(db_path, o.clone(), storage)),
            manual_compaction_queue: Mutex::new(VecDeque::default()),
            background_work_finished_signal: Condvar::new(),
            background_compaction_scheduled: AtomicBool::new(false),
            do_compaction: crossbeam_channel::unbounded(),
            mem: ShardedLock::new(MemTable::with_capacity(icmp, o.write_buffer_size)),
            im_mem: ShardedLock::new(None),
            bg_error: RwLock::new(None),
            is_shutting_down: Default::default(),
        }
    }

    fn snapshot(&self) -> Arc<Snapshot> {
        self.versions.lock().unwrap().new_snapshot()
    }

    fn get(&self, options: ReadOptions, key: &[u8]) -> IResult<Option<Vec<u8>>> {
        if self.is_shutting_down.load(Ordering::Acquire) {
            return Err(Error::DBClosed("get request".to_owned()));
        }
        let snapshot = match &options.snapshot {
            Some(snaphost) => snaphost.sequence(),
            None => self.versions.lock().unwrap().last_sequence(),
        };
        let look_key = LookupKey::new(key, snapshot);
        // Search the memtable.
        if let Some(result) = self.mem.read().unwrap().get(&look_key) {
            match result {
                Ok(value) => return Ok(Some(value.to_vec())),
                Err(_) => return Ok(None),
            }
        }
        // Search the immutable memtable.
        if let Some(im_mem) = self.im_mem.read().unwrap().as_ref() {
            if let Some(result) = im_mem.get(&look_key) {
                match result {
                    Ok(value) => return Ok(Some(value.to_vec())),
                    Err(_) => return Ok(None),
                }
            }
        }

        // Search the sstable.
        let current = self.versions.lock().unwrap().current();
        let (value, seek_stats) = current.get(options, look_key, &self.table_cache)?;
        if current.update_stats(seek_stats) {
            self.maybe_schedule_compaction(current);
        }
        Ok(value)
    }

    /// Record a sample of bytes read at the specified internal key
    /// Might schedule a background compaction.
    fn record_read_sample(&self, internal_key: &[u8]) {
        let current = self.versions.lock().unwrap().current();
        if current.record_read_sample(internal_key) {
            self.maybe_schedule_compaction(current);
        }
    }

    /// Recover DB from `db_path`.
    /// Returns the newest `VersionEdit` and whether we need to persistent `VersionEdit` to `Manifest`.
    fn recover(&mut self) -> IResult<(VersionEdit, bool)> {
        info!("Start recovering db : {}", &self.db_path);
        // Ignore error from `mkdir_all` since creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        let _ = self.env.mkdir_all(&self.db_path);

        // Try acquire file lock.
        let lock_file = self.env.create(&generate_filename(&self.db_path, FileType::Lock, 0))?;
        lock_file.lock_file()?;
        self.db_lock = Some(lock_file);
        if !self.env.exists(&generate_filename(&self.db_path, FileType::Current, 0)) {
            if self.options.create_if_missing {
                // Create new necessary files for DB.
                let mut new_db = VersionEdit::new(self.options.max_levels);
                new_db.set_comparator_name(self.options.comparator.name().to_owned());
                new_db.set_log_number(0);
                new_db.set_next_file(2);
                new_db.set_last_sequence(0);
                // Create manifest.
                let manifest_filenum = 1;
                let manifest_filename = generate_filename(&self.db_path, FileType::Manifest, manifest_filenum);
                debug!("Create manifest file: {}", &manifest_filename);
                let manifest = self.env.create(manifest_filename.as_str())?;
                let mut manifest_writer = Writer::new(manifest);
                let mut record = vec![];
                new_db.encode_to(&mut record);
                debug!("Append manifest record: {:?}", &new_db);
                match manifest_writer.add_record(&record) {
                    Ok(()) => update_current(&self.env, &self.db_path, manifest_filenum)?,
                    Err(e) => {
                        self.env.remove(manifest_filename.as_str())?;
                        return Err(e);
                    }
                }
            } else {
                return Err(Error::InvalidArgument(self.db_path.to_owned() + "dose not exist (create_if_missing is false)."));
            }
        } else if self.options.error_if_exists {
            return Err(Error::InvalidArgument(self.db_path.to_owned() + "exists (error_if_exists is true)."));
        }
        let mut versions = self.versions.lock().unwrap();
        let mut should_save_manifest = versions.recover()?;

        // Recover from all newer log files than the ones named in the
        // MANIFEST (new log files may have been added by the previous
        // incarnation without registering them in the MANIFEST).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an order version of leveldb.
        let min_log = versions.log_number();
        let prev_log = versions.prev_log_number();
        let mut expected_files = versions.live_files();
        let all_files = self.env.list(&self.db_path)?;
        let mut logs_to_recover = vec![];
        for filename in all_files {
            if let Some((file_type, file_number)) = parse_filename(filename) {
                expected_files.remove(&file_number);
                if file_type == FileType::Log && (file_number >= min_log || file_number == prev_log) {
                    logs_to_recover.push(file_number);
                }
            }
        }
        if !expected_files.is_empty() && self.options.paranoid_checks {
            return Err(Error::CorruptionString(format!("missing file {:?}", expected_files)));
        }

        // Recover in the order in which the logs were generated.
        logs_to_recover.sort_unstable();
        let mut max_sequence = 0;
        let mut edit = VersionEdit::new(self.options.max_levels);
        for (i, log_number) in logs_to_recover.iter().enumerate() {
            let last_seq = self.replay_log_file(
                &mut versions,
                *log_number,
                i == logs_to_recover.len() - 1,
                &mut should_save_manifest,
                &mut edit,
            )?;
            if max_sequence < last_seq {
                max_sequence = last_seq;
            }
            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number. So we manually
            // update the file number allocation counter in `VersionSet`.
            versions.mark_file_number_used(*log_number);
        }
        if versions.last_sequence() < max_sequence {
            versions.set_last_sequence(max_sequence);
        }
        Ok((edit, should_save_manifest))
    }

    /// Replays the edits in the named log file and returns the last sequence of insertions.
    fn replay_log_file(
        &self,
        versions: &mut MutexGuard<VersionSet<S, C>>,
        log_number: u64,
        last_log: bool,
        save_manifest: &mut bool,
        edit: &mut VersionEdit,
    ) -> IResult<u64> {
        let file_name = generate_filename(&self.db_path, FileType::Log, log_number);

        // Open the log file.
        let log_file = match self.env.open(file_name.as_str()) {
            Ok(f) => f,
            Err(e) => {
                return if self.options.paranoid_checks {
                    Err(e)
                } else {
                    info!("ignore errors when replaying log file: {:?}",e);
                    Ok(0)
                };
            }
        };

        // We intentionally make `Reader` do checksumming even if
        // paranoid_checks is false so that corruptions cause entries commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        let reporter = LogReporter::new();
        let mut reader = Reader::new(log_file, Some(Box::new(reporter.clone())), true, 0);
        info!("Recovering log #{}",log_number);

        // Read all the records and add to memtable.
        let mut mem = None;
        let mut record_buf = vec![];
        let mut batch = WriteBatch::default();
        let mut max_sequence = 0;
        let mut need_compaction = false; // indicates whether the memtable needs to be compacted.
        let mut inserted_size = 0;
        while reader.read_record(&mut record_buf) {
            if let Err(e) = reporter.result() {
                return Err(e);
            }
            if record_buf.len() < HEADER_SIZE {
                return Err(Error::Corruption("log record too small."));
            }
            if mem.is_none() {
                mem = Some(MemTable::with_capacity(
                    self.internal_compactor.clone(),
                    self.options.write_buffer_size,
                ))
            }
            let mem_ref = mem.as_ref().unwrap();
            batch.set_contents(&mut record_buf);
            let last_seq = batch.get_sequence() + u64::from(batch.get_count()) - 1;
            if let Err(e) = batch.insert_into(mem_ref) {
                if self.options.paranoid_checks {
                    return Err(e);
                } else {
                    info!("ignore errors when replaying log file : {:?}",e);
                }
            }
            inserted_size += batch.approximate_size();
            if last_seq > max_sequence {
                max_sequence = last_seq;
            }
            if mem_ref.memory_usage() > self.options.write_buffer_size {
                need_compaction = true;
                *save_manifest = true;
                let mut iter = mem_ref.iter();
                versions.write_level0_files(
                    &self.db_path,
                    &self.table_cache,
                    &mut iter,
                    edit,
                    false,
                )?;
                mem = None;
            }
            debug!("{} bytes inserted into Memtable in recovering",inserted_size);
        }

        // See if we should keep reusing the last log file.
        if self.options.reuse_logs && last_log && !need_compaction {
            let log_file = reader.into_file();
            debug!("Reusing old log file {}",file_name);
            versions.record_writer = Some(Writer::new(log_file));
            versions.set_log_number(log_number);
            if let Some(m) = mem {
                *self.mem.write().unwrap() = m;
                mem = None;
            } else {
                *self.mem.write().unwrap() = MemTable::with_capacity(
                    self.internal_compactor.clone(),
                    self.options.write_buffer_size,
                );
            }
        }

        if let Some(m) = &mem {
            debug!("Try to flush memtable into level 0 in recovering.");
            *save_manifest = true;
            let mut iter = m.iter();
            versions.write_level0_files(
                &self.db_path,
                &self.table_cache,
                &mut iter,
                edit,
                false,
            )?;
        }
        Ok(max_sequence)
    }

    /// Delete any unneeded files and stale in-memory entries.
    /// This func could delete generated compaction files when the compaction is failed due some reasons (e.g. block entry currupted.)
    fn delete_obsolete_file(&self, mut versions: MutexGuard<VersionSet<S, C>>) -> IResult<()> {
        versions.lock_live_files();
        // Ignore IO error on purpose
        let files = self.env.list(&self.db_path)?;
        for file in files.iter() {
            if let Some((file_type, number)) = parse_filename(file) {
                let keep = match file_type {
                    FileType::Log => {
                        number >= versions.log_number() || number == versions.prev_log_number()
                    }
                    FileType::Manifest => number >= versions.manifest_number(),
                    FileType::Table => versions.pending_outputs.contains(&number),
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs.
                    FileType::Temp => versions.pending_outputs.contains(&number),
                    _ => true,
                };
                if !keep {
                    if file_type == FileType::Table {
                        self.table_cache.evict(number)
                    }
                    info!("Delete type={:?} #{} [filename {:?}]",
                        file_type,number,&file);

                    // Ignore the IO error here
                    if let Err(e) = self.env.remove(&file) {
                        error!("Delete file failed [filename {:?}]: {:?}",&file,e)
                    }
                }
            }
        }
        versions.pending_outputs.clear();
        Ok(())
    }

    /// Schedule a WriteBatch to close batch processing thread for gracefully shutting down db.
    fn schedule_close_batch(&self) {
        let (send, _) = crossbeam_channel::bounded(0);
        let task = BatchTask {
            stop_process: true,
            force_mem_compaction: false,
            batch: WriteBatch::default(),
            signal: send,
            options: WriteOptions::default(),
        };
        self.batch_queue.lock().unwrap().push_back(task);
        self.process_batch_sem.notify_all();
    }

    /// Schedule the WriteBatch and wait for the result from the receiver.
    /// This function wakes up the thread in `process_batch`.
    /// An empty `WriteBatch` will trigger a force memtable compaction.
    fn schedule_batch_and_wait(
        &self,
        options: WriteOptions,
        batch: WriteBatch,
        force_mem_compaction: bool,
    ) -> IResult<()>
    {
        if self.is_shutting_down.load(Ordering::Acquire) {
            return Err(Error::DBClosed("Schedule WriteBatch".to_owned()));
        }
        if batch.is_empty() && !force_mem_compaction {
            return Ok(());
        }
        let (send, recv) = crossbeam_channel::bounded(0);
        let task = BatchTask {
            stop_process: false,
            force_mem_compaction,
            batch,
            signal: send,
            options,
        };
        self.batch_queue.lock().unwrap().push_back(task);
        self.process_batch_sem.notify_all();
        recv.recv().unwrap_or_else(|e| Err(Error::RecvError(e)))
    }

    /// Make sure there is enough space in memtable.
    /// This method acquires the mutex of `VersionSet` and deliver it to the caller.
    /// The `force` flag is used for forcing to compact current memtable into level0 sst files.
    fn make_room_for_write(&self, mut force: bool) -> IResult<MutexGuard<VersionSet<S, C>>> {
        let mut allow_delay = !force;
        let mut versions = self.versions.lock().unwrap();
        loop {
            if let Some(e) = self.take_bg_error() {
                return Err(e);
            } else if allow_delay && versions.level_files_count(0) >= self.options.l0_slowdown_writes_threshold {
                // We are getting close to hitting a hard limit on the number of
                // L0 files. Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.
                // Also, this delay hands over some CPU to the compaction thread
                // in case it is sharing the same core as the writer.
                thread::sleep(Duration::from_micros(1000));
                allow_delay = false;
            } else if !force && self.mem.read().unwrap().memory_usage() <= self.options.write_buffer_size {
                // There is room in current memtable.
                break;
            } else if self.im_mem.read().unwrap().is_some() {
                info!("Current memtable full; waiting...");
                versions = self.background_work_finished_signal.wait(versions).unwrap();
            } else if versions.level_files_count(0) >= self.options.l0_stop_writes_threshold {
                info!("Too many L0 files {}; waiting...",versions.level_files_count(0));
                versions = self.background_work_finished_signal.wait(versions).unwrap();
            } else {
                let new_log_num = versions.get_next_file_number();
                let log_file = self.env.create(
                    &generate_filename(&self.db_path, FileType::Log, new_log_num).as_str(),
                )?;
                versions.set_next_file_number(new_log_num + 1);
                versions.set_log_number(new_log_num);
                versions.record_writer = Some(Writer::new(log_file));
                // Rotate the mem to immutable mem.
                {
                    let mut mem = self.mem.write().unwrap();
                    if mem.len() > 0 {
                        let memtable = std::mem::replace(
                            &mut *mem,
                            MemTable::with_capacity(
                                self.internal_compactor.clone(),
                                self.options.write_buffer_size,
                            ),
                        );
                        let mut im_mem = self.im_mem.write().unwrap();
                        *im_mem = Some(memtable);
                    }
                    force = false; // do not force another compaction if have room.
                }
                self.maybe_schedule_compaction(versions.current());
            }
        }
        Ok(versions)
    }

    /// Group a bunch of batches in the waiting queue
    /// This will ignore the task with `force_mem_compaction` after batch
    fn group_batches(&self, first: BatchTask) -> (BatchTask, Vec<Sender<IResult<()>>>) {
        let mut size = first.batch.approximate_size();
        // Allow the group to grow up to a maximum size, but if the
        // origin write is small, limit the growth so we do not slow
        // down the small write too much.
        let mut max_size = 1 << 20;
        if size <= 128 << 10 {
            max_size = size + (128 << 10);
        }
        let mut signals = vec![first.signal.clone()];
        let mut grouped = first;

        let mut queue = self.batch_queue.lock().unwrap();
        // Group several batches from queue.
        while !queue.is_empty() {
            let current = queue.pop_front().unwrap();
            if current.stop_process || (current.options.sync && !grouped.options.sync) {
                // Do not include a stop process batch
                // Do not include a sync write into a batch handled by a non-sync write.
                queue.push_front(current);
                break;
            }
            size += current.batch.approximate_size();
            if size > max_size {
                // Do not make batch too big.
                break;
            }
            grouped.batch.append(current.batch);
            signals.push(current.signal.clone());
        }
        (group, signals)
    }

    /// Compact immutable memory table to level0 files.
    fn compact_memt_table(&self) -> IResult<()> {
        debug!("Compact memtable.");
        let mut versions = self.versions.lock().unwrap();
        let mut edit = VersionEdit::new(self.options.max_levels);
        let mut im_mem = self.im_mem.write().unwrap();
        let mut iter = im_mem.as_ref().unwrap().iter();
        versions.write_level0_files(
            &self.db_path,
            &self.table_cache,
            &mut iter,
            &mut edit,
            true,
        )?;
        if self.is_shutting_down.load(Ordering::Acquire) {
            Err(Error::DBClosed("When compacting memory table".to_owned()))
        } else {
            edit.prev_log_number = Some(0);
            edit.log_number = Some(versions.log_number()); // earlier logs no longer needed.
            let res = versions.log_and_apply(edit);
            *im_mem = None;
            self.delete_obsolete_file(versions)?;
            res
        }
    }

    /// Force current memtable contents(even if the memtable is not full) to be compacted into sst files.
    fn force_compact_mem_table(&self) -> IResult<()> {
        let empty_batch = WriteBatch::default();
        // Schedule a force memory compaction.
        self.schedule_batch_and_wait(WriteOptions::default(), empty_batch, true)?;
        // Waiting for memory compaction complete.
        thread::sleep(Duration::from_secs(1));
        if self.im_mem.read().unwrap().is_some() {
            return self.take_bg_error().map_or(Ok(()), Err);
        }
        Ok(())
    }

    /// Compact the underlying storage for the key range `[begin, end]`.
    ///
    /// In particular, deleted and overwritten versions are discarded,
    /// and the data is rearranged to reduce the cost of operations
    /// needed to access the data.
    ///
    /// This operation should typically only be invoked by users
    /// who understand the underlying implementation.
    ///
    /// A `None` is treated as a key before all keys for `begin`
    /// and a key after all keys for `end` in the database.
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> IResult<()> {
        let mut max_level_with_files = 1;
        {
            let versions = self.versions.lock().unwrap();
            let current = versions.current();
            for l in 1..self.options.max_levels as usize {
                if current.overlap_in_level(l, begin, end) {
                    max_level_with_files = l;
                }
            }
        }
        self.force_compact_mem_table()?;
        for l in 0..max_level_with_files {
            self.manual_compact_range(l, begin, end)?
        }
        Ok(())
    }

    /// Schedule a manual compaction for the key range `[begin, end]` and waits utils the
    /// compaction completes.
    fn manual_compact_range(&self, level: usize, begin: Option<&[u8]>, end: Option<&[u8]>) -> IResult<()> {
        assert!(level + 1 < self.options.max_levels as usize);
        let (sender, finished) = crossbeam_channel::bounded::<IResult<()>>(1);
        {
            let mut m_queue = self.manual_compaction_queue.lock().unwrap();
            m_queue.push_back(ManualCompaction {
                level,
                done: sender,
                begin: begin.map(|k| InternalKey::new(k, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK)),
                end: end.map(|k| InternalKey::new(k, 0, ValueType::KTypeValue)),
            });
        }
        let v = self.versions.lock().unwrap().current();
        self.maybe_schedule_compaction(v);
        match finished.recv() {
            Ok(res) => res,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    /// The complete compaction process
    /// Returns true if a compaction is actually scheduled.
    fn background_compaction(&self) -> bool {
        if self.im_mem.read().unwrap().is_some() {
            if let Err(e) = self.compact_memt_table() {
                warn!("Compact memtable error: {:?}",e);
            }
            true
        } else {
            let mut versions = self.versions.lock().unwrap();
            let mut is_manual = false;
            let (compaction, done) = {
                if let Some(manual) = self.manual_compaction_queue.lock().unwrap().pop_front() {
                    is_manual = true;
                    let begin = if let Some(begin) = &manual.begin {
                        format!("{:?}", begin)
                    } else {
                        "(-∞)".to_owned()
                    };

                    let end = if let Some(end) = &manual.end {
                        format!("{:?}", end)
                    } else {
                        "(+∞)".to_owned()
                    };

                    match versions.compact_range(
                        manual.level,
                        manual.begin.as_ref(),
                        manual.end.as_ref(), )
                    {
                        Some(c) => {
                            info!("Received manual compaction at level {} from {} .. {}; will stop at {:?}",
                            manual.level, begin,end,&c.inputs.base.last().unwrap().largest);
                            (Some(c), Some(manual.done))
                        }
                        None => {
                            info!("Received manual compaction at level {} from {} .. {}; No compaction needs to be done",
                            manual.level,begin,end);
                            manual.done.send(Ok(())).unwrap();
                            (None, None)
                        }
                    }
                } else {
                    (versions.pick_compaction(), None)
                }
            };
            let has_compaction = compaction.is_some();
            if let Some(mut compaction) = compaction {
                let level = compaction.level;
                info!(
                    "[{:?}] Compacting [{}]@{} = [{}]@{} files",
                    compaction.reason,
                    compaction.inputs.desc_base_files(),
                    level,
                    compaction.inputs.desc_parent_files(),
                    level + 1,
                );
                if !is_manual && compaction.is_trivial_move() {
                    // just move file to next leve
                    let f = compaction.inputs.base.first().unwrap();
                    compaction.edit.delete_file(compaction.level, f.number);
                    compaction.edit.add_file(
                        compaction.level + 1,
                        f.number,
                        f.file_size,
                        f.smallest.clone(),
                        f.largest.clone(),
                    );
                    let res = versions.log_and_apply(compaction.edit);
                    if let Err(e) = res.as_ref() {
                        error!("Compaction error: {:?}",e);
                    }
                    let current_summary = versions.current().level_summary();
                    info!(
                        "Moved #{} to level-{} {} bytes, current level summary: {}",
                        f.number,
                        compaction.level + 1,
                        f.file_size,
                        current_summary
                    );
                    if let Some(done) = done {
                        done.send(res).unwrap();
                    }
                    if let Err(e) = self.delete_obsolete_files(versions) {
                        error!("Delete obsolete files error: {}", e);
                    }
                } else {
                    {
                        let snapshots = &mut versions.snapshots;
                        // Cleanup all redundant snapshots first.
                        snapshots.gc();
                        if snapshots.is_empty() {
                            compaction.oldest_snapshot_alive = versions.last_sequence();
                        } else {
                            compaction.oldest_snapshot_alive = snapshots.oldest().sequence();
                        }
                    }
                    // Unlock VersionSet here to avoid dead lock.
                    std::mem::drop(versions);
                    match self.do_compaction(compaction) {
                        Ok(versions) => {
                            let res = self.delete_obsolete_file(versions);
                            if let Some(done) = done {
                                done.send(res).unwrap();
                            }
                        }
                        Err(e) => {
                            {
                                let versions = self.versions.lock().unwrap();
                                let _ = self.delete_obsolete_files(versions);
                            }
                            error!("Compaction error: {:?}", &e);
                            if let Some(done) = done {
                                done.send(Err(e)).unwrap();
                            }
                        }
                    }
                };
            }
            has_compaction
        }
    }

    fn do_compaction(&self, mut c: Compaction<S::F, C>) -> IResult<MutexGuard<VersionSet<S, C>>> {}

    /// Replace the `bg_error` with new `Error` if it's `None`.
    fn record_bg_error(&self, e: Error) {
        if !self.has_bg_error() {
            let mut x = self.bg_error.write().unwrap();
            *x = Some(e);
            self.background_work_finished_signal.notify_all();
        }
    }

    fn take_bg_error(&self) -> Option<Error> {
        self.bg_error.write().unwrap().take()
    }

    fn has_bg_error(&self) -> bool {
        self.bg_error.read().unwrap().is_some()
    }

    /// Check whether db needs to run a compaction. DB will run a compaction when:
    /// 1. no background compaction is running
    /// 2. DB is not shutting down
    /// 3. no error has been encountered
    /// 4. there is an immutable table or a manual compaction request or current version needs to be compacted
    fn maybe_schedule_compaction(&self, version: Arc<Version<C>>) -> bool {
        // Areadly scheduled.
        if self.background_compaction_scheduled.load(Ordering::Acquire)
            // DB is being shutting down
            || self.is_shutting_down.load(Ordering::Acquire)
            || self.has_bg_error()
            || (self.im_mem.read().unwrap().is_none())
            && self.manual_compaction_queue.lock().unwrap().is_empty() && !version.needs_compaction()
        {
            // No work need to be done.
            false
        } else {
            self.background_compaction_scheduled.store(true, Ordering::Release);
            if let Err(e) = self.do_compaction.0.send(()) {
                error!("[Schedule compaction] Failed sending signal to compaction channel: {}",e);
            }
            true
        }
    }

    /// Finish the current output file by calling `builder.finish` and insert it into the table cache.
    fn finish_output_file(
        &self,
        c: &mut Compaction<S::F, C>,
        input_iter_status: IResult<()>,
    ) -> IResult<()>
    {
        assert!(!c.outputs.is_empty());
        assert!(c.builder.is_some());
        let current_entries = c.builder.as_ref().unwrap().num_entries();
        let status = if input_iter_status.is_ok() {
            c.builder.as_mut().unwrap().finish(true)
        } else {
            c.builder.as_mut().unwrap().close();
            input_iter_status
        };
        let current_bytes = c.builder.as_ref().unwrap().file_size();
        // Update current output.
        c.outputs.last_mut().unwrap().file_size = current_bytes;
        c.total_bytes += current_bytes;
        c.builder = None;
        if status.is_ok() && current_entries > 0 {
            let f = c.outputs.last_mut().unwrap();
            let _ = self.table_cache.new_iter(
                self.internal_compactor.clone(),
                ReadOptions::default(),
                f.number,
                f.file_size,
            )?;
            info!(
                "Compaction output table #{}@{}: {} keys, {} bytes, [{:?} ... {:?}]",
                f.number,
                c.level + 1,
                current_entries,
                f.file_size,
                f.smallest,
                f.largest,
            );
        }
        status
    }

    /// Returns the approximate file system space used by keys in "[start .. end]"
    ///
    /// Note that thre returned sizes measure file system space usage, so
    /// if the user data compresses by a factor of ten, the returned
    /// sizes will be one-tenth the size of the corresponding user data size.
    ///
    /// The results may not include the sizez of recently written data.
    pub fn get_approximate_size(&self, start: &[u8], end: &[u8]) -> u64 {
        let current = self.versions.lock().unwrap().current();
        let start_ikey = InternalKey::new(start, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
        let end_ikey = InternalKey::new(end, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
        let start = current.approximate_offset_of(&start_ikey, &self.table_cache);
        let limit = current.approximate_offset_of(&end_ikey, &self.table_cache);
        if limit >= start {
            limit - start
        } else {
            0
        }
    }
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
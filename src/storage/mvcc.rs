use crate::storage::Storage;
use std::sync::{Arc, RwLock};
use crate::storage::error::{Result, Error};
use std::collections::HashSet;
use std::borrow::Cow;
use std::iter::Peekable;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::ops::RangeBounds;

/// The status of mvcc
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

/// An MVCC-based transactional key-value Storage.
pub struct MVCC {
    /// The underlying KV storage. It is protected by a mutex so it can be shared between txns.
    storage: Arc<RwLock<Box<dyn Storage>>>,
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl MVCC {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self { storage: Arc::new(RwLock::new(storage)) }
    }

    /// Begins a new transaction in read-write mode.
    pub fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.storage.clone(), Mode::ReadWrite)
    }

    /// Begins a new transaction in the given mode.
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        Transaction::begin(self.storage.clone(), mode)
    }

    /// Resumes a transaction with the given ID.
    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.storage.clone(), id)
    }

    /// Returns an unversioned metadata value
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.storage.read()?;
        session.get(&Key::Metadata(key.into()).encode())
    }

    /// Sets an unversioned metadata value
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let mut session = self.storage.write()?;
        session.set(&Key::Metadata(key.into()).encode(), value)
    }

    /// Return engine status
    //
    // Bizarrely, the return statement is in fact necessary - see:
    // https://github.com/rust-lang/reference/issues/452
    #[allow(clippy::needless_return)]
    pub fn status(&self) -> Result<Status> {
        let store = self.storage.read()?;
        return Ok(Status {
            txns: match store.get(&Key::TxnNext.encode())? {
                Some(ref v) => deserialize(v)?,
                None => 1,
            },

            txns_active: store
                .scan(crate::storage::Range::from(
                    Key::TxnActive(0).encode()..Key::TxnActive(std::u64::MAX).encode(),
                ))
                .try_fold(0, |count, r| r.map(|_| count + 1))?,
            storage: store.to_string(),
        });
    }
}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}

/// An MVCC transaction
pub struct Transaction {
    /// The underlying storage for the transaction. Shared between transactions using a mutex.
    storage: Arc<RwLock<Box<dyn Storage>>>,
    /// Tbe unique transaction Id.
    id: u64,
    /// The transaction mode.
    mode: Mode,
    /// The snapshot that the transaction is running in.
    snapshot: Snapshot,
}

impl Transaction {
    /// Begins a new transaction in the given mode.
    fn begin(storage: Arc<RwLock<Box<dyn Storage>>>, mode: Mode) -> Result<Self> {}

    /// Resumes an active transaction with the given Id, Errors if the transaction is not active.
    fn resume(storage: Arc<RwLock<Box<dyn Storage>>>, id: u64) -> Result<Self> {}

    /// Commits the transaction, by removing the txn from the active set.
    pub fn commit(self) -> Result<()> {
        let mut session = self.storage.read()?;
        // FIXME not atomic, what to do if the delete operation occur error.
        session.delete(&Key::TxnActive(self.id).encode())?;
        session.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {}

    /// Delete a key. Instead of actually deleting, let the null value make the key
    /// call the tombstone message. The background thread compresses periodically to
    /// delete tombstone messages.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Returns a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {}

    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<crate::storage::Scan> {}

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<crate::storage::Range> {}

    /// Set a key
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Writes a value for key. None is tombstone message is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {}

    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[inline]
    pub fn mode(&self) -> Mode {
        self.mode
    }
}

/// MVCC transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    /// A read-write transaction.
    ReadWrite,
    /// A read-only transaction.
    ReadOnly,
    /// A read-only transaction running in a snapshot of a given version.
    ///
    /// The version must refer to a committed transaction Id. Any changes visible to to original
    /// transaction will be visible in the snapshot (i.e. transactions that had not committed before
    /// the snapshot transaction will not be visible, even though they have a lower version).
    Snapshot { version: u64 },
}

impl Mode {
    /// Checks whether the transaction mode can mutate data.
    #[inline]
    pub fn mutable(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            Self::ReadOnly => false,
            Self::Snapshot { .. } => false,
        }
    }

    /// Checks whether a mode satisfies a mode (i.e. ReadWrite satisfies ReadOnly).
    #[inline]
    pub fn satisfies(&self, other: &Mode) -> bool {
        match (self, other) {
            (Mode::ReadWrite, Mode::ReadOnly) => true,
            (Mode::Snapshot { .. }, Mode::ReadOnly) => true,
            (_, _) if self == other => true,
            (_, _) => false,
        }
    }
}

/// A versioned snapshot, containing visibility information about concurrent transactions.
#[derive(Clone)]
struct Snapshot {
    /// The version (i.e. transaction id) that the snapshot belongs to.
    version: u64,
    /// The set of transaction Ids that were active at the start of the transaction,
    /// and thus should be invisible to the snapshot.
    invisible: HashSet<u64>,
}

impl Snapshot {}

/// MVCC keys. The encoding preserves the grouping and ordering of keys. Uses a Cow since we want
/// to take borrows when encoding and return owned when decoding.
#[derive(Debug)]
enum Key<'a> {
    /// The next available txn Id. Used when starting new txns.
    TxnNext,
    /// Active txn markers, containing the mode. Used to detect concurrent txns,and to resume.
    TxnActive(u64),
    /// Txn snapshot, containing concurrent active txns at start of txn.
    TxnSnapshot(u64),
    /// Update marker for a txn Id and key, used for rollback.
    TxnUpdate(u64, Cow<'a, [u8]>),
    /// A record for a key/version pair.
    Record(Cow<'a, [u8]>, u64),
    /// Arbitrary unversioned metadata.
    Metadata(Cow<'a, [u8]>),
}

impl<'a> Key<'a> {
    /// Encodes a key into a byte vector.
    fn encode(self) -> Vec<u8> {}
}

/// A key range scan.
pub struct Scan {
    /// The augmented KV store iterator, with key(decoded) and value. Node that we don't retain
    /// the decoded version, so there wil be multiple keys (for each version). We want the last.
    scan: Peekable<crate::storage::Scan>,
    /// Keep track of next_back() seen key, whose previous versions should be ignored.
    next_back_seen: Option<Vec<u8>>,
}

impl Scan {}
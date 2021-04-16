use crate::storage::encoding::{encode_bytes, encode_u64, take_byte, take_bytes, take_u64};
use crate::storage::error::{Error, Result};
use crate::storage::Storage;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};
use std::option::Option::Some;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
        Self {
            storage: Arc::new(RwLock::new(storage)),
        }
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
    fn begin(storage: Arc<RwLock<Box<dyn Storage>>>, mode: Mode) -> Result<Self> {
        let mut session = storage.write()?;

        let id = match session.get(&Key::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        session.set(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;
        session.set(&Key::TxnActive(id).encode(), serialize(&mode)?)?;

        // We always take a new snapshot, even for snapshot transaction, because all transactions
        // increment the transaction Id and we need to properly record currently active transactions
        // for any future snapshot transactions looking at this one.
        let mut snapshot = Snapshot::take(&mut session, id)?;
        drop(session);
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&storage.read()?, *version)?
        }
        Ok(Self {
            storage,
            id,
            mode,
            snapshot,
        })
    }

    /// Resumes an active transaction with the given Id, Errors if the transaction is not active.
    fn resume(storage: Arc<RwLock<Box<dyn Storage>>>, id: u64) -> Result<Self> {
        let session = storage.read()?;
        let mode = match session.get(&Key::TxnActive(id).encode())? {
            Some(ref v) => deserialize(v)?,
            None => return Err(Error::InternalTnx(format!("No active transaction {}", id))),
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _ => Snapshot::restore(&session, id)?,
        };
        drop(session);

        Ok(Self {
            storage,
            id,
            mode,
            snapshot,
        })
    }

    /// Commits the transaction, by removing the txn from the active set.

    pub fn commit(self) -> Result<()> {
        let mut session = self.storage.write()?;
        // FIXME not atomic, what to do if the delete operation occur error.
        session.delete(&Key::TxnActive(self.id).encode())?;
        session.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {
        let mut session = self.storage.write()?;
        if self.mode.mutable() {
            let mut rollback = Vec::new();
            let mut scan = session.scan(crate::storage::Range::from(
                Key::TxnUpdate(self.id, vec![].into()).encode()
                    ..Key::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = scan.next().transpose()? {
                match Key::decode(&key)? {
                    Key::TxnUpdate(_, updated_key) => rollback.push(updated_key.into_owned()),
                    k => {
                        return Err(Error::InternalTnx(format!(
                            "Expected TxnUpdate, got {:?}",
                            k
                        )));
                    }
                };
                rollback.push(key);
            }
            std::mem::drop(scan);
            for key in rollback.into_iter() {
                session.delete(&key)?;
            }
        }
        session.delete(&Key::TxnActive(self.id).encode())
    }

    /// Delete a key. Instead of actually deleting, let the null value make the key
    /// call the tombstone message. The background thread compresses periodically to
    /// delete tombstone messages.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Returns a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.storage.read()?;
        let mut scan = session
            .scan(crate::storage::Range::from(
                Key::Record(key.into(), 0).encode()..=Key::Record(key.into(), self.id).encode(),
            ))
            .rev();
        while let Some((k, v)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(_, version) => {
                    if self.snapshot.is_visible(version) {
                        return deserialize(&v);
                    }
                }
                k => {
                    return Err(Error::InternalTnx(format!(
                        "Expected Txn::Record, got {:?}",
                        k
                    )));
                }
            };
        }
        Ok(None)
    }

    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<crate::storage::Scan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self
            .storage
            .read()?
            .scan(crate::storage::Range::from((start, end)));
        Ok(Box::new(Scan::new(scan, self.snapshot.clone())))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<crate::storage::Scan> {
        if prefix.is_empty() {
            return Err(Error::InternalErr("Scan prefix cannot be empty".into()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                // if all 0xff we could in principle use Range::Unbounded,but it won't happen
                0xff if i == 0 => {
                    return Err(Error::InternalErr("Invalid prefix scan range".into()));
                }
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }

    /// Set a key
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Writes a value for key. None is tombstone message is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::InternalTnx("Read-only transaction".into()));
        }

        let mut session = self.storage.write()?;

        let min = self
            .snapshot
            .invisible
            .iter()
            .min()
            .cloned()
            .unwrap_or(self.id + 1);
        let mut scan = session
            .scan(crate::storage::Range::from(
                Key::Record(key.into(), min).encode()
                    ..=Key::Record(key.into(), std::u64::MAX).encode(),
            ))
            .rev();
        while let Some((k, _)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(_, version) => {
                    if !self.snapshot.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                k => {
                    return Err(Error::InternalTnx(format!(
                        "Expected Txn::Record, got {:?}",
                        k
                    )));
                }
            };
        }
        drop(scan);

        // Write the key and its update record.
        let key = Key::Record(key.into(), self.id).encode();
        let update = Key::TxnUpdate(self.id, (&key).into()).encode();
        session.set(&update, vec![])?;
        session.set(&key, serialize(&value)?)
    }

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

impl Snapshot {
    /// Takes a new snapshot, persisting it as `key::TxnSnapshot(version)`.
    fn take(session: &mut RwLockWriteGuard<Box<dyn Storage>>, version: u64) -> Result<Self> {
        let mut snapshot = Self {
            version,
            invisible: HashSet::new(),
        };
        let mut scan = session.scan(crate::storage::Range::from(
            Key::TxnActive(0).encode()..Key::TxnActive(version).encode(),
        ));
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(id) => snapshot.invisible.insert(id),
                k => {
                    return Err(Error::InternalTnx(format!(
                        "Expected TxnActive, got {:?}",
                        k
                    )));
                }
            };
        }
        std::mem::drop(scan);
        session.set(
            &Key::TxnSnapshot(version).encode(),
            serialize(&snapshot.invisible)?,
        )?;
        Ok(snapshot)
    }

    /// Restore an existing snapshot from `Key::TxnSnapshot(Version)`, or errors if not found.
    fn restore(session: &RwLockReadGuard<Box<dyn Storage>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref v) => Ok(Self {
                version,
                invisible: deserialize(v)?,
            }),
            None => Err(Error::InternalTnx(format!(
                "Snapshot not found for version: {}",
                version
            ))),
        }
    }

    /// Checks whether the given version is visible in this snapshot.
    #[inline]
    fn is_visible(&self, version: u64) -> bool {
        version <= self.version && self.invisible.get(&version).is_none()
    }
}

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
    fn encode(self) -> Vec<u8> {
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
            Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
            Self::TxnUpdate(id, key) => {
                [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
            }
            Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
            Self::Record(key, version) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
            }
        }
    }

    /// Decodes a key from a byte representation.
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        let mut bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Metadata(take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => {
                return Err(Error::InternalErr(format!(
                    "Unknown MVCC key prefix {:x?}",
                    b
                )));
            }
        };

        if !bytes.is_empty() {
            return Err(Error::InternalErr(
                "Unexcepted data remaining at end of key".into(),
            ));
        }
        Ok(key)
    }
}

/// A key range scan.
pub struct Scan {
    /// The augmented KV store iterator, with key(decoded) and value. Node that we don't retain
    /// the decoded version, so there wil be multiple keys (for each version). We want the last.
    scan: Peekable<crate::storage::Scan>,
    /// Keep track of next_back() seen key, whose previous versions should be ignored.
    next_back_seen: Option<Vec<u8>>,
}

impl Scan {
    fn new(mut scan: crate::storage::Scan, snapshot: Snapshot) -> Self {
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(_, version) if !snapshot.is_visible(version) => Ok(None),
                Key::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::InternalErr(format!("Excepted Record, got {:?}", k))),
            })
                .transpose()
        }));
        Self {
            scan: scan.peekable(),
            next_back_seen: None,
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // Only return the item if it is the last version of the key.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // Only return the last version of the key (so skip if seen).
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Scan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

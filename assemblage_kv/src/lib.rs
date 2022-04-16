//! # Versioned and transactional key-value store for native and wasm targets.
//!
//! This crate provides a persistent key-value store implemented as a log-structured hash table
//! similar to [Bitcask](https://riak.com/assets/bitcask-intro.pdf). Writes of new or changed values
//! never overwrite old entries, but are simply appended to the end of the storage. Old values are
//! kept at earlier offsets in the storage and remain accessible. An in-memory hash table tracks the
//! storage offsets of all keys and allows efficient reads directly from the relevant portions of
//! the storage. A store can be merged, which discards old versions and builds a more compact
//! representation containing only the latest value of each key.
//!
//! ## Features
//!
//!   - _simple_: log-structured hash architecture, with all keys in memory
//!   - _fully versioned:_ old values remain accessible until merged
//!   - _transactional:_ all reads and writes happen only in isolated transactions
//!   - _storage-agnostic:_ supports files on native and IndexedDB on wasm
//!
//! ## Example
//!
//! ```
//! use assemblage_kv::{run, storage::{self, Storage}, KvStore, Snapshot, Result};
//!
//! fn main() -> Result<()> {
//!     // The `run!` macro abstracts away the boilerplate of setting up the
//!     // right async environment and storage for native / wasm and is not
//!     // needed outside of doc tests.
//!     run!(async |storage| {
//!         let store_name = storage.name().to_string();
//!         let mut store = KvStore::open(storage).await?;
//!
//!         {
//!             let mut current = store.current().await;
//!             assert_eq!(current.get(&[1, 2]).await?, None);
//!             current.insert(vec![1, 2], vec![5, 6, 7])?;
//!             current.commit().await?;
//!         }
//!
//!         {
//!             let mut current = store.current().await;
//!             assert_eq!(current.get(&[1, 2]).await?, Some(vec![5, 6, 7]));
//!             current.remove(vec![1, 2])?;
//!             current.commit().await?;
//!         }
//!
//!         {
//!             let mut current = store.current().await;
//!             assert_eq!(current.get(&[1, 2]).await?, None);
//!             current.insert(vec![1, 2], vec![8])?;
//!             current.commit().await?;
//!         }
//!
//!         {
//!             let current = store.current().await;
//!             let versions = current.versions(&[1, 2]).await?;
//!             assert_eq!(versions.len(), 3);
//!             assert_eq!(current.get_version(&[1, 2], versions[0]).await?, Some(vec![5, 6, 7]));
//!             assert_eq!(current.get_version(&[1, 2], versions[1]).await?, None);
//!             assert_eq!(current.get_version(&[1, 2], versions[2]).await?, Some(vec![8]));
//!         }
//!
//!         store.merge().await?;
//!         let storage = storage::open(&store_name).await?;
//!         let store = KvStore::open(storage).await?;
//!
//!         {
//!             let current = store.current().await;
//!             let versions = current.versions(&[1, 2]).await?;
//!             assert_eq!(versions.len(), 1);
//!             assert_eq!(current.get(&[1, 2]).await?, Some(vec![8]));
//!         }
//!         Ok(())
//!     })
//! }
//! ```
#![deny(unsafe_code)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

use crate::{storage::Storage, timestamp::timestamp_now_monotonic};
use crc32fast::Hasher;
use log::warn;
use std::{
    cmp::max,
    collections::HashMap,
    mem,
    sync::{Mutex as SyncMutex, MutexGuard as SyncMutexGuard},
};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

pub mod storage;
pub mod timestamp;

const BYTES_TIMESTAMP_FULL: usize = 6;
const BYTES_CRC: usize = 4;

/// The error type for store operations.
#[derive(Debug)]
pub enum Error {
    /// Caused by storage read or write operations.
    StorageError(storage::Error),
    /// The CRC checksum of an entry did not match its content.
    CorruptDataError(u64),
    /// The kv entry had an invalid format.
    InvalidEntryError {
        /// The reason why the entry was invalid.
        reason: String,
    },
    /// The store key or value exceeded the maximum size supported by the store.
    MaxSizeExceeded {
        /// The size of the key or value in bytes.
        size: usize,
        /// The maximum bytes available for storing the key or value size.
        max_bytes: u8,
        /// The bytes that would be required to store the key or value size.
        bytes_required: u8,
    },
    /// The bytes read exceeded the expected number of bytes for an int.
    InvalidIntLength {
        /// The number of bytes of the int that were expected.
        bytes_expected: u8,
        /// The number of bytes that were found.
        bytes_found: u8,
    },
    /// The storage could not be locked.
    StorageLockError,
    /// The transaction has read a value that has since been overwritten.
    TransactionConflict,
}

/// A specialized `Result` type for store operations.
pub type Result<T> = std::result::Result<T, Error>;

impl<'a> From<storage::Error> for Error {
    fn from(e: storage::Error) -> Self {
        Error::StorageError(e)
    }
}

/// A versioned key-value store using a log-structured hash table.
///
/// Reads and writes `&[u8]`/`Vec<u8>` keys and values. All reads/writes happen through
/// transactions. A store can be merged to discard old versions and thus store a more compact
/// representation containing only the latest version of each key-value pair.
///
/// Versions are also used to implement a "move to trash" behavior. Whenever a value is removed, it
/// is not purged from storage but simply marked as removed. It remains accessible until the "trash
/// is emptied" during the next merge. As a consequence there are 2 different methods that can read
/// values from the store, depending on whether the trash should be included or not,
/// [`Snapshot::get()`] (which will return `None` if the value was "moved to the trash") and
/// [`Snapshot::get_unremoved()`] (which will return the last unremoved version if the value was
/// "moved to the trash").
pub struct KvStore<S: Storage> {
    name: String,
    storage: AsyncMutex<S>,
    offsets: AsyncMutex<HashMap<Vec<u8>, Vec<BlobVersion>>>,
    latest_timestamp: AsyncMutex<u64>,
}

impl<S: Storage> KvStore<S> {
    /// Opens and reads a store from storage.
    ///
    /// If no store exists at the storage location, a new store will be initialized. Otherwise, the
    /// store will be read and checked for corrupted data. In case of corruption, everything after
    /// the corrupted offset will be truncated and later writes will overwrite the corrupted
    /// entries. After the initial read, a hash table of all the keys in the store and their storage
    /// offsets is kept in memory.
    pub async fn open(storage: S) -> Result<Self> {
        let mut store = Self {
            name: String::from(storage.name()),
            storage: AsyncMutex::new(storage),
            offsets: AsyncMutex::new(HashMap::new()),
            latest_timestamp: AsyncMutex::new(0),
        };
        init_store(&mut store).await?;
        Ok(store)
    }

    /// Returns the (file-)name of the storage.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consumes the store to return its underlying storage.
    pub fn into_storage(self) -> Result<S> {
        Ok(self.storage.into_inner())
    }

    /// Returns the total length of the storage in bytes.
    pub async fn len(&self) -> u64 {
        self.storage.lock().await.len()
    }

    /// Returns `true` if the storage is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Creates a transactional read-write snapshot of the store at the current point in time, see
    /// [`Snapshot`].
    pub async fn current(&self) -> Snapshot<'_, S> {
        let latest_timestamp = *self.latest_timestamp.lock().await;
        let latest_offset = self.storage.lock().await.len();
        let snapshot_timestamp = timestamp_now_monotonic(latest_timestamp);
        Snapshot {
            store: self,
            snapshot_timestamp,
            latest_timestamp,
            latest_offset,
            transaction_entries: HashMap::new(),
            cached_entries: SyncMutex::new(HashMap::new()),
        }
    }

    /// Merges and compacts the store by removing old versions.
    ///
    /// Merging a store reclaims space by removing all versions that were superseded by newer writes
    /// to the same key. As a side effect, a merge "empties the trash" and ensures that removed
    /// values cannot be read and restored anymore.
    pub async fn merge(&mut self) -> Result<()> {
        {
            self.storage.lock().await.flush().await?;
            self.storage.lock().await.start_merge().await?;

            let mut crc = Hasher::new();
            let mut offset = 0;
            let mut storage = self.storage.lock().await;
            while offset < storage.len() {
                let mut entry = Entry::read_from(&mut storage, offset).await?;
                let entry_length = entry.len() as u64;
                let offsets = &mut (*self.offsets.lock().await);

                // all kv writes have Some(key), all transactions have None
                if let Some(k) = entry.key.as_ref() {
                    if offset == offsets[k].last().unwrap().offset {
                        entry.update_crc(&mut crc);
                        entry.write_to(&mut storage).await?;
                    }
                } else if entry.is_transaction_commit() {
                    entry.update_crc(&mut crc);
                    let crc_merged = crc.finalize();
                    let crc_original = entry.crc()?;
                    if crc_merged != crc_original {
                        entry.set_crc(crc_merged);
                    }
                    entry.write_to(&mut storage).await?;
                    crc = Hasher::new();
                }
                offset += entry_length as u64;
            }

            storage.flush().await?;
            storage.stop_merge().await?;
            storage.flush().await?;
        }
        init_store(self).await?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
enum SnapshotBoundary {
    Timestamp(u64),
    Offset(u64),
}

/// A transactional snapshot of a store at a particular point in time that caches all reads and
/// buffers all writes in memory.
///
/// A transaction is a snapshot of the store at the point in time when the transaction was started.
/// New values can be added inside the transaction, but writes from other transactions are isolated
/// from the current transaction. Reads are cached for each transaction, so that multiple reads of
/// the same key (and version) only have to access storage once. Writes are only persisted at the
/// end of a successful transaction, until then all writes simply mutate an in-memory `HashMap`.
///
/// Transactions provide some basic ACID guarantees and must be
/// [serializable](https://en.wikipedia.org/wiki/Serializability), meaning that a transaction can
/// only be committed if it does not conflict with a previously committed transaction. If a
/// transaction `t1` reads any key-value pair (even a version with an older timestamp) that is
/// modified and committed in a later transaction `t2` before `t1` is comitted, `t1` will fail with
/// an [`Error::TransactionConflict`] and must be explicitly rerun by user of the store. In other
/// words, the following transaction behaviour will lead to a conflict:
///
/// ```text
/// +- t1: -------+
/// | read key1   |   +- t2 --------+
/// |             |   | write key1  |
/// |             |   | commit: ok  |
/// | write key1  |   +-------------+
/// | commit: err |
/// +-------------+
/// ```
pub struct Snapshot<'a, S: Storage> {
    store: &'a KvStore<S>,
    snapshot_timestamp: u64,
    latest_timestamp: u64,
    latest_offset: u64,
    transaction_entries: HashMap<Vec<u8>, Option<Vec<u8>>>,
    cached_entries: SyncMutex<HashMap<Vec<u8>, ValuesByVersion>>,
}

impl<'a, S: Storage> Clone for Snapshot<'a, S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            snapshot_timestamp: self.snapshot_timestamp.clone(),
            latest_timestamp: self.latest_timestamp.clone(),
            latest_offset: self.latest_offset.clone(),
            transaction_entries: self.transaction_entries.clone(),
            cached_entries: SyncMutex::new(self.cached_entries_locked().clone()),
        }
    }
}

type ValuesByVersion = HashMap<Version, Option<Vec<u8>>>;

impl<'a, S: Storage> Snapshot<'a, S> {
    /// Returns the (file-)name of the store associated with this snapshot.
    pub fn name(&self) -> &str {
        self.store.name()
    }

    /// Returns the latest value associated with the key.
    ///
    /// Returns `None` if the key is not found in the store _or if the value associated with the key
    /// has been removed and was thus "moved to trash"_.
    pub async fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let versions = self.versions(k).await?;
        if let Some(version) = versions.last().copied() {
            self.get_version(k, version).await
        } else {
            let mut cached_entries = self.cached_entries_locked();
            cached_entries.insert(k.to_vec(), HashMap::new());
            Ok(None)
        }
    }

    /// Returns the latest _non-removed_ value associated with the key (even if the value was moved
    /// to the trash).
    ///
    /// Returns `None` only if the key is not found in the store _and there is no old version of it
    /// in the store_. If the value associated with the key has been removed from the store but is
    /// still "in the trash", the value in the trash will be returned. In other words, _some_ value
    /// will always be returned unless the key has never been written to the store (since the last
    /// merge).
    pub async fn get_unremoved(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let versions = self.versions(k).await?;
        let unremoved = versions.iter().filter(|v| !v.is_removed);
        if let Some(version) = unremoved.last().copied() {
            self.get_version(k, version).await
        } else {
            let mut cached_entries = self.cached_entries_locked();
            cached_entries.insert(k.to_vec(), HashMap::new());
            Ok(None)
        }
    }

    /// Returns the specified version of the value with the given key.
    pub async fn get_version(&self, k: &[u8], version: Version) -> Result<Option<Vec<u8>>> {
        let mut cached_entries = self.cached_entries_locked();
        if !cached_entries.contains_key(k) {
            cached_entries.insert(k.to_vec(), HashMap::new());
        }
        if let Some(entry) = self.transaction_entries.get(k) {
            if !version.is_committed {
                return Ok(entry.clone());
            }
        }
        let versions = cached_entries.get_mut(k).unwrap();
        if let Some(entry) = versions.get(&version) {
            return Ok(entry.clone());
        }

        if let Some(offset) = version.offset {
            let entry = Entry::read_from(&mut self.store.storage.lock().await, offset).await?;
            versions.insert(version, entry.val.clone());
            Ok(entry.val)
        } else {
            Ok(None)
        }
    }

    /// Returns all versions contained in the store for the given key, ordered from earliest to
    /// latest version.
    ///
    /// Since all keys are stored in memory, this operation is quite fast, as there is no need to
    /// access the persistent storage.
    pub async fn versions(&self, k: &[u8]) -> Result<Vec<Version>> {
        let up_until = self.latest_time_or_offset();
        let mut versions: Vec<Version> =
            versions_up_until(self.store.offsets.lock().await.get(k), up_until)
                .into_iter()
                .map(|v| v.into())
                .collect();
        if let Some(entry) = self.transaction_entries.get(k) {
            versions.push(Version {
                offset: None,
                is_committed: false,
                is_removed: entry.is_none(),
                timestamp: self.snapshot_timestamp,
            });
        }
        Ok(versions)
    }

    /// Returns the timestamp of the last write to the store (in milliseconds since the Unix epoch).
    pub async fn last_updated(&self) -> Result<Option<u64>> {
        Ok(if !self.transaction_entries.is_empty() {
            Some(self.snapshot_timestamp)
        } else if self.latest_timestamp > 0 {
            Some(self.latest_timestamp)
        } else {
            None
        })
    }

    /// Returns all non-removed keys in the store.
    ///
    /// Since all keys are stored in memory, this operation is quite fast, as there is no need to
    /// access the persistent storage.
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        // TODO: write test for this, as previously only persisted keys were returned
        for (key, value) in self.transaction_entries.iter() {
            if value.is_some() {
                keys.push(key.clone());
            }
        }
        for (key, versions) in self.store.offsets.lock().await.iter() {
            if !self.transaction_entries.contains_key(key) {
                let versions = versions_up_until(Some(versions), self.latest_time_or_offset());
                if !versions.last().unwrap().is_removed {
                    keys.push(key.clone());
                }
            }
        }
        Ok(keys)
    }

    /// Inserts a key-value pair in the store, superseding older versions.
    ///
    /// All inserts are buffered in memory and only persisted at the end of a transaction. If an
    /// insert is later followed by another insert with the same key in the same transaction, only
    /// the second insert is written to storage, as from the point of view of the transaction both
    /// inserts happen at the same time and thus only the last one for each key must be stored as a
    /// new version in the store.
    pub fn insert(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<()> {
        self.transaction_entries.insert(k, Some(v));
        Ok(())
    }

    /// Removes the value associated with the key (and moves it to the trash).
    ///
    /// All removes are buffered in memory and only persisted at the end of the transaction.
    /// Removing a key does not purge the associated value from the store, instead it simply adds a
    /// new version that marks the key as removed, while keeping the old versions of the key
    /// accessible. As a result, this acts like a "move to trash" operation and allows the value to
    /// be restored from the trash if desired. The trash will be emptied when the store is merged,
    /// at which point the removed value will be purged from the store.
    pub fn remove(&mut self, k: Vec<u8>) -> Result<()> {
        self.transaction_entries.insert(k, None);
        Ok(())
    }

    /// Aborts the current transaction, discarding all of its write operations.
    pub async fn abort(mut self) -> Result<()> {
        // Clear transaction explicitly to suppress warning on drop:
        self.transaction_entries.clear();
        Ok(())
    }

    /// Commits the current transaction, persisting all of its write operations as new versions in
    /// the store.
    pub async fn commit(mut self) -> Result<()> {
        let entries = mem::take(&mut self.transaction_entries);
        if entries.is_empty() {
            return Ok(());
        }
        let mut storage = self.store.storage.lock().await;
        let mut offsets = self.store.offsets.lock().await;
        {
            for k in self.cached_entries_locked().keys() {
                if let Some(versions) = offsets.get(k) {
                    let version = versions
                        .last()
                        .unwrap_or_else(|| panic!("could not find last version of key {:?}", k));

                    // the value that was read in this transaction has since been modified by
                    // another transaction and committed, the current transaction is thus in
                    // conflict and cannot be committed
                    if version.offset >= self.latest_offset {
                        return Err(Error::TransactionConflict);
                    }
                }
            }
        }

        let mut crc = Hasher::new();
        let mut uncommitted_offsets = Vec::with_capacity(entries.len());
        for (k, buf) in entries.into_iter() {
            if let Some(buf) = buf {
                let entry = Entry::kv_insert(k, buf)?;
                let offset = entry.write_to(&mut storage).await?;
                entry.update_crc(&mut crc);
                uncommitted_offsets.push((entry.key.unwrap(), offset, false));
            } else {
                let entry = Entry::kv_remove(k)?;
                let offset = entry.write_to(&mut storage).await?;
                entry.update_crc(&mut crc);
                uncommitted_offsets.push((entry.key.unwrap(), offset, true));
            }
        }

        let t_commit = timestamp_now_monotonic(self.latest_timestamp);
        let mut entry = Entry::transaction_commit(t_commit)?;
        entry.update_crc(&mut crc);
        entry.set_crc(crc.finalize());
        entry.write_to(&mut storage).await?;

        for (k, offset, is_removed) in uncommitted_offsets {
            offsets
                .entry(k.to_vec())
                .or_insert_with(Vec::new)
                .push(BlobVersion {
                    offset,
                    is_removed,
                    timestamp: t_commit,
                });
        }
        *self.store.latest_timestamp.lock().await = t_commit;
        storage.flush().await?;
        Ok(())
    }

    fn cached_entries_locked(&self) -> SyncMutexGuard<'_, HashMap<Vec<u8>, ValuesByVersion>> {
        self.cached_entries
            .lock()
            .expect("another thread holding the lock panicked")
    }

    fn latest_time_or_offset(&self) -> SnapshotBoundary {
        if self.snapshot_timestamp == self.latest_timestamp {
            SnapshotBoundary::Offset(self.latest_offset)
        } else {
            SnapshotBoundary::Timestamp(self.latest_timestamp)
        }
    }
}

impl<S: Storage> Drop for Snapshot<'_, S> {
    fn drop(&mut self) {
        if !self.transaction_entries.is_empty() {
            warn!("Snapshot with changes was dropped without being committed!");
        }
    }
}

async fn init_store<S: Storage>(store: &mut KvStore<S>) -> Result<()> {
    let mut uncommitted = Vec::new();
    let mut crc = Hasher::new();
    let mut latest_timestamp = 0;
    let mut offset = 0;
    let max_offset = store.len().await;
    while offset < max_offset {
        let entry = Entry::read_from(&mut store.storage.lock().await, offset).await?;
        let entry_length = entry.len() as u64;
        let offsets = &mut (*store.offsets.lock().await);

        if !entry.is_transaction() {
            entry.update_crc(&mut crc);
            uncommitted.push((entry.key.unwrap(), offset, entry.val.is_none()));
        } else if entry.is_transaction_commit() {
            entry.update_crc(&mut crc);
            let crc_kv_writes = crc.finalize();
            let crc_commit = entry.crc()?;
            if crc_kv_writes != crc_commit {
                warn!("Truncating corrupt store at offset {}", offset);
                store
                    .storage
                    .lock()
                    .await
                    .truncate(offset)
                    .await
                    .expect("Error while truncating storage to remove corrupt data");
                break;
            }

            let timestamp_commit = u64_from_bytes(entry.val.as_ref().unwrap())?;
            for (k, offset, is_removed) in uncommitted.iter() {
                offsets
                    .entry(k.to_vec())
                    .or_insert_with(Vec::new)
                    .push(BlobVersion {
                        offset: *offset,
                        is_removed: *is_removed,
                        timestamp: timestamp_commit,
                    });
            }
            uncommitted.clear();
            crc = Hasher::new();
            latest_timestamp = max(latest_timestamp, timestamp_commit);
        }

        offset += entry_length as u64;
    }
    store.latest_timestamp = AsyncMutex::new(latest_timestamp);
    Ok(())
}

#[derive(Debug, Copy, Clone)]
struct BlobVersion {
    offset: u64,
    is_removed: bool,
    timestamp: u64,
}

fn versions_up_until(
    versions: Option<&Vec<BlobVersion>>,
    up_until: SnapshotBoundary,
) -> Vec<BlobVersion> {
    versions.map_or(Vec::new(), |v| {
        v.iter()
            .filter(|v| match up_until {
                SnapshotBoundary::Timestamp(t) => v.timestamp <= t,
                SnapshotBoundary::Offset(o) => v.offset < o,
            })
            .cloned()
            .collect()
    })
}

type Value = Vec<u8>;

#[derive(Debug)]
struct Entry {
    header: u8,
    sizes: Vec<u8>,
    key: Option<Vec<u8>>,
    val: Option<Vec<u8>>,
    crc: Option<Vec<u8>>,
}

// BITS IN THE ENTRY HEADER BYTE:
//
// 0b____000_00_000
//       ||| || \\\__ bytes required to store the value size (0-6 bytes)
//       ||| \\______ bytes required to store the key size (0-3 bytes)
//       \\\_________ flags reserved for later use
impl Entry {
    fn transaction_commit(timestamp: u64) -> Result<Self> {
        let mut buf = vec![0; BYTES_TIMESTAMP_FULL];
        buf.copy_from_slice(&timestamp.to_le_bytes()[..BYTES_TIMESTAMP_FULL]);
        Self::new(None, Some(buf))
    }

    fn kv_insert(k: Vec<u8>, v: Value) -> Result<Self> {
        Self::new(Some(k), Some(v))
    }

    fn kv_remove(k: Vec<u8>) -> Result<Self> {
        Self::new(Some(k), None)
    }

    fn new(k: Option<Vec<u8>>, v: Option<Value>) -> Result<Self> {
        let key_size = k.as_ref().map_or(0, |k| k.len());
        let bytes_key_size = k
            .as_ref()
            .map_or(Ok(0), |k| bytes_required_for(k.len(), 3))?;
        let val_size = v.as_ref().map_or(0, |v| v.len());
        let bytes_val_size = v
            .as_ref()
            .map_or(Ok(0), |v| bytes_required_for(v.len(), 6))?;
        let header = (bytes_key_size << 3) | bytes_val_size;

        let mut sizes = vec![0; (bytes_key_size + bytes_val_size) as usize];
        sizes[..bytes_key_size as usize]
            .copy_from_slice(&key_size.to_le_bytes()[0..bytes_key_size as usize]);
        sizes[bytes_key_size as usize..]
            .copy_from_slice(&val_size.to_le_bytes()[0..bytes_val_size as usize]);

        Ok(Self {
            header,
            sizes,
            key: k,
            val: v,
            crc: None,
        })
    }

    async fn read_from<S: Storage>(
        storage: &mut AsyncMutexGuard<'_, S>,
        offset: u64,
    ) -> Result<Self> {
        let max_length_of_header_and_sizes = 1 + 3 + 6;
        let mut header_and_sizes = storage.read(offset, max_length_of_header_and_sizes).await?;
        if header_and_sizes.is_empty() {
            return Err(Error::InvalidEntryError {
                reason: "Offset exceeds storage bounds".to_string(),
            });
        }
        let header = header_and_sizes.remove(0);
        let mut sizes = header_and_sizes;
        let bytes_val_size = (header & 0b111) as u32;
        let bytes_key_size = ((header & 0b11000) >> 3) as u32;
        if bytes_key_size > 3 {
            return Err(Error::InvalidEntryError {
                reason: format!(
                    "Key size can have a maximum of 3 bytes, but has {}",
                    bytes_key_size
                ),
            });
        }
        if bytes_val_size > 6 {
            return Err(Error::InvalidEntryError {
                reason: format!(
                    "Value size can have a maximum of 6 bytes, but has {}",
                    bytes_val_size
                ),
            });
        }

        let offset_sizes = offset + 1;
        let bytes_sizes = bytes_key_size + bytes_val_size;
        if sizes.len() < bytes_key_size as usize {
            return Err(Error::InvalidEntryError {
                reason: "Invalid length of entry size buffer".to_string(),
            });
        }
        sizes.truncate(bytes_sizes as usize);

        let key_size = u32_from_bytes(&sizes[..bytes_key_size as usize])?;
        let val_size = u32_from_bytes(&sizes[bytes_key_size as usize..])?;
        let offset_content = offset_sizes + bytes_sizes as u64;

        if key_size > (1 << 16) {
            return Err(Error::InvalidEntryError {
                reason: "Key size is > max size of 2^16 bytes".to_string(),
            });
        }
        if val_size > (1 << 24) {
            return Err(Error::InvalidEntryError {
                reason: "Value size is > max size of 2^24 bytes".to_string(),
            });
        }

        let is_transaction = bytes_key_size == 0;
        let (key, val, crc) = if is_transaction {
            if bytes_val_size > 0 {
                let bytes_content = key_size + val_size + BYTES_CRC as u32;
                let content = storage.read(offset_content, bytes_content).await?;
                if content.len() < val_size as usize {
                    return Err(Error::InvalidEntryError {
                        reason: "Invalid length of entry content buffer".to_string(),
                    });
                }
                (
                    None,
                    Some(content[..val_size as usize].to_vec()),
                    Some(content[val_size as usize..].to_vec()),
                )
            } else {
                (None, None, None)
            }
        } else {
            let bytes_content = key_size + val_size;
            let content = storage.read(offset_content, bytes_content).await?;
            if content.len() < key_size as usize {
                return Err(Error::InvalidEntryError {
                    reason: "Invalid length of entry content buffer".to_string(),
                });
            }
            let key = Some(content[..key_size as usize].to_vec());
            if bytes_val_size > 0 {
                (key, Some(content[key_size as usize..].to_vec()), None)
            } else {
                (key, None, None)
            }
        };
        Ok(Self {
            header,
            sizes,
            key,
            val,
            crc,
        })
    }

    async fn write_to<S: Storage>(&self, storage: &mut AsyncMutexGuard<'_, S>) -> Result<u64> {
        let offset = storage.write(&[self.header]).await?;
        storage.write(&self.sizes).await?;
        if let Some(k) = &self.key {
            storage.write(k).await?;
        }
        if let Some(v) = &self.val {
            storage.write(v).await?;
        }
        if let Some(crc) = &self.crc {
            storage.write(crc).await?;
        }
        Ok(offset)
    }

    fn set_crc(&mut self, crc: u32) {
        self.crc = Some(crc.to_le_bytes().to_vec());
    }

    fn crc(&self) -> Result<u32> {
        assert!(
            self.is_transaction_commit(),
            "Trying to read CRC value from a non-commit entry"
        );
        u32_from_bytes(self.crc.as_ref().unwrap())
    }

    fn update_crc(&self, crc: &mut Hasher) {
        crc.update(&[self.header]);
        crc.update(&self.sizes);
        if let Some(k) = &self.key {
            crc.update(k);
        }
        if let Some(v) = &self.val {
            crc.update(v);
        }
    }

    fn len(&self) -> usize {
        1 + self.sizes.len()
            + self.key.as_ref().map_or(0, |k| k.len())
            + self.val.as_ref().map_or(0, |v| v.len())
            + self.crc.as_ref().map_or(0, |crc| crc.len())
    }

    fn is_transaction(&self) -> bool {
        self.key.is_none()
    }

    fn is_transaction_commit(&self) -> bool {
        self.is_transaction() && self.val.as_ref().is_some()
    }
}

fn u64_from_bytes(bytes: &[u8]) -> Result<u64> {
    if bytes.len() > 8 {
        Err(Error::InvalidIntLength {
            bytes_expected: 8,
            bytes_found: bytes.len() as u8,
        })
    } else {
        let mut buf = [0; 8];
        buf[..bytes.len()].copy_from_slice(bytes);
        Ok(u64::from_le_bytes(buf))
    }
}

fn u32_from_bytes(bytes: &[u8]) -> Result<u32> {
    if bytes.len() > 4 {
        Err(Error::InvalidIntLength {
            bytes_expected: 4,
            bytes_found: bytes.len() as u8,
        })
    } else {
        let mut buf = [0; 4];
        buf[..bytes.len()].copy_from_slice(bytes);
        Ok(u32::from_le_bytes(buf))
    }
}

fn bytes_required_for(n: usize, max_bytes: u8) -> Result<u8> {
    let zero: usize = 0;
    let bit_length = zero.leading_zeros() - n.leading_zeros();
    let mut bytes_required = (bit_length / 8) as u8;
    if bit_length == 0 || bit_length % 8 != 0 {
        bytes_required += 1;
    };
    if bytes_required > max_bytes {
        Err(Error::MaxSizeExceeded {
            size: n,
            max_bytes,
            bytes_required,
        })
    } else {
        Ok(bytes_required)
    }
}

/// A version of a key-value pair in a store at a particular point in time.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Version {
    offset: Option<u64>,
    /// True if the entry is persisted in the store, false if not yet committed.
    pub is_committed: bool,
    /// True if the key is removed ("moved to trash"), false otherwise.
    pub is_removed: bool,
    /// Timestamp of the version in milliseconds since the Unix epoch.
    pub timestamp: u64,
}

impl From<BlobVersion> for Version {
    fn from(v: BlobVersion) -> Self {
        Self {
            offset: Some(v.offset),
            is_committed: true,
            is_removed: v.is_removed,
            timestamp: v.timestamp,
        }
    }
}

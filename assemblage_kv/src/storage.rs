//! A storage backend abstraction for kv stores, similar to an append-only file.
pub mod file_storage;
pub mod memory_storage;
pub mod web_storage;

use async_trait::async_trait;
use std::io;

#[cfg(target_arch = "wasm32")]
pub use web_storage::WebStorage;

#[cfg(not(target_arch = "wasm32"))]
pub use file_storage::FileStorage;

pub use memory_storage::MemoryStorage;

/// Opens a web storage with the specified name (and creates it if none exists).
#[cfg(target_arch = "wasm32")]
pub async fn open(name: impl Into<String>) -> Result<WebStorage> {
    WebStorage::open(name).await
}

/// Opens a file storage with the specified name (and creates it if none
/// exists).
#[cfg(not(target_arch = "wasm32"))]
pub async fn open(name: impl Into<String>) -> Result<FileStorage> {
    FileStorage::open(name).await
}

/// Deletes the web storage.
#[cfg(target_arch = "wasm32")]
pub async fn purge(name: impl Into<String>) -> Result<()> {
    WebStorage::purge(name).await
}

/// Deletes the file storage.
#[cfg(not(target_arch = "wasm32"))]
pub async fn purge(name: impl Into<String>) -> Result<()> {
    FileStorage::purge(name).await
}

/// The storage implementation used on a particular target_arch.
///
/// Will be WebStorage](web_storage::WebStorage) on wasm,
/// `file_storage::FileStorage` on native.
#[cfg(target_arch = "wasm32")]
pub type PlatformStorage = WebStorage;

/// The storage implementation used on a particular target_arch.
///
/// Will be `web_storage::WebStorage` on wasm,
/// [FileStorage](file_storage::FileStorage) on native.
#[cfg(not(target_arch = "wasm32"))]
pub type PlatformStorage = FileStorage;

/// A specialized `Result` type for storage operations.
pub type Result<T> = std::result::Result<T, Error>;

/// A (mostly) append-only storage backend for stores.
#[async_trait(?Send)]
pub trait Storage: Sized {
    /// Opens a storage with the specified name (and creates it if none exists).
    async fn open<'a>(name: impl Into<String> + 'a) -> Result<Self>;

    /// Deletes the storage and all its contents.
    async fn purge<'a>(name: impl Into<String> + 'a) -> Result<()>;

    /// Returns the name of the storage.
    fn name(&self) -> &str;

    /// Returns the total length of the storage in bytes.
    fn len(&self) -> u64;

    /// Reads the specified number of bytes starting at the specified offset.
    ///
    /// If the storage ends before the expected number of bytes could be read,
    /// the rest of the resulting bytes will all be 0, but no error is returned.
    async fn read(&mut self, offset: u64, bytes: u32) -> Result<Vec<u8>>;

    /// Appends the buffer to the end of the storage.
    async fn write(&mut self, buffer: &[u8]) -> Result<u64>;

    /// Truncates the storage to the specified length.
    ///
    /// Bytes after the specified offset do not have to be zeroed. Later writes
    /// will eventually overwrite these bytes, so it is up to the storage
    /// implementations whether zeroing takes place or not.
    async fn truncate(&mut self, offset: u64) -> Result<()>;

    /// Flushes all writes to disk.
    async fn flush(&mut self) -> Result<()>;

    /// Starts merge mode.
    ///
    /// When a storage enters merge mode, writes are "forked" and all later
    /// writes will not be written to the current storage, but rather to a
    /// temporary second storage that stores the resulting writes of the merge
    /// until the merge is done. All reads continue to read from the current
    /// storage, _not_ the temporary merge storage.
    async fn start_merge(&mut self) -> Result<()>;

    /// Stops merge mode.
    ///
    /// When a storage exists merge mode, the current storage will be replaced
    /// with the temporary merge storage and the pre-merge storage is deleted.
    /// From then on, all operations read from and write to the same (now
    /// merged) storage again, until the next merge is started.
    async fn stop_merge(&mut self) -> Result<()>;

    /// Checks whether the storage is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The error type for storage operations (wraps [`std::io::Error`]).
#[derive(Debug)]
pub enum Error {
    /// Caused by file IO, only returned by file storage.
    IoError(io::Error),
    /// Caused by IndexedDB operations, only returned by web storage.
    WebError(String),
    /// Caused by an offset greater than than the current storage length.
    OffsetError {
        /// The offset requested / expected by the operation.
        offset: u64,
        /// The maximum length and thus the maximum possible offset.
        max_length: u64,
    },
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IoError(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<Error> for wasm_bindgen::JsValue {
    fn from(e: Error) -> Self {
        wasm_bindgen::JsValue::from_str(&format!("{:?}", e))
    }
}

// Merging the following 2 macros into a single one would be cleaner, but leads
// to rust-analyzer flagging the generated code as inactive, since the wasm32
// target is not checked.

/// Allows the same test to be used on both native and wasm target_archs.
#[macro_export]
#[cfg(target_arch = "wasm32")]
macro_rules! hybrid_test {
    ($test_name:ident, $ret:ty, $test:block) => {
        #[wasm_bindgen_test::wasm_bindgen_test]
        async fn $test_name() -> () {
            if cfg!(test) {
                let _ignored = console_log::init();
            }
            async fn $test_name() -> $ret {
                $test
            }
            $test_name().await.unwrap();
        }
    };
}

/// Allows the same test to be used on both native and wasm target_archs.
#[macro_export]
#[cfg(not(target_arch = "wasm32"))]
macro_rules! hybrid_test {
    ($test_name:ident, $ret:ty, $test:block) => {
        #[test]
        fn $test_name() -> $ret {
            if cfg!(test) {
                let _ignored = env_logger::builder()
                    .is_test(true)
                    .filter_level(log::LevelFilter::Info)
                    .try_init();
            }
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async { $test })
        }
    };
}

/// Creates a new storage, passes it to an async block and runs it.
///
/// Mostly used to quickly build an execution environment for doc tests.
///
/// # Examples
///
/// ```
/// use assemblage_kv::{run, storage::Storage, Result};
///
/// fn main() -> Result<()> {
///     run!(async |storage| {
///         assert_eq!(storage.len(), 0);
///         Ok(())
///     })
/// }
/// ```
#[macro_export]
#[cfg(not(target_arch = "wasm32"))]
macro_rules! run {
    (async |$storage:ident| $b:block) => {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let file = std::path::Path::new(file!())
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap();
            let name = format!("{}_{}", file, line!());
            assemblage_kv::storage::purge(&name).await?;

            let mut $storage = assemblage_kv::storage::open(&name).await?;
            let ret = $b;

            assemblage_kv::storage::purge(&name).await?;
            ret
        })
    };

    (async || $b:block) => {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { $b })
    };
}

/// Wraps test fns using `hybrid-test!` and automatically chooses the right
/// storage implementation for the target_arch.
#[macro_export]
macro_rules! test {
    (async fn $test_name:ident($storage:ident) -> $ret:ty $test:block) => {
        assemblage_kv::hybrid_test!($test_name, $ret, {
            let file = std::path::Path::new(file!()).file_stem().unwrap().to_str().unwrap();
            let name = format!("{}_{}", file, line!());
            assemblage_kv::storage::purge(&name).await?;

            let mut $storage = assemblage_kv::storage::open(&name).await?;
            $test

            assemblage_kv::storage::purge(&name).await?;
            Ok(())
        });
    };

    (async fn $test_name:ident() -> $ret:ty $test:block) => {
        assemblage_kv::hybrid_test!($test_name, $ret, {
            $test
            Ok(())
        });
    };
}

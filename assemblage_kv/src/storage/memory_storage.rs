//! An in-memory storage backed by a `Vec<u8>`.

use super::{Error, Result, Storage};
use async_trait::async_trait;

/// An in-memory storage backed by a `Vec<u8>`.
pub struct MemoryStorage {
    name: String,
    bytes: Vec<u8>,
    bytes_for_merge: Option<Vec<u8>>,
}

impl MemoryStorage {
    /// Creates an empty in-memory storage with the name `""`.
    pub fn new() -> Self {
        Self::from(Vec::new())
    }

    /// Creates an in-memory storage filled with the specified bytes and the
    /// name `""`.
    pub fn from(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            name: "".to_string(),
            bytes: bytes.into(),
            bytes_for_merge: None,
        }
    }

    /// Consumes the store and returns its contents.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl Storage for MemoryStorage {
    async fn open<'a>(name: impl Into<String> + 'a) -> Result<Self> {
        Ok(Self {
            name: name.into(),
            bytes: Vec::new(),
            bytes_for_merge: None,
        })
    }

    async fn purge<'a>(_name: impl Into<String> + 'a) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    async fn read(&mut self, offset: u64, bytes: u32) -> Result<Vec<u8>> {
        let end = offset + bytes as u64;
        if end <= self.bytes.len() as u64 {
            Ok(self.bytes[offset as usize..(offset + bytes as u64) as usize].to_vec())
        } else {
            Err(Error::OffsetError {
                offset: end,
                max_length: self.bytes.len() as u64,
            })
        }
    }

    async fn write(&mut self, buffer: &[u8]) -> Result<u64> {
        let bytes = self.bytes_for_merge.as_mut().unwrap_or(&mut self.bytes);
        bytes.extend(buffer);
        Ok((bytes.len() - buffer.len()) as u64)
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        self.bytes.resize(offset as usize, 0);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn start_merge(&mut self) -> Result<()> {
        self.bytes_for_merge = Some(Vec::new());
        Ok(())
    }

    async fn stop_merge(&mut self) -> Result<()> {
        self.bytes = self.bytes_for_merge.take().unwrap_or_default();
        Ok(())
    }
}

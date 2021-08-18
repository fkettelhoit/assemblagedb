//! A storage backend for stores backed by async file IO.
#![cfg(not(target_arch = "wasm32"))]

use super::{Error, Result, Storage};

use async_trait::async_trait;
use std::{cmp::min, convert::TryInto, io, path::Path};
use tokio::fs::{remove_file, rename, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// A storage backend for stores backed by a file.
pub struct FileStorage {
    name: String,
    len_read: u64,
    len_write: u64,
    file: File,
    merge_file: Option<File>,
}

#[async_trait(?Send)]
impl Storage for FileStorage {
    async fn open<'a>(name: impl Into<String> + 'a) -> Result<Self> {
        let name = name.into();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_name(&name))
            .await?;
        let file_length = file.metadata().await?.len();
        Ok(Self {
            name,
            len_read: file_length,
            len_write: file_length,
            file,
            merge_file: None,
        })
    }

    async fn purge<'a>(name: impl Into<String> + 'a) -> Result<()> {
        let path = name.into() + ".aeon";
        if Path::new(&path).exists() {
            remove_file(&path).await?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn len(&self) -> u64 {
        self.len_read
    }

    async fn read(&mut self, offset: u64, bytes: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0; bytes.try_into().unwrap()];
        let bytes_to_read = min(bytes, (self.len_read - offset) as u32);
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file
            .read_exact(&mut buf[..bytes_to_read as usize])
            .await?;
        Ok(buf)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<u64> {
        let file = self.merge_file.as_mut().unwrap_or(&mut self.file);
        let offset = self.len_write;
        file.seek(io::SeekFrom::Start(offset)).await?;
        file.write_all(buf).await?;
        file.flush().await?;
        self.len_write += buf.len() as u64;
        if self.merge_file.is_none() {
            self.len_read += buf.len() as u64;
        }
        Ok(offset)
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        let max_length = self.len();
        if offset > max_length {
            Err(Error::OffsetError { offset, max_length })
        } else {
            self.file.set_len(offset).await?;
            self.len_write = offset;
            if self.merge_file.is_none() {
                self.len_read = offset;
            }
            self.flush().await?;
            Ok(())
        }
    }

    async fn flush(&mut self) -> Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        Ok(())
    }

    async fn start_merge(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(merge_file_name(&self.name))
            .await?;
        self.merge_file = Some(file);
        self.len_write = 0;
        Ok(())
    }

    async fn stop_merge(&mut self) -> Result<()> {
        let src = merge_file_name(&self.name);
        let dst = file_name(&self.name);
        rename(src, dst).await?;
        self.file = self.merge_file.take().unwrap();
        self.len_read = self.len_write;
        Ok(())
    }
}

fn file_name(name: &str) -> String {
    String::from(name) + ".aeon"
}

fn merge_file_name(name: &str) -> String {
    String::from(name) + ".aeon.merged"
}

//! A storage backend built on top of IndexedDB.
#![cfg(target_arch = "wasm32")]

use super::{Error, Result, Storage};
use async_trait::async_trait;
use js_sys::{Array, Uint8Array};
use std::{cmp::min, collections::HashMap, future::Future, pin::Pin, task::Context, task::Poll};
use wasm_bindgen::{prelude::*, JsCast};
use web_sys::{
    window, DomException, IdbDatabase, IdbOpenDbRequest, IdbRequest, IdbRequestReadyState,
    IdbTransaction, IdbTransactionMode, IdbVersionChangeEvent,
};

const BLOCK_SIZE: usize = 1 << 14; // 16 KB

const CONTENT_STORE_A: &str = "a";
const CONTENT_STORE_B: &str = "b";
const META_STORE: &str = "meta";

const META_FIELD_LENGTH: &str = "length";
const META_FIELD_ACTIVE_STORE: &str = "store";

#[derive(Debug, Copy, Clone)]
enum ActiveStore {
    A = 0,
    B = 1,
}

impl ActiveStore {
    fn name(&self) -> &str {
        match self {
            ActiveStore::A => CONTENT_STORE_A,
            ActiveStore::B => CONTENT_STORE_B,
        }
    }
}

impl From<wasm_bindgen::JsValue> for Error {
    fn from(e: wasm_bindgen::JsValue) -> Self {
        Error::WebError(format!("{:?}", e))
    }
}

impl From<Option<DomException>> for Error {
    fn from(e: Option<DomException>) -> Self {
        Error::WebError(e.map_or(String::from(""), |e| {
            format!("{}: {}", e.name(), e.message())
        }))
    }
}

struct AsyncIdbRequest {
    req: IdbRequest,
    callback: Option<Closure<dyn FnMut()>>,
}

impl AsyncIdbRequest {
    fn from(req: impl Into<IdbRequest>) -> Self {
        Self {
            req: req.into(),
            callback: None,
        }
    }
}

impl Future for AsyncIdbRequest {
    type Output = std::result::Result<JsValue, Option<DomException>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.req.ready_state() {
            IdbRequestReadyState::Pending => {
                let waker = cx.waker().clone();
                let callback = Closure::once(Box::new(move || {
                    waker.wake();
                }) as Box<dyn FnOnce()>);
                self.req
                    .set_onsuccess(Some(callback.as_ref().unchecked_ref()));
                self.req
                    .set_onerror(Some(callback.as_ref().unchecked_ref()));
                self.callback = Some(callback);
                Poll::Pending
            }
            IdbRequestReadyState::Done => Poll::Ready(match self.req.result() {
                Ok(r) => Ok(r),
                Err(_) => Err(self.req.error().expect("error is not an exception")),
            }),
            _ => panic!("invalid ready state"),
        }
    }
}

/// A storage backend built on top of IndexedDB.
pub struct WebStorage {
    name: String,
    db: IdbDatabase,
    offset_buffer: u64,
    blocks: HashMap<u64, HashMap<u64, Uint8Array>>,
    offsets: HashMap<u64, u64>,
    store_for_reads: ActiveStore,
    store_for_writes: ActiveStore,
}

impl WebStorage {
    fn stores(&self, stores: &[&str], mode: IdbTransactionMode) -> Result<IdbTransaction> {
        let seq = Array::new();
        for store in stores {
            seq.push(&JsValue::from_str(*store));
        }
        Ok(self.db.transaction_with_str_sequence_and_mode(&seq, mode)?)
    }

    fn offset_store(&self, store: u64) -> u64 {
        *self
            .offsets
            .get(&store)
            .expect("Could not find offset of store")
    }

    async fn read_block(&mut self, store: ActiveStore, offset: u64) -> Result<&Uint8Array> {
        let store_number = store as u64;
        let store_name = store.name();
        let offset_max = self.offset_store(store_number);
        let blocks = self.blocks.entry(store_number).or_insert(HashMap::new());
        let block_offset = offset - (offset % BLOCK_SIZE as u64);
        if !blocks.contains_key(&block_offset) {
            if offset < offset_max {
                let t = self.db.transaction_with_str(store_name)?;
                let store = t.object_store(store_name)?;
                let req = store.get(&JsValue::from_f64(offset as f64))?;
                let block: Uint8Array = AsyncIdbRequest::from(req)
                    .await?
                    .dyn_into()
                    .expect("block is not a Uint8Array");
                blocks.insert(block_offset, block);
            } else {
                blocks.insert(block_offset, Uint8Array::new_with_length(BLOCK_SIZE as u32));
            }
        }
        Ok(&blocks[&block_offset])
    }
}

#[async_trait(?Send)]
impl Storage for WebStorage {
    async fn open<'a>(name: impl Into<String> + 'a) -> Result<Self> {
        let name = name.into();
        let window = window().expect("no global `window` exists");
        let factory = window
            .indexed_db()?
            .expect("could not find IndexedDB factory");
        let open_req = factory.open(&name)?;

        let onupgradeneeded = move |event: IdbVersionChangeEvent| {
            let req: IdbOpenDbRequest = event
                .target()
                .expect("version change event does not have an event target")
                .dyn_into()
                .expect("event target is not an open request");
            let idb: IdbDatabase = req
                .result()
                .expect("result of request is missing")
                .dyn_into()
                .expect("result of request is not a database");
            idb.create_object_store(CONTENT_STORE_A)
                .expect("cannot create content store");
            idb.create_object_store(CONTENT_STORE_B)
                .expect("cannot create content store");
            idb.create_object_store(META_STORE)
                .expect("cannot create meta store");
        };
        let onupgradeneeded =
            Closure::wrap(Box::new(onupgradeneeded) as Box<dyn FnMut(IdbVersionChangeEvent)>);
        open_req.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));

        let db: IdbDatabase = AsyncIdbRequest::from(open_req)
            .await?
            .dyn_into()
            .expect("idb request result is not an idb database");

        let t = db.transaction_with_str(META_STORE)?;
        let meta = t.object_store(META_STORE)?;

        let req = meta.get(&JsValue::from_str(META_FIELD_LENGTH))?;
        let length = AsyncIdbRequest::from(req)
            .await?
            .as_f64()
            .map_or(0, |x| x as u64);

        let req = meta.get(&JsValue::from_str(META_FIELD_ACTIVE_STORE))?;
        let active_store =
            AsyncIdbRequest::from(req)
                .await?
                .as_f64()
                .map_or(ActiveStore::A, |store| match store as u8 {
                    0 => ActiveStore::A,
                    1 => ActiveStore::B,
                    _ => panic!("invalid store number: '{}'", store),
                });

        let mut offsets = HashMap::new();
        offsets.insert(active_store as u64, length);

        Ok(Self {
            db,
            offset_buffer: length,
            name,
            blocks: HashMap::new(),
            offsets,
            store_for_reads: active_store,
            store_for_writes: active_store,
        })
    }

    async fn purge<'a>(name: impl Into<String> + 'a) -> Result<()> {
        let storage = WebStorage::open(name).await?;
        let mode = IdbTransactionMode::Readwrite;
        let t = storage.stores(&[META_STORE, CONTENT_STORE_A, CONTENT_STORE_B], mode)?;
        AsyncIdbRequest::from(t.object_store(META_STORE)?.clear()?).await?;
        AsyncIdbRequest::from(t.object_store(CONTENT_STORE_A)?.clear()?).await?;
        AsyncIdbRequest::from(t.object_store(CONTENT_STORE_B)?.clear()?).await?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn len(&self) -> u64 {
        if self.store_for_reads as u64 == self.store_for_writes as u64 {
            self.offset_buffer
        } else {
            self.offset_store(self.store_for_reads as u64)
        }
    }

    async fn read(&mut self, mut offset: u64, bytes: u32) -> Result<Vec<u8>> {
        let bytes = bytes as usize;
        let mut buf = vec![0; bytes];
        let mut bytes_read = 0;
        while bytes_read < bytes {
            let block_offset = offset - (offset % BLOCK_SIZE as u64);
            let block_start = (offset - block_offset) as usize;
            let bytes_to_read = min(bytes - bytes_read, BLOCK_SIZE - block_start);
            let block_end = block_start + bytes_to_read;
            let buf_end = bytes_read + bytes_to_read;
            self.read_block(self.store_for_reads, block_offset)
                .await?
                .slice(block_start as u32, block_end as u32)
                .copy_to(&mut buf[bytes_read..buf_end]);
            bytes_read += bytes_to_read;
            offset += bytes_to_read as u64;
        }
        Ok(buf)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<u64> {
        let bytes = buf.len();
        let offset = self.offset_buffer;
        let mut bytes_written = 0;
        while bytes_written < bytes {
            let block_offset = self.offset_buffer - (self.offset_buffer % BLOCK_SIZE as u64);
            let block_start = (self.offset_buffer - block_offset) as usize;
            let bytes_to_write = min(bytes - bytes_written, BLOCK_SIZE - block_start);
            let block = self.read_block(self.store_for_writes, block_offset).await?;
            for i in 0..bytes_to_write {
                block.set_index((block_start + i) as u32, buf[bytes_written + i]);
            }
            bytes_written += bytes_to_write;
            self.offset_buffer += bytes_to_write as u64;
        }
        Ok(offset)
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        let max_length = self.len();
        if offset > max_length {
            return Err(Error::OffsetError { offset, max_length });
        }
        let block_offset = offset - (offset % BLOCK_SIZE as u64);
        let block_start = offset - block_offset;
        let block = self.read_block(self.store_for_writes, block_offset).await?;
        block.fill(0, block_start as u32, BLOCK_SIZE as u32);
        for (_k, blocks) in self.blocks.iter_mut() {
            blocks.retain(|k, _v| *k <= block_offset);
        }
        self.offset_buffer = offset;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let offset_store = self.offset_store(self.store_for_writes as u64);
        if offset_store == self.offset_buffer {
            return Ok(());
        }
        let t = self.stores(
            &vec![self.store_for_writes.name(), META_STORE],
            IdbTransactionMode::Readwrite,
        )?;
        let content = t.object_store(self.store_for_writes.name())?;
        if offset_store < self.offset_buffer {
            for (k, v) in self
                .blocks
                .entry(self.store_for_writes as u64)
                .or_insert(HashMap::new())
                .iter()
                .filter(|(k, _v)| **k + BLOCK_SIZE as u64 > offset_store)
            {
                let k = JsValue::from_f64(*k as f64);
                AsyncIdbRequest::from(content.put_with_key(v, &k)?).await?;
            }
        } else {
            let block_offset = self.offset_buffer - (self.offset_buffer % BLOCK_SIZE as u64);
            let block_start = self.offset_buffer - block_offset;
            let block = self.read_block(self.store_for_writes, block_offset).await?;
            block.fill(0, block_start as u32, BLOCK_SIZE as u32);
            for offset in (block_offset..offset_store).step_by(BLOCK_SIZE) {
                AsyncIdbRequest::from(content.delete(&JsValue::from_f64(offset as f64))?).await?;
            }
            let k = JsValue::from_f64(block_offset as f64);
            AsyncIdbRequest::from(content.put_with_key(block, &k)?).await?;
        }
        let meta = t.object_store(META_STORE)?;
        let k = JsValue::from_str(META_FIELD_LENGTH);
        let v = JsValue::from_f64(self.offset_buffer as f64);
        AsyncIdbRequest::from(meta.put_with_key(&v, &k)?).await?;
        self.offsets
            .insert(self.store_for_writes as u64, self.offset_buffer);
        self.blocks.clear();
        Ok(())
    }

    async fn start_merge(&mut self) -> Result<()> {
        self.store_for_writes = match self.store_for_writes {
            ActiveStore::A => ActiveStore::B,
            ActiveStore::B => ActiveStore::A,
        };
        self.offsets.insert(self.store_for_writes as u64, 0);
        self.offset_buffer = 0;
        Ok(())
    }

    async fn stop_merge(&mut self) -> Result<()> {
        self.flush().await?;
        self.store_for_reads = self.store_for_writes;
        self.offsets.insert(
            self.store_for_reads as u64,
            *self.offsets.get(&(self.store_for_writes as u64)).unwrap(),
        );
        let t = self
            .db
            .transaction_with_str_and_mode(META_STORE, IdbTransactionMode::Readwrite)?;
        let meta = t.object_store(META_STORE)?;

        let k = JsValue::from_str(META_FIELD_ACTIVE_STORE);
        let v = JsValue::from_f64(self.store_for_reads as u32 as f64);
        AsyncIdbRequest::from(meta.put_with_key(&v, &k)?).await?;

        let k = JsValue::from_str(META_FIELD_LENGTH);
        let v = JsValue::from_f64(self.offset_buffer as f64);
        AsyncIdbRequest::from(meta.put_with_key(&v, &k)?).await?;

        Ok(())
    }
}

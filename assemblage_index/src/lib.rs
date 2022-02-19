use std::{cell::RefCell, collections::HashMap, hash::Hash, rc::Rc};

use crate::data::{Node, Parent, Result};
use assemblage_kv::{
    storage::{MemoryStorage, Storage},
    KvStore,
};
use data::{Id, Match, Parents};

pub mod data;
pub mod sequitur;

pub struct Db<S: Storage> {
    kv: KvStore<S>,
}

impl<S: Storage> Db<S> {
    /// Opens and reads a DB from storage or creates it if none exists.
    ///
    /// If the storage is empty, a new empty list with page layout will be
    /// automatically added as the root node of the DB.
    pub async fn open(storage: S) -> Result<Self> {
        let db = Self {
            kv: KvStore::open(storage).await?,
        };
        if db.kv.is_empty().await {
            let root = Node::List(vec![]);
            let id = Id::root();
            let mut snapshot = db.current().await;
            snapshot.insert_node(id, root)?;
            snapshot.insert_parents(id, Parents::empty())?;
            snapshot.commit().await?;
        }
        Ok(db)
    }

    /// Returns a transactional snapshot of the DB at the current point in time.
    ///
    /// A transaction is a snapshot of the DB at the point in time when the
    /// transaction was started. Nodes can be read, added and modified inside
    /// the transaction, but writes from other transactions are isolated from
    /// the current transaction. Reads are cached for each transaction, so that
    /// multiple reads of the same node(s) only have to access storage once.
    /// Writes are only persisted at the end of a successful transaction, until
    /// then all writes simply mutate in-memory data structures.
    pub async fn current(&self) -> Snapshot<'_, S> {
        Snapshot {
            kv: self.kv.current().await,
        }
    }
}

impl Db<MemoryStorage> {
    pub async fn try_from(content_type: ContentType, bytes: Vec<u8>) -> Result<Self> {
        let storage = MemoryStorage::new();
        let db = Db::open(storage).await?;

        let id = Id::new();
        let mut snapshot = db.current().await;

        for (i, byte) in bytes.iter().enumerate() {
            let parents = Parents::from(vec![Parent::new(id, i as u32)]);
            snapshot.insert_indexed(content_type, *byte, parents)?;
        }

        let node = Node::Content(bytes);
        snapshot.insert_node(id, node)?;

        snapshot.insert_parents(id, Parents::empty())?;

        snapshot.commit().await?;
        Ok(db)
    }
}

pub struct Snapshot<'a, S: Storage> {
    kv: assemblage_kv::Snapshot<'a, S>,
}

impl<'a, S: Storage> Snapshot<'a, S> {
    pub async fn import<S2: Storage>(&mut self, other: &Snapshot<'a, S2>) -> Result<Id> {
        for k in other.kv.keys().await? {
            if k.starts_with(&[KvKeyPrefix::Index as u8]) {
                if let &[_prefix, ty1, ty2, byte] = k.as_slice() {
                    let content_type = ContentType::from([ty1, ty2]);
                    if let Some(indexed_self) = self.get_indexed(content_type, byte).await? {
                        let indexed_other = other
                            .get_indexed(content_type, byte)
                            .await?
                            .expect("key does not exist or has been removed");
                        // ??
                    }
                } else {
                    return todo!();
                }
            }
        }
        todo!()
    }

    pub async fn search(&self, term: &[u8]) -> Result<Vec<Match>> {
        todo!()
    }

    pub async fn get_similar(&self, id: Id) -> Result<Vec<Match>> {
        todo!()
    }

    /// Commits the current transaction, thereby persisting all of its changes.
    pub async fn commit(self) -> Result<()> {
        Ok(self.kv.commit().await?)
    }

    fn insert_node(&mut self, id: Id, node: Node) -> Result<()> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Node as u8);
        k.extend_from_slice(id_bytes);

        let v = node.into();
        Ok(self.kv.insert(k, v)?)
    }

    async fn get_node(&self, id: Id) -> Result<Node> {
        todo!()
    }

    fn insert_parents(&mut self, id: Id, parents: Parents) -> Result<()> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(id_bytes);

        let v = parents.into();
        Ok(self.kv.insert(k, v)?)
    }

    async fn get_parents(&self, id: Id) -> Result<Parents> {
        todo!()
    }

    fn insert_indexed(&mut self, ty: ContentType, byte: u8, parents: Parents) -> Result<()> {
        let [type_byte1, type_byte2] = ty.as_bytes();
        let k = vec![KvKeyPrefix::Index as u8, type_byte1, type_byte2, byte];
        let v = parents.into();
        Ok(self.kv.insert(k, v)?)
    }

    async fn get_indexed(&self, ty: ContentType, byte: u8) -> Result<Option<Parents>> {
        let [type_byte1, type_byte2] = ty.as_bytes();
        let k = vec![KvKeyPrefix::Index as u8, type_byte1, type_byte2, byte];
        let v = self.kv.get(&k).await?;
        Ok(if let Some(v) = v {
            Some(Parents::try_from(v)?)
        } else {
            None
        })
    }
}

enum KvKeyPrefix {
    Node = 0,
    Parents = 1,
    Index = 2,
}

#[derive(Debug, Clone, Copy)]
pub struct ContentType(pub u16);

impl ContentType {
    fn as_bytes(&self) -> [u8; 2] {
        self.0.to_be_bytes()
    }
}

impl From<[u8; 2]> for ContentType {
    fn from(bytes: [u8; 2]) -> Self {
        Self(u16::from_be_bytes(bytes))
    }
}

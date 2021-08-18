//! Query and update functions for a wasm frontend backed by an AssemblageDB.
//!
//! These bindings expose a DB container that can be used to
//! [refresh](DbContainer::refresh), [sync](DbContainer::sync),
//! [broadcast](DbContainer::broadcast) and [fetch](DbContainer::fetch) nodes
//! from JS. All methods return promises, the resulting
//! [tiles](crate::model::Tile) are serialized as JS objects using `serde_json`.
//!
//! Note that most of the wasm implementations have slightly different function
//! signatures than their native counterparts, which is caused by the need for
//! serialization between wasm and JS.

use crate::{
    markup::{markup_to_node, DeserializationError},
    model::Tile,
    DbView,
};
use assemblage_db::{
    broadcast::BroadcastId,
    data::{Child, Id, Layout, Node},
    Db,
};
use assemblage_kv::storage::{self, PlatformStorage, Storage};
use log::info;
use serde::{Deserialize, Serialize};
use std::{
    convert::{TryFrom, TryInto},
    rc::Rc,
};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::future_to_promise;

/// An opaque handle to an AssemblageDB that can be used to query and update
/// nodes.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub struct DbContainer {
    wrapped: Rc<Db<PlatformStorage>>,
}

/// Opens and returns the DB with the specified name.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub async fn open(name: String) -> Result<DbContainer, JsValue> {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
    let _ignored = console_log::init();
    info!("Opening AssemblageDB \"{}\"", &name);
    let storage = storage::open(&name).await?;
    Ok(DbContainer {
        wrapped: Rc::new(Db::open(storage).await?),
    })
}

/// Opens and returns the DB with the specified name.
#[cfg(not(target_arch = "wasm32"))]
pub async fn open(name: String) -> crate::Result<DbContainer> {
    let _ignored = env_logger::try_init();
    info!("Opening AssemblageDB \"{}\"", &name);
    let storage = storage::open(&name).await?;
    Ok(DbContainer {
        wrapped: Rc::new(Db::open(storage).await?),
    })
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl DbContainer {
    /// Looks up the specified id in the DB and returns it rendered as a tile.
    ///
    /// Ids prefixed with `broadcast:` will be interpreted as broadcast ids and
    /// the corresponding broadcast will be fetched and updated (if the
    /// broadcast does not exist in the DB, a subscription will be created),
    /// before refreshing and returning the root node of the broadcast as a
    /// tile.
    pub fn refresh(&self, id: String) -> js_sys::Promise {
        let db = Rc::clone(&self.wrapped);
        future_to_promise(async move {
            let tile = refresh(db, id).await?;
            Ok(JsValue::from_serde(&tile).unwrap())
        })
    }

    /// Persists a tile in the DB and returns its updated version (which might
    /// include additional branches for example).
    pub fn sync(&self, id: Option<String>, tile: JsValue) -> js_sys::Promise {
        let db = Rc::clone(&self.wrapped);
        future_to_promise(async move {
            let tile: Result<Vec<SyncedSection>, serde_json::Error> = tile.into_serde();
            match tile {
                Ok(tile) => {
                    let updated_tile = sync(db, id, tile).await?;
                    Ok(JsValue::from_serde(&updated_tile).unwrap())
                }
                Err(e) => Err(JsValue::from_str(&format!("{}", e))),
            }
        })
    }

    /// Uploads the specified id and all of its descendants as a broadcast that
    /// can be shared via its url.
    ///
    /// If an active broadcast for this id already exists, the broadcast will be
    /// updated by transmitting only the changes since the last upload.
    pub fn broadcast(&self, id: String) -> js_sys::Promise {
        let db = Rc::clone(&self.wrapped);
        future_to_promise(async move {
            let updated_tile = broadcast(db, id).await?;
            Ok(JsValue::from_serde(&updated_tile).unwrap())
        })
    }

    /// Updates broadcast nodes by fetching the most recent version of the
    /// broadcast with the specified id and returning it as a tile.
    pub fn fetch(&self, id: String) -> js_sys::Promise {
        let db = Rc::clone(&self.wrapped);
        future_to_promise(async move {
            match id.as_str().try_into() {
                Ok(id) => {
                    let mut current = db.current().await;
                    current.fetch_broadcast(&BroadcastId::from(id)).await?;
                    let tile = current.tile(id).await?;
                    current.commit().await?;
                    Ok(JsValue::from_serde(&tile).unwrap())
                }
                Err(_) => {
                    let e = BroadcastError::InvalidId(id);
                    Err(JsValue::from_str(&format!("{:?}", e)))
                }
            }
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DbContainer {
    /// Looks up the specified id in the DB and returns it rendered as a tile.
    ///
    /// Ids prefixed with `broadcast:` will be interpreted as broadcast ids and
    /// the corresponding broadcast will be fetched and updated (if the
    /// broadcast does not exist in the DB, a subscription will be created),
    /// before refreshing and returning the root node of the broadcast as a
    /// tile.
    pub async fn refresh(&self, id: String) -> Result<Tile, RefreshError> {
        let db = Rc::clone(&self.wrapped);
        Ok(refresh(db, id).await?)
    }

    /// Persists a tile in the DB and returns its updated version (which might
    /// include additional branches for example).
    pub async fn sync(
        &self,
        id: Option<String>,
        tile: Vec<SyncedSection>,
    ) -> Result<Tile, SyncError> {
        let db = Rc::clone(&self.wrapped);
        Ok(sync(db, id, tile).await?)
    }

    /// Uploads the specified id and all of its descendants as a broadcast that
    /// can be shared via its url.
    ///
    /// If an active broadcast for this id already exists, the broadcast will be
    /// updated by transmitting only the changes since the last upload.
    pub async fn broadcast(&self, id: String) -> Result<Tile, BroadcastError> {
        let db = Rc::clone(&self.wrapped);
        Ok(broadcast(db, id).await?)
    }

    /// Updates broadcast nodes by fetching the most recent version of the
    /// broadcast with the specified id and returning it as a tile.
    pub async fn fetch(&self, id: String) -> Result<Tile, BroadcastError> {
        match id.as_str().try_into() {
            Ok(id) => {
                let db = Rc::clone(&self.wrapped);
                let mut current = db.current().await;
                current.fetch_broadcast(&BroadcastId::from(id)).await?;
                let tile = current.tile(id).await?;
                current.commit().await?;
                Ok(tile)
            }
            Err(_) => Err(BroadcastError::InvalidId(id)),
        }
    }
}

/// The error type raised if the refreshed id is invalid or the view could not
/// be refreshed.
#[derive(Debug)]
pub enum RefreshError {
    /// The specified broadcast string is not a valid broadcast UUID.
    InvalidBroadcastId(String),
    /// The specified id string is not a valid DB UUID.
    InvalidId(String),
    /// The refreshed node could not be rendered as a tile.
    ViewError(crate::Error),
}

impl<E: Into<crate::Error>> From<E> for RefreshError {
    fn from(e: E) -> Self {
        Self::ViewError(e.into())
    }
}

#[cfg(target_arch = "wasm32")]
impl From<RefreshError> for JsValue {
    fn from(e: RefreshError) -> Self {
        JsValue::from_str(&format!("{:?}", e))
    }
}

async fn refresh<S: Storage>(db: Rc<Db<S>>, id: String) -> Result<Tile, RefreshError> {
    if id.starts_with("broadcast:") {
        let id = id.replace("broadcast:", "");
        match Id::try_from(id.as_str()) {
            Ok(id) => {
                let mut current = db.current().await;
                let tile = current.tile_from_broadcast(&BroadcastId::from(id)).await?;
                current.commit().await?;
                Ok(tile)
            }
            Err(_) => Err(RefreshError::InvalidBroadcastId(id)),
        }
    } else {
        match id.as_str().try_into() {
            Ok(id) => {
                let current = db.current().await;
                let tile = current.tile(id).await?;
                current.commit().await?;
                Ok(tile)
            }
            Err(_) => Err(RefreshError::InvalidId(id)),
        }
    }
}

/// A section of a tile that should be persisted in the DB.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub enum SyncedSection {
    /// The version in the DB should be reused, as no changes have been made.
    Existing {
        /// The id of the section's node in the DB.
        id: Id,
    },
    /// The section should become a new link to an existing node.
    Linked {
        /// The id of the linked node in the DB.
        id: Id,
    },
    /// The section should be replaced in the DB with an edited version.
    Edited {
        /// The edited blocks.
        blocks: Vec<SyncedSubsection>,
    },
}

/// A subsection of a tile that should be synced with the DB.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub enum SyncedSubsection {
    /// A block of markup text.
    Text {
        /// The markup to construct the node tree of the block.
        markup: String,
    },
}

/// The error type raised if the edited blocks cannot be deserialized or
/// inserted.
#[derive(Debug)]
pub enum SyncError {
    /// The specified id belongs to an externally imported broadcast and cannot
    /// be edited.
    ExternalId(String),
    /// The specified id string is not a valid DB uuid.
    InvalidId(String),
    /// One of the blocks could not be deserialized from markup into a node.
    DeserializationError(DeserializationError),
    /// One of the sections nodes could not be found or inserted.
    DbError(assemblage_db::Error),
    /// The swapped sections could not be rendered as a tile.
    ViewError(crate::Error),
}

impl<E: Into<assemblage_db::Error>> From<E> for SyncError {
    fn from(e: E) -> Self {
        Self::DbError(e.into())
    }
}

impl From<DeserializationError> for SyncError {
    fn from(e: DeserializationError) -> Self {
        Self::DeserializationError(e)
    }
}

impl From<crate::Error> for SyncError {
    fn from(e: crate::Error) -> Self {
        Self::ViewError(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<SyncError> for JsValue {
    fn from(e: SyncError) -> Self {
        JsValue::from_str(&format!("{:?}", e))
    }
}

async fn sync<S>(
    db: Rc<Db<S>>,
    id: Option<String>,
    s: Vec<SyncedSection>,
) -> Result<Tile, SyncError>
where
    S: Storage,
{
    let id = match id {
        None => None,
        Some(id) => match id.as_str().try_into() {
            Ok(id) => Some(id),
            Err(_) => return Err(SyncError::InvalidId(id)),
        },
    };
    let mut db = db.current().await;
    let mut children = Vec::with_capacity(s.len());
    for section in s.iter() {
        children.push(match section {
            SyncedSection::Existing { id } => Child::Lazy(*id),
            SyncedSection::Linked { id } => Child::Eager(Node::list(Layout::Chain, vec![*id])),
            SyncedSection::Edited { blocks } => {
                let mut children = Vec::with_capacity(blocks.len());
                for b in blocks.iter() {
                    match b {
                        SyncedSubsection::Text { markup } => {
                            children.push(markup_to_node(markup)?);
                        }
                    }
                }
                Child::Eager(Node::list(Layout::Page, children))
            }
        })
    }
    let replacement = Node::list(Layout::Page, children);
    let id = match id {
        None => db.add(replacement).await?,
        Some(id) => {
            db.swap(id, replacement).await?;
            id
        }
    };
    let result = db.tile(id).await?;
    db.update_broadcasts(id).await?;
    db.commit().await?;
    Ok(result)
}

/// The error type raised if the tile with the specified id could not be
/// broadcast.
#[derive(Debug)]
pub enum BroadcastError {
    /// The specified id string is not a valid DB uuid.
    InvalidId(String),
    /// The broadcast failed due to a DB error.
    DbError(assemblage_db::Error),
    /// The broadcast succeeded, but the refreshed tile could not be displayed.
    ViewError(crate::Error),
}

impl<E: Into<assemblage_db::Error>> From<E> for BroadcastError {
    fn from(e: E) -> Self {
        Self::DbError(e.into())
    }
}

impl From<crate::Error> for BroadcastError {
    fn from(e: crate::Error) -> Self {
        Self::ViewError(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<BroadcastError> for JsValue {
    fn from(e: BroadcastError) -> Self {
        JsValue::from_str(&format!("{:?}", e))
    }
}

async fn broadcast<S>(db: Rc<Db<S>>, id: String) -> Result<Tile, BroadcastError>
where
    S: Storage,
{
    let id = match id.as_str().try_into() {
        Ok(id) => id,
        Err(_) => return Err(BroadcastError::InvalidId(id)),
    };
    let mut db = db.current().await;
    db.publish_broadcast(id).await?;
    let result = db.tile(id).await?;
    db.commit().await?;
    Ok(result)
}

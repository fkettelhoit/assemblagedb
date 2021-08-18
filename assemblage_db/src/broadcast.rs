//! Data structures for published or subscribed broadcasts.
use super::{Error, Result};
use crate::{data::Id, DbSnapshot};
use assemblage_kv::storage::Storage;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fmt::{self, Display, Formatter},
};

#[cfg(target_arch = "wasm32")]
use js_sys::{try_iter, Object, Reflect, Uint8Array};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, UnwrapThrowExt};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use web_sys::{window, RequestInit, Response};

const BROADCAST_URL: &str = if cfg!(feature = "workers-localhost") {
    "http://localhost:8787"
} else if cfg!(feature = "workers-env-prod") {
    "https://assemblage-broadcast.assemblage.workers.dev"
} else {
    "https://assemblage-broadcast-dev.assemblage.workers.dev"
};

pub(crate) async fn push<S: Storage>(
    db: &DbSnapshot<'_, S>,
    id: Id,
    existing_broadcast: Option<&OwnedBroadcast>,
) -> Result<OwnedBroadcast> {
    let timestamp = db.last_updated().await?.unwrap_or_default();
    if let Some(broadcast) = existing_broadcast {
        let last_pushed = get_ep_timestamps(&broadcast.broadcast_id)
            .await?
            .last()
            .copied()
            .unwrap_or_default();
        if timestamp > last_pushed {
            let (bytes, ids) = db.export_since(id, last_pushed).await?;
            if bytes.is_empty() {
                Ok(broadcast.clone())
            } else {
                put_ep(broadcast, bytes, ids, timestamp).await
            }
        } else {
            Ok(broadcast.clone())
        }
    } else {
        let (bytes, ids) = db.export(id).await?;
        post_broadcast(bytes, id, ids, timestamp).await
    }
}

pub(crate) async fn pull(id: &BroadcastId, last_updated: u64) -> Result<(Vec<u8>, u64)> {
    let mut bytes = Vec::new();
    let mut last_pushed = 0;
    for ep in get_ep_timestamps(id).await? {
        if ep > last_pushed {
            last_pushed = ep;
        }
        if ep > last_updated {
            bytes.extend_from_slice(&get_ep(id, ep).await?);
        }
    }
    Ok((bytes, last_pushed))
}

#[cfg(not(target_arch = "wasm32"))]
async fn post_broadcast(
    bytes: Vec<u8>,
    root: Id,
    ids: HashSet<Id>,
    timestamp: u64,
) -> Result<OwnedBroadcast> {
    let url = format!("{}/broadcast?episode={}", BROADCAST_URL, timestamp);
    let resp = reqwest::Client::new().post(&url).body(bytes).send().await;
    if let Ok(resp) = resp {
        let status = resp.status();
        let resp = resp.json::<BroadcastResponse>().await;
        if let Ok(resp) = resp {
            Ok(resp.into_owned_broadcast(root, ids, timestamp))
        } else {
            let err = format!("Error response: {}, {:?}", status, resp.err());
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(target_arch = "wasm32")]
async fn post_broadcast(
    bytes: Vec<u8>,
    root: Id,
    ids: HashSet<Id>,
    timestamp: u64,
) -> Result<OwnedBroadcast> {
    use std::convert::TryFrom;

    let url = format!("{}/broadcast?episode={}", BROADCAST_URL, timestamp);
    let window = window().unwrap_throw();
    let mut init = RequestInit::new();
    init.method("POST");
    let typed_array = Uint8Array::new_with_length(bytes.len() as u32);
    typed_array.copy_from(&bytes);
    init.body(Some(&typed_array.into()));
    let resp = JsFuture::from(window.fetch_with_str_and_init(&url, &init)).await;
    if let Ok(resp) = resp {
        assert!(resp.is_instance_of::<Response>());
        let resp: Response = resp.dyn_into().unwrap();
        if resp.ok() {
            if let Ok(json) = resp.json() {
                if let Ok(json) = JsFuture::from(json).await {
                    let broadcast_id = Reflect::get(&json, &"broadcast_id".into());
                    let token = Reflect::get(&json, &"token".into());
                    let expiration = Reflect::get(&json, &"expiration".into());
                    if let (Ok(broadcast_id), Ok(token), Ok(expiration)) =
                        (broadcast_id, token, expiration)
                    {
                        let id_as_string = broadcast_id.as_string().unwrap_or_default();
                        let id = Id::try_from(id_as_string.as_str());
                        if let Ok(id) = id {
                            let broadcast_id = BroadcastId(id);
                            let token = token.as_string().unwrap_or_default();
                            let expiration = expiration.as_f64().unwrap_or_default() as u64;
                            let broadcast = BroadcastResponse {
                                broadcast_id,
                                token,
                                expiration,
                            };
                            Ok(broadcast.into_owned_broadcast(root, ids, timestamp))
                        } else {
                            let err = format!("Unexpected broadcast id format: {}", id_as_string);
                            Err(Error::InvalidBroadcastResponse { url, err })
                        }
                    } else {
                        let err = format!("Unexpected json keys: {:?}", json);
                        Err(Error::InvalidBroadcastResponse { url, err })
                    }
                } else {
                    let err = format!("Invalid json: {:?}", resp);
                    Err(Error::InvalidBroadcastResponse { url, err })
                }
            } else {
                let err = format!("Invalid json: {:?}", resp);
                Err(Error::InvalidBroadcastResponse { url, err })
            }
        } else {
            let err = format!(
                "Error response: {}, '{}'",
                resp.status(),
                resp.status_text()
            );
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn put_ep(
    b: &OwnedBroadcast,
    bytes: Vec<u8>,
    ids: HashSet<Id>,
    timestamp: u64,
) -> Result<OwnedBroadcast> {
    let url = format!(
        "{}/broadcast/{}/{}",
        BROADCAST_URL, b.broadcast_id.0, timestamp
    );
    let resp = reqwest::Client::new()
        .put(&url)
        .bearer_auth(b.token.clone())
        .body(bytes)
        .send()
        .await;
    if let Ok(resp) = resp {
        let status = resp.status();
        if let reqwest::StatusCode::CREATED = resp.status() {
            Ok(b.updated_at(timestamp, ids))
        } else {
            let err = format!("Error response: {}", status);
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(target_arch = "wasm32")]
async fn put_ep(
    b: &OwnedBroadcast,
    bytes: Vec<u8>,
    ids: HashSet<Id>,
    timestamp: u64,
) -> Result<OwnedBroadcast> {
    let url = format!(
        "{}/broadcast/{}/{}",
        BROADCAST_URL, b.broadcast_id.0, timestamp
    );
    let window = window().unwrap_throw();
    let mut init = RequestInit::new();
    init.method("PUT");
    let typed_array = Uint8Array::new_with_length(bytes.len() as u32);
    typed_array.copy_from(&bytes);
    init.body(Some(&typed_array.into()));
    let headers = Object::new();
    let auth = format!("Bearer {}", b.token);
    Reflect::set(&headers, &"Authorization".into(), &auth.into())
        .expect("could not set 'Authorization' key in header object");
    init.headers(&headers);
    let resp = JsFuture::from(window.fetch_with_str_and_init(&url, &init)).await;
    if let Ok(resp) = resp {
        assert!(resp.is_instance_of::<Response>());
        let resp: Response = resp.dyn_into().unwrap();
        if resp.ok() && resp.status() == 201 {
            Ok(b.updated_at(timestamp, ids))
        } else {
            let err = format!(
                "Error response: {}, '{}'",
                resp.status(),
                resp.status_text()
            );
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn get_ep_timestamps(broadcast_id: &BroadcastId) -> Result<Vec<u64>> {
    let url = format!("{}/broadcast/{}", BROADCAST_URL, broadcast_id.0);
    let resp = reqwest::get(&url).await;
    if let Ok(resp) = resp {
        let status = resp.status();
        if let Ok(episodes) = resp.json::<Vec<String>>().await {
            Ok(episodes
                .into_iter()
                .map(|ep| ep.parse::<u64>().unwrap())
                .collect())
        } else {
            let err = format!("Error response: {}", status);
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(target_arch = "wasm32")]
async fn get_ep_timestamps(broadcast_id: &BroadcastId) -> Result<Vec<u64>> {
    let url = format!("{}/broadcast/{}", BROADCAST_URL, broadcast_id.0);
    let window = window().unwrap_throw();
    let resp = JsFuture::from(window.fetch_with_str(&url)).await;
    if let Ok(resp) = resp {
        assert!(resp.is_instance_of::<Response>());
        let resp: Response = resp.dyn_into().unwrap();
        if resp.ok() {
            if let Ok(json) = resp.json() {
                if let Ok(json) = JsFuture::from(json).await {
                    let mut episodes = Vec::new();
                    if let Some(episodes_array) =
                        try_iter(&json).expect("response is not a json array")
                    {
                        for ep in episodes_array {
                            let ep = ep.unwrap().as_string().unwrap();
                            episodes.push(ep.parse::<u64>().unwrap());
                        }
                    }
                    Ok(episodes)
                } else {
                    let err = format!("Invalid json: {:?}", resp);
                    Err(Error::InvalidBroadcastResponse { url, err })
                }
            } else {
                let err = format!("Invalid json: {:?}", resp);
                Err(Error::InvalidBroadcastResponse { url, err })
            }
        } else {
            let err = format!(
                "Error response: {}, '{}'",
                resp.status(),
                resp.status_text()
            );
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn get_ep(broadcast_id: &BroadcastId, timestamp: u64) -> Result<Vec<u8>> {
    let url = format!(
        "{}/broadcast/{}/{}",
        BROADCAST_URL, broadcast_id.0, timestamp
    );
    let resp = reqwest::get(&url).await;
    if let Ok(resp) = resp {
        if let Ok(bytes) = resp.bytes().await {
            Ok(bytes.to_vec())
        } else {
            let err = "Broadcast episode response body is not a byte array".to_string();
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

#[cfg(target_arch = "wasm32")]
async fn get_ep(broadcast_id: &BroadcastId, timestamp: u64) -> Result<Vec<u8>> {
    let url = format!(
        "{}/broadcast/{}/{}",
        BROADCAST_URL, broadcast_id.0, timestamp
    );
    let window = window().unwrap_throw();
    let resp = JsFuture::from(window.fetch_with_str(&url)).await;
    if let Ok(resp) = resp {
        assert!(resp.is_instance_of::<Response>());
        let resp: Response = resp.dyn_into().unwrap();
        if resp.ok() {
            let buffer = JsFuture::from(resp.array_buffer().expect("body is not an array buffer"))
                .await
                .unwrap();
            let typed_array = Uint8Array::new_with_byte_offset(&buffer, 0);
            let mut v = vec![0; typed_array.length() as usize];
            typed_array.copy_to(v.as_mut_slice());
            Ok(v)
        } else {
            let err = format!(
                "Error response: {}, '{}'",
                resp.status(),
                resp.status_text()
            );
            Err(Error::InvalidBroadcastResponse { url, err })
        }
    } else {
        let err = format!("{:?}", resp);
        Err(Error::InvalidBroadcastUrl { url, err })
    }
}

/// Allows its owner to locate and append to a broadcast.
#[derive(Deserialize)]
struct BroadcastResponse {
    /// The UUID of the broadcast
    broadcast_id: BroadcastId,
    /// The access token necessary to append a new episode to the broadcast
    token: String,
    /// The expiration time of the broadcast _in seconds_ since tbe Unix epoch
    expiration: u64,
}

impl BroadcastResponse {
    fn into_owned_broadcast(
        self,
        root: Id,
        exported: HashSet<Id>,
        last_updated: u64,
    ) -> OwnedBroadcast {
        OwnedBroadcast {
            broadcast_id: self.broadcast_id,
            root,
            exported,
            token: self.token,
            last_updated,
            expiration: if self.expiration == 0 {
                None
            } else {
                Some(self.expiration * 1000)
            },
        }
    }
}

/// Allows its owner to locate and append to a broadcast.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct OwnedBroadcast {
    /// The UUID of the broadcast
    pub broadcast_id: BroadcastId,
    /// The root node of the broadcast node tree
    pub root: Id,
    /// The ids of the nodes being broadcast as part of the node tree
    pub exported: HashSet<Id>,
    /// The access token necessary to append a new episode to the broadcast
    pub token: String,
    /// The time (in milliseconds since the Unix epoch) the broadcast was last
    /// updated
    pub last_updated: u64,
    /// The time (in milliseconds since the Unix epoch) the broadcast will
    /// become unavailable or `None` if it has no expiration date
    pub expiration: Option<u64>,
}

impl OwnedBroadcast {
    fn updated_at(&self, last_updated: u64, exported: HashSet<Id>) -> Self {
        OwnedBroadcast {
            broadcast_id: self.broadcast_id,
            root: self.root,
            exported,
            token: self.token.clone(),
            last_updated,
            expiration: self.expiration,
        }
    }
}

/// A type that wraps a broadcast id (which is just a DB id)
#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct BroadcastId(Id);

impl From<Id> for BroadcastId {
    fn from(id: Id) -> Self {
        Self(id)
    }
}

impl From<BroadcastId> for Id {
    fn from(broadcast_id: BroadcastId) -> Self {
        broadcast_id.0
    }
}

impl From<BroadcastId> for String {
    fn from(broadcast_id: BroadcastId) -> Self {
        broadcast_id.0.to_string()
    }
}

impl Display for BroadcastId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A collection of metadata tracking an active broadcast
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Broadcast {
    /// The UUID of the broadcast, necessary to fetch broadcast updates
    pub broadcast_id: BroadcastId,
    /// The root node of the broadcast node tree
    pub node_id: Id,
    /// The time (in milliseconds since the Unix epoch) the broadcast was last
    /// updated
    pub last_updated: u64,
    /// The time (in milliseconds since the Unix epoch) the broadcast will
    /// become unavailable or `None` if it has no expiration date
    pub expiration: Option<u64>,
}

impl From<&OwnedBroadcast> for Broadcast {
    fn from(b: &OwnedBroadcast) -> Self {
        Self {
            broadcast_id: b.broadcast_id,
            node_id: b.root,
            last_updated: b.last_updated,
            expiration: b.expiration,
        }
    }
}

impl Ord for Broadcast {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_updated
            .cmp(&other.last_updated)
            .reverse()
            .then(self.expiration.cmp(&other.expiration).reverse())
    }
}

impl PartialOrd for Broadcast {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct BroadcastSubscription {
    pub(crate) last_updated: u64,
    pub(crate) namespace: Id,
}

impl Default for BroadcastSubscription {
    fn default() -> Self {
        Self {
            last_updated: 0,
            namespace: Id::root(),
        }
    }
}

use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter, Debug},
    hash::Hash,
};
use uuid::Uuid;

/// The error type for node operations.
#[derive(Debug)]
pub enum Error {
    /// The id is not a valid uuid.
    InvalidId(String),
    /// The specified bytes could not be deserialized into a list of ids.
    InvalidIds(Vec<u8>),
    /// The specified bytes could not be deserialized into a list of parents.
    InvalidParents(Vec<u8>),
    /// Caused by a failed operation of the underlying KV store.
    StoreError(assemblage_kv::Error),
}

impl From<assemblage_kv::Error> for Error {
    fn from(e: assemblage_kv::Error) -> Self {
        Self::StoreError(e)
    }
}

pub type Result<R> = std::result::Result<R, Error>;

/// Used to distinguish bytes of different types (e.g. utf-8 text, png, ...).
#[derive(Debug, Clone, Copy)]
pub struct ContentType(pub u8);

/// Unique identifier for a node in an AssemblageDB.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct Id(pub(crate) Uuid);

impl Id {
    /// Creates a new random id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the root id (a nil uuid, with all bits set to 0).
    pub fn root() -> Self {
        Self(Uuid::nil())
    }

    pub fn num_bytes() -> usize {
        16
    }

    pub fn parse_all(bytes: &[u8]) -> Result<Vec<Id>> {
        let bytes_per_child = Self::num_bytes();
        if bytes.len() % bytes_per_child != 0 {
            return Err(Error::InvalidIds(bytes.to_vec()));
        }
        let mut ids = Vec::with_capacity(bytes.len() / bytes_per_child);
        for i in (0..bytes.len()).step_by(bytes_per_child) {
            let id_bytes: [u8; 16] = bytes[i..(i + 16)].try_into().unwrap();
            ids.push(Id::from(Uuid::from_bytes(id_bytes)));
        }
        Ok(ids)
    }

    /// Returns the id that points to a single byte (which is just the UUID of the byte as a u128).
    pub fn from_byte(ty: ContentType, byte: u8) -> Self {
        Self(
            uuid::Builder::from_u128(((ty.0 as u128) << 8) | byte as u128)
                .set_variant(uuid::Variant::RFC4122)
                .set_version(uuid::Version::Random)
                .build(),
        )
    }

    pub fn points_to_byte(&self) -> bool {
        (Id::from_byte(ContentType(0), 0).0.as_u128() ^ self.0.as_u128()) < 256
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl From<Uuid> for Id {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.points_to_byte() {
            f.debug_tuple("Id").field(&(*self.0.as_bytes().last().unwrap() as char)).finish()
        } else {
            f.debug_tuple("Id").field(&self.0).finish()
        }
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.points_to_byte() {
            write!(f, "{}", *self.0.as_bytes().last().unwrap() as char)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl From<Id> for String {
    fn from(id: Id) -> Self {
        format!("{}", id)
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&str> for Id {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match Uuid::parse_str(value) {
            Ok(uuid) => Ok(Id(uuid)),
            Err(e) => Err(Error::InvalidId(e.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    id: Id,
    kind: NodeKind,
    parents: Vec<Parent>,
}

impl Node {
    pub fn new(id: Id, kind: NodeKind, parents: Vec<Parent>) -> Self {
        Self { id, kind, parents }
    }

    pub fn similar(&self) -> Vec<Match> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub enum NodeKind {
    Cyclic(Id),
    List(Vec<Node>),
    Byte(u8),
}

/// A node that contains a child node at the specified index.
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Parent {
    /// The id of the parent node.
    pub id: Id,
    /// The index of the child inside the parent's children.
    pub index: u32,
}

impl Parent {
    /// Constructs a new parent with the specified id that contains a child at
    /// the specified index.
    pub fn new(id: Id, index: u32) -> Self {
        Self { id, index }
    }

    pub fn num_bytes() -> usize {
        16 + 4
    }

    pub fn parse_all(bytes: &[u8]) -> Result<Vec<Parent>> {
        let bytes_per_parent = Self::num_bytes();
        if bytes.len() % bytes_per_parent != 0 {
            return Err(Error::InvalidIds(bytes.to_vec()));
        }
        let mut parents = Vec::with_capacity(bytes.len() / bytes_per_parent);
        for i in (0..bytes.len()).step_by(bytes_per_parent) {
            let id_bytes: [u8; 16] = bytes[i..(i + 16)].try_into().unwrap();
            let id = Id::from(Uuid::from_bytes(id_bytes));
            let index_bytes: [u8; 4] = bytes[(i + 16)..(i + 16 + 4)].try_into().unwrap();
            let index = u32::from_be_bytes(index_bytes);
            parents.push(Parent::new(id, index));
        }
        Ok(parents)
    }

    pub fn as_bytes(&self) -> [u8; 20] {
        let mut bytes = [0; 20];
        bytes[..16].copy_from_slice(self.id.0.as_bytes());
        bytes[16..].copy_from_slice(&self.index.to_be_bytes());
        bytes
    }
}

pub struct Parents(Vec<Parent>);

impl Parents {
    pub fn empty() -> Self {
        Self(vec![])
    }

    pub fn from(mut parents: Vec<Parent>) -> Self {
        parents.sort();
        Self(parents)
    }
}

impl From<Parents> for Vec<u8> {
    fn from(parents: Parents) -> Self {
        let bytes_per_parent = 16 + 4;
        let mut bytes = Vec::with_capacity(bytes_per_parent * parents.0.len());
        for parent in parents.0 {
            bytes.extend_from_slice(parent.id.0.as_bytes());
            bytes.extend_from_slice(&parent.index.to_be_bytes());
        }
        bytes
    }
}

impl TryFrom<Vec<u8>> for Parents {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        let bytes_per_parent = 16 + 4;
        let mut i = 0;
        let mut parents = Vec::new();
        while i + bytes_per_parent <= value.len() {
            let id_bytes: [u8; 16] = value[i..(i + 16)].try_into().unwrap();
            let id = Id::from(Uuid::from_bytes(id_bytes));
            let index_bytes: [u8; 4] = value[(i + 16)..(i + 16 + 4)].try_into().unwrap();
            let index = u32::from_be_bytes(index_bytes);
            parents.push(Parent::new(id, index));
            i += bytes_per_parent;
        }
        if i == value.len() {
            Ok(Parents::from(parents))
        } else {
            Err(Error::InvalidParents(value))
        }
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Match {
    pub id: Id,
    pub bytes_matched: u32,
}

use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    hash::Hash,
};
use uuid::Uuid;

/// The error type for node operations.
#[derive(Debug)]
pub enum Error {
    /// The id is not a valid uuid.
    InvalidId(String),
    /// The specified bytes could not be deserialized into a collection of parents.
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

/// Unique identifier for a node in an AssemblageDB.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
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

impl Display for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
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

pub enum Node {
    Content(Vec<u8>),
    List(Vec<Id>),
}

enum NodeSerializerSuffix {
    Content = 0,
    List = 1,
}

impl From<Node> for Vec<u8> {
    fn from(n: Node) -> Self {
        match n {
            Node::Content(mut bytes) => {
                bytes.push(NodeSerializerSuffix::Content as u8);
                bytes
            }
            Node::List(ids) => {
                let id_bytes = 16;
                let mut bytes = Vec::with_capacity(id_bytes * ids.len() + 1);
                for id in ids {
                    bytes.extend_from_slice(id.0.as_bytes());
                }
                bytes.push(NodeSerializerSuffix::List as u8);
                bytes
            }
        }
    }
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

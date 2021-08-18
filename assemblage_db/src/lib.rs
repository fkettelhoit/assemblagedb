//! # Distributed document/graph database for connected & overlapping pages.
//!
//! AssemblageDB is a transactional high-level database for connected webs of
//! pages, notes, texts and other media. Think of it like a _personal web_, but
//! easily editable, with more connections and better navigation than the web.
//! It is high-level in the sense that it defines a document model similar to
//! HTML but vastly simpler and with graph-like 2-way links instead of tree-like
//! 1-way jump links. The data model is both:
//!
//!   - _document-oriented:_ supports nested documents without a fixed schema
//!   - _graph-based:_ documents can have multiple parents and form a directed,
//!     possibly cyclic graph
//!
//! ## Features
//!
//!   - _versioned:_ old values remain accessible until merged
//!   - _transactional:_ snapshots are isolated through
//!     [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
//!   - _storage-agnostic:_ supports native (files) and wasm (IndexedDB) targets
//!   - _indexed:_ maintains an automatic index for similarity/overlap search
//!   - _distributed:_ nodes can be published/subscribed as remote broadcasts
//!
//! ## Distributed DBs
//!
//! AssemblageDB is distributed in a sense very similar to a [distributed
//! version control
//! system](https://en.wikipedia.org/wiki/Distributed_version_control) such as
//! git: All content is stored and edited locally without any coordination with
//! other distributed copies, but AssemblageDBs can _broadcast_ parts or all of
//! their content to a cloud service and allow other instances to fetch the
//! content into their local instances. These local "borrowed" copies are only
//! modified when updates are fetched from the cloud service, but never directly
//! edited by anyone but the owner instance, ensuring that no conflicts arise.
//! The connection between borrowed content and owned content is instead
//! constructed implicitly through _overlaps_, automatic links between textually
//! similar paragraphs.
//!
//! In other words, AssemblageDBs form an overlapping network of document graphs
//! where each DB can be independently edited by their owner and connections
//! between different DBs are found automatically if their content is similar
//! enough. A single AssemblageDB instance is always consistent, but consistency
//! between different instances is explicitly not a goal. Instead of trying to
//! achieve consensus between different owners, each DB has full control over
//! their own graph of documents, with overlaps between graphs established
//! through textual overlap search.
//!
//! ## Overlap Search
//!
//! All content inserted into an AssemblageDB is automatically indexed and fully
//! searchable. The search index is not particularly space-efficient, but
//! general enough to find overlaps between arbitrary sequences of bytes, so
//! that the strings "MyVeryInterestingString" and "MyVeryUninterestingString"
//! would match with a large overlap.
//!
//! ## Data model
//!
//! Nodes in an AssemblageDB can be either atomic (a line of text for example)
//! or nested, either in a _list_ containing multiple children or a _styled_
//! node containing just a single child. [`data::Node::List`] nodes have a
//! [_layout_](data::Layout), which controls how children are laid out in
//! relation to each other, while [`data::Node::Styled`] nodes have zero or more
//! [_block styles_](data::BlockStyle) or [_span styles_](data::SpanStyle), that
//! control how their (possibly nested) child is displayed. Examples for layouts
//! and styles are:
//!
//!   - [`data::Layout::Chain`]: lays out children as a consecutive chain of
//!     inline spans. With 2 text children "foo" and "bar", the chain would be
//!     displayed as "foobar".
//!   - [`data::Layout::Page`]: lays out children as blocks on a page, separated
//!     vertically by a new line. With 2 text children "foo" and "bar", the page
//!     would be displayed as 2 lines, the first line containing "foo", the
//!     second line containing "bar".
//!   - [`data::SpanStyle::Italic`]: A span (inline) style that would display
//!     the child "foo" as "_foo_"
//!   - [`data::BlockStyle::Heading`]: A block style that would display the
//!     child "foo" in its own block with a larger font size.
//!
//! A node is always either a _span_ or a _block_. Text nodes are considered to
//! be spans by default and remain spans if styled using span styles such as
//! [`data::SpanStyle::Italic`] or [`data::SpanStyle::Bold`]. However, a single
//! block style (such as [`data::BlockStyle::Heading`]) in a set of styles is
//! always "contagious" and turns a text node "foo" styled with both
//! [`data::SpanStyle::Italic`] and [`data::BlockStyle::Heading`] into a block.
//! Similarly, layouts control whether a list is displayed as a span or a block:
//! [`data::Layout::Chain`] turns a list into a span, while
//! [`data::Layout::Page`] turns a list into a sequence of blocks.
//!
//! So, what happens when a span contains a block? Or when a list of blocks is
//! styled using a set of span styles? There are a few rules that govern
//! interactions between spans, blocks and styles:
//!
//!   - Whenever styles apply to nested children, all styles are applied to all
//!     children. A span style such as [`data::SpanStyle::Italic`] applied to a
//!     list of blocks would thus style each child as italic, a block style such
//!     as [`data::BlockStyle::Heading`] would style each child as a heading
//!     block.
//!   - Whenever a block occurs inside a span, the block is displayed as a _link
//!     to the block_. These links are always displayed as (inline) spans, so
//!     that blocks are never directly displayed inside spans.
//!   - Whenever a list of blocks occurs as a child of a list of blocks, the
//!     child is "unwrapped" and displayed as if the parent list contained all
//!     these blocks directly. So if a page A contains the children "A1" and
//!     "A2" and another page B contains the children "B1", the page A and "B2",
//!     then B would be displayed as the blocks "B1", "A1", "A2", "B2".
//!   - Whenever a list of spans occurs as a child of a list of spans, the child
//!     is similarly "unwrapped" and displayed as if the parent list contained
//!     all these spans directly.
//!
//! ## Example
//!
//! ```
//! use assemblage_db::{Db, Result, data::{BlockStyle, Child, Id, Layout, Node, SpanStyle}, tx};
//! use assemblage_kv::{run, storage::{self, MemoryStorage}};
//!
//! fn main() -> Result<()> {
//!     // The `run!` macro abstracts away the boilerplate of setting up the
//!     // right async environment and storage for native / wasm and is not
//!     // needed outside of doc tests.
//!     run!(async |storage| {
//!         let db = Db::open(storage).await?;
//!
//!         // Nodes support layouts and styles, for example as a page of blocks...
//!         let page1_id = tx!(|db| {
//!             db.add(Node::list(
//!                 Layout::Page,
//!                 vec![
//!                     Node::styled(BlockStyle::Heading, Node::text("A Heading!")),
//!                     Node::text("This is the first paragraph."),
//!                     Node::text("Unsurprisingly this is the second one..."),
//!                 ],
//!             ))
//!             .await?
//!         });
//!
//!         // ...or as inline spans that are chained together:
//!         let page2_id = tx!(|db| {
//!             db.add(Node::list(
//!                 Layout::Page,
//!                 vec![Node::list(
//!                     Layout::Chain,
//!                     vec![
//!                         Node::text("And this is the "),
//!                         Node::styled(SpanStyle::Italic, Node::text("last")),
//!                         Node::text(" paragraph..."),
//!                     ],
//!                 )],
//!             ))
//!             .await?
//!         });
//!
//!         // Documents can form a graph, with nodes keeping track of all parents:
//!         tx!(|db| {
//!             db.add(Node::list(Layout::Page, vec![page1_id, page1_id]))
//!                 .await?;
//!
//!             assert_eq!(db.parents(page1_id).await?.len(), 2);
//!             assert_eq!(db.parents(page2_id).await?.len(), 0);
//!         });
//!
//!         // All text is indexed, the DB supports "overlap" similarity search:
//!         tx!(|db| {
//!             let paragraph1_id = db.get(page1_id).await?.unwrap().children()[1].id()?;
//!             let paragraph3_id = db.get(page2_id).await?.unwrap().children()[0].id()?;
//!
//!             let overlaps_of_p1 = db.overlaps(paragraph1_id).await?;
//!             assert_eq!(overlaps_of_p1.len(), 1);
//!             assert_eq!(overlaps_of_p1[0].id, paragraph3_id);
//!             assert!(overlaps_of_p1[0].score() > 0.5);
//!         });
//!         Ok(())
//!     })
//! }
//! ```

#![deny(missing_docs)]
#![deny(broken_intra_doc_links)]
#![deny(unsafe_code)]

use assemblage_kv::{self, storage::Storage, KvStore, Snapshot};
use async_recursion::async_recursion;
use broadcast::BroadcastId;
use data::{BlockStyle, Child, Id, Layout, Node, Parent, SpanStyle, Styles};
use std::collections::{BTreeSet, HashSet};

pub mod broadcast;
mod core;
pub mod data;
mod index;

enum Slot {
    Node = 0,
    Parents = 1,
    Grams = 2,
    Count = 3,
    Overlaps = 4,
    BroadcastPublished = 5,
    BroadcastSubscribed = 6,
}

/// The error type for DB operations.
#[derive(Debug)]
pub enum Error {
    /// Caused by a failed operation of the underlying KV store.
    StoreError {
        /// The underlying KV store error.
        err: assemblage_kv::Error,
        /// The DB operation that triggered the error.
        operation: String,
        /// Information about the context of the call in the larger DB
        /// operation.
        context: String,
    },
    /// Caused by an unexpected node type or other problems with the data.
    NodeError(data::Error),
    /// No store with the specified name could be found in the DB.
    IdNotFound {
        /// The id of the node that could not be found
        id: Id,
        /// The DB operation that triggered the error
        operation: String,
        /// Information about the context of call in the larger DB operation
        context: String,
    },
    /// No broadcast with the specified id exists as a subscription in the DB.
    BroadcastIdNotFound(BroadcastId),
    /// No broadcast could be found at the specified url.
    InvalidBroadcastUrl {
        /// The url of the response endpoint.
        url: String,
        /// The reason why the broadcast request failed.
        err: String,
    },
    /// The response of the broadcast endpoint did not have the expected format.
    InvalidBroadcastResponse {
        /// The url of the response endpoint.
        url: String,
        /// The reason why the broadcast request failed.
        err: String,
    },
}

trait AsDbErrorWithContext<T> {
    fn with_context(self, op: &str, context: &str) -> Result<T>;
}

impl<T> AsDbErrorWithContext<T> for std::result::Result<T, assemblage_kv::Error> {
    fn with_context(self, op: &str, context: &str) -> Result<T> {
        self.map_err(|err| Error::StoreError {
            err,
            operation: op.to_string(),
            context: context.to_string(),
        })
    }
}

impl<T> AsDbErrorWithContext<T> for std::result::Result<T, assemblage_kv::storage::Error> {
    fn with_context(self, op: &str, context: &str) -> Result<T> {
        self.map_err(assemblage_kv::Error::from)
            .with_context(op, context)
    }
}

trait AsIdNotFoundErrorWithContext<T> {
    fn ok_or_invalid(self, id: Id, op: &str, context: &str) -> Result<T>;
}

impl<T> AsIdNotFoundErrorWithContext<T> for std::result::Result<Option<T>, Error> {
    fn ok_or_invalid(self, id: Id, op: &str, context: &str) -> Result<T> {
        match self {
            Ok(Some(n)) => Ok(n),
            Ok(None) => Err(Error::IdNotFound {
                id,
                operation: op.to_string(),
                context: context.to_string(),
            }),
            Err(e) => Err(e),
        }
    }
}

impl<T> AsIdNotFoundErrorWithContext<T> for std::result::Result<Option<T>, assemblage_kv::Error> {
    fn ok_or_invalid(self, id: Id, op: &str, context: &str) -> Result<T> {
        self.with_context(op, context)
            .ok_or_invalid(id, op, context)
    }
}

impl<E: Into<assemblage_kv::Error>> From<E> for Error {
    fn from(e: E) -> Self {
        Error::StoreError {
            err: e.into(),
            operation: "".to_string(),
            context: "".to_string(),
        }
    }
}

impl From<data::Error> for Error {
    fn from(e: data::Error) -> Self {
        Error::NodeError(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<Error> for wasm_bindgen::JsValue {
    fn from(e: Error) -> Self {
        wasm_bindgen::JsValue::from_str(&format!("{:?}", e))
    }
}

/// A specialized `Result` type for DB operations.
pub type Result<R> = std::result::Result<R, Error>;

/// A versioned and transactional document/graph DB.
pub struct Db<S: Storage> {
    store: KvStore<S>,
}

/// An isolated snapshot of a DB at a single point in time.
pub struct DbSnapshot<'a, S: Storage> {
    pub(crate) store: Snapshot<'a, S>,
}

/// The result of a [`DbSnapshot::restore()`] call, if successful.
#[derive(Debug, Clone)]
pub enum RestoredNode {
    /// The node was successfully restored from the trash.
    Restored(Node),
    /// The node with the specified id exists, but is not removed.
    NoNeedToRestoreNode,
}

/// The result of a [`DbSnapshot::preview()`] call, if successful.
#[derive(Debug, Clone)]
pub enum PreviewedNode {
    /// The block node that acts as a preview for the specified id.
    Block(Id, Node),
    /// Nothing to display, the node(s) is/are either empty or blank.
    Empty,
    /// No preview possible due to a cycle in the node graph.
    Cyclic,
}

impl<S: Storage> DbSnapshot<'_, S> {
    /// Returns true if the node will be displayed as an inline span due to its
    /// layout or styles.
    pub async fn is_span(&self, node: &Node) -> Result<bool> {
        self.is_span_recur(node).await
    }

    #[async_recursion(?Send)]
    async fn is_span_recur(&self, node: &Node) -> Result<bool> {
        Ok(match node {
            Node::Text(_) => true,
            Node::List(layout, _) => *layout == Layout::Chain,
            Node::Styled(styles, child) => match styles {
                Styles::Block(_) => false,
                Styles::Span(_) => match child.as_ref() {
                    Child::Lazy(id) => {
                        let child =
                            self.get(*id)
                                .await
                                .ok_or_invalid(*id, "is_span", "get lazy child")?;
                        self.is_span_recur(&child).await?
                    }
                    Child::Eager(node) => self.is_span_recur(node).await?,
                },
            },
        })
    }

    /// Returns true if the node will be displayed as a separate block due to
    /// its layout or styles.
    pub async fn is_block(&self, node: &Node) -> Result<bool> {
        Ok(!self.is_span(node).await?)
    }

    /// Returns true if the child node will be displayed as a link in the
    /// specified parent.
    ///
    /// A child must be displayed as a link if the child is a block and the
    /// parent is a span (which can only contain the block if the child is
    /// rendered as a link-span).
    pub async fn is_link(&self, child: &Node, parent: &Node) -> Result<bool> {
        Ok(self.is_block(child).await? && self.is_span(parent).await?)
    }

    /// Returns true if the node has no children or contains only blank text.
    pub async fn is_blank(&self, id: Id) -> Result<bool> {
        let mut visited = HashSet::new();
        let mut candidates = vec![id];
        while let Some(id) = candidates.pop() {
            if visited.contains(&id) {
                continue;
            }
            visited.insert(id);
            let node = self
                .get(id)
                .await
                .ok_or_invalid(id, "is_blank", "get node")?;
            match &node {
                Node::Text(l) => {
                    if !l.is_blank() {
                        return Ok(false);
                    }
                }
                Node::List(_, children) => {
                    for child in children {
                        candidates.push(child.id()?);
                    }
                }
                Node::Styled(_, child) => candidates.push(child.id()?),
            }
        }
        Ok(true)
    }

    /// Returns true if the node or any of its descendants contains itself or
    /// one of its ancestors.
    ///
    /// Checks whether traversing all of the children of the node recursively
    /// downwards would lead to a cycle and returns true if a cyclic
    /// parent-child relationship is found.
    pub async fn is_cyclic(&self, id: Id) -> Result<bool> {
        let mut visited = HashSet::new();
        let mut candidates = vec![id];
        while let Some(id) = candidates.pop() {
            if visited.contains(&id) {
                return Ok(true);
            }
            visited.insert(id);
            let (_, children) = self
                .get(id)
                .await
                .ok_or_invalid(id, "is_cyclic", "get node")?
                .split();
            for child in children {
                candidates.push(child.id()?);
            }
        }
        Ok(false)
    }

    /// Returns the first block of a node, if possible.
    ///
    /// For big recursive nodes it is often desirable to display only the first
    /// block as a preview of that node. But since list nodes can contain other
    /// (possibly empty) list nodes as their first child or blank nodes, it's
    /// not enough to just take the first child of a list with page layout and
    /// use it as a preview. Instead, the node has to be traversed depth-first
    /// recursively and all empty or blank children have to be skipped until the
    /// first non-blank node is found, which is then returned as the preview,
    /// wrapped with all styles that were traversed along the way.
    ///
    /// Returns:
    ///
    ///   - [`PreviewedNode::Block(Id, Node)`] if a preview can be returned
    ///   - [`PreviewedNode::Empty`] if the node contains only blank / empty
    ///     nodes
    ///   - [`PreviewedNode::Cyclic`] if the path to the first non-blank node
    ///     leads to a cycle
    pub async fn preview(&self, mut id: Id) -> Result<PreviewedNode> {
        let mut block_styles: BTreeSet<BlockStyle> = BTreeSet::new();
        let mut span_styles: BTreeSet<SpanStyle> = BTreeSet::new();
        let mut visited = HashSet::new();
        while !visited.contains(&id) {
            visited.insert(id);
            let mut node = self
                .get(id)
                .await
                .ok_or_invalid(id, "preview", "get node")?;
            match &node {
                Node::Text(l) => {
                    return Ok(if l.is_blank() {
                        PreviewedNode::Empty
                    } else {
                        node = Node::styled(span_styles, node);
                        node = Node::styled(block_styles, node);
                        PreviewedNode::Block(id, node)
                    });
                }
                Node::List(_, children) if children.is_empty() => {
                    return Ok(PreviewedNode::Empty);
                }
                Node::List(Layout::Chain, _) => {
                    return Ok(if self.is_blank(id).await? {
                        PreviewedNode::Empty
                    } else if self.is_cyclic(id).await? {
                        PreviewedNode::Cyclic
                    } else {
                        node = Node::styled(span_styles, node);
                        node = Node::styled(block_styles, node);
                        PreviewedNode::Block(id, node)
                    })
                }
                Node::List(_, children) => {
                    id = children[0].id()?;
                }
                Node::Styled(s, child) => {
                    match s {
                        Styles::Block(s) => block_styles.extend(s),
                        Styles::Span(s) => span_styles.extend(s),
                    };
                    id = (*child).id()?;
                }
            };
        }
        Ok(PreviewedNode::Cyclic)
    }

    /// Returns the path from the "oldest" unique ancestor to the parent of the
    /// specified id, _including unique ancestors that link to their child_.
    ///
    /// The result is ordered from "oldest" to "youngest" ancestor, with the
    /// immediate parent as the last element of the result.
    ///
    /// Returns the node with the specified id itself if it has zero or multiple
    /// parents and thus not a single unique ancestor. Otherwise it picks the
    /// parent of the current node as the unique ancestor and traverses upwards
    /// in this fashion until zero or multiple parents are found.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Child, Layout, Node},
    ///     tx, Db, Result,
    /// };
    /// use assemblage_kv::{run, storage};
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let descendant = Node::text("descendant");
    ///         let descendant_id = tx!(|db| db.add(descendant).await?);
    ///
    ///         let ancestor1 = Node::list(Layout::Chain, vec![descendant_id]);
    ///         let ancestor2 = Node::list(Layout::Chain, vec![ancestor1]);
    ///         let ancestor3 = Node::list(Layout::Chain, vec![ancestor2]);
    ///         let oldest_ancestor = Node::list(Layout::Chain, vec![ancestor3]);
    ///
    ///         let ancestor_id = tx!(|db| db.add(oldest_ancestor).await?);
    ///
    ///         let ancestors = tx!(|db| db.ancestor_path(descendant_id).await?);
    ///         assert_eq!(ancestors.first().unwrap().id, ancestor_id);
    ///         assert_eq!(ancestors.len(), 4);
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn ancestor_path(&self, id: Id) -> Result<Vec<Parent>> {
        let stop_at_link = false;
        self.ancestor_path_until(id, stop_at_link).await
    }

    /// Returns the path from the "oldest" unique ancestor to the parent of the
    /// specified id, _up until the first ancestors that links to its child_.
    ///
    /// The result is ordered from "oldest" to "youngest" ancestor, with the
    /// immediate parent as the last element of the result.
    ///
    /// Returns the node with the specified id itself if it has zero or multiple
    /// parents and thus not a single unique ancestor. Otherwise it picks the
    /// parent of the current node as the unique ancestor and traverses upwards
    /// in this fashion until zero or multiple parents are found or the parent
    /// would render the child as a link (and thus not embed it directly).
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Child, Layout, Node},
    ///     tx, Db, Result,
    /// };
    /// use assemblage_kv::{run, storage};
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let descendant = Node::text("descendant");
    ///         let descendant_id = tx!(|db| db.add(descendant).await?);
    ///
    ///         let ancestor1 = Node::list(Layout::Chain, vec![descendant_id]);
    ///         let ancestor2 = Node::list(Layout::Chain, vec![ancestor1]);
    ///         let ancestor3 = Node::list(Layout::Chain, vec![ancestor2]);
    ///         let oldest_ancestor = Node::list(Layout::Page, vec![ancestor3]);
    ///
    ///         // the following ancestor should not be returned (it embeds the child as a link):
    ///         let link_to_oldest_ancestor = Node::list(Layout::Chain, vec![oldest_ancestor]);
    ///
    ///         let link_id = tx!(|db| db.add(link_to_oldest_ancestor).await?);
    ///
    ///         let ancestors = tx!(|db| db.ancestor_path_until_link(descendant_id).await?);
    ///         assert_ne!(ancestors.first().unwrap().id, link_id);
    ///         assert_eq!(ancestors.len(), 4);
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn ancestor_path_until_link(&self, id: Id) -> Result<Vec<Parent>> {
        let stop_at_link = true;
        self.ancestor_path_until(id, stop_at_link).await
    }

    async fn ancestor_path_until(&self, mut id: Id, stop_at_link: bool) -> Result<Vec<Parent>> {
        let mut path = Vec::new();
        Ok(loop {
            let parents = self.parents(id).await?;
            if parents.len() != 1 {
                break path.into_iter().rev().collect();
            } else {
                let parent = parents.iter().next().unwrap();
                let is_cyclic = path.iter().any(|p| p == parent);
                let is_link = if stop_at_link {
                    let child =
                        self.get(id)
                            .await
                            .ok_or_invalid(id, "ancestor_path_until", "get child")?;
                    let parent = self.get(parent.id).await.ok_or_invalid(
                        parent.id,
                        "ancestor_path_until",
                        "get parent",
                    )?;
                    self.is_link(&child, &parent).await?
                } else {
                    false
                };
                if is_cyclic || is_link {
                    break path.into_iter().rev().collect();
                } else {
                    let parent = parents.into_iter().next().unwrap();
                    id = parent.id;
                    path.push(parent);
                }
            }
        })
    }

    /// Returns all the descendant ids of the specified id, _including_
    /// descendants displayed as links.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Layout, Node},
    ///     tx, Db, Result,
    /// };
    /// use assemblage_kv::{run, storage};
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let a_id = tx!(|db| db.add(Node::text("A")).await?);
    ///         let b_id = tx!(|db| db.add(Node::text("B")).await?);
    ///         let c_id = tx!(|db| db.add(Node::text("C")).await?);
    ///         let d_id = tx!(|db| db.add(Node::text("D")).await?);
    ///         let e_id = tx!(|db| db.add(Node::text("E")).await?);
    ///
    ///         let link_inside_tree_id = tx!(|db| {
    ///             db.add(Node::list(Layout::Page, vec![ // will be displayed as a link
    ///                 d_id, // inside a link, will be included
    ///                 e_id, // inside a link, will be included
    ///             ])).await?
    ///         });
    ///
    ///         let tree = Node::list(Layout::Page, vec![ // descendant 0
    ///             Node::list(Layout::Page, vec![ // descendant 1
    ///                 a_id, // block -> atom, descendant 2
    ///                 b_id, // block -> atom, descendant 3
    ///             ]),
    ///             Node::list(Layout::Chain, vec![ // descendant 4
    ///                 c_id, // block -> chain -> atom, descendant 5
    ///             ]),
    ///             Node::list(Layout::Chain, vec![ // descendant 6
    ///                 link_inside_tree_id, // descendant 7, with all of its children
    ///             ]),
    ///         ]);
    ///
    ///         let tree_id = tx!(|db| db.add(tree).await?);
    ///         let mut descendants = tx!(|db| db.descendants(tree_id).await?);
    ///         assert_eq!(descendants.len(), 10);
    ///         assert!(descendants.contains(&a_id));
    ///         assert!(descendants.contains(&b_id));
    ///         assert!(descendants.contains(&c_id));
    ///         assert!(descendants.contains(&d_id));
    ///         assert!(descendants.contains(&e_id));
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn descendants(&self, id: Id) -> Result<HashSet<Id>> {
        let stop_at_link = false;
        self.descendants_until(id, stop_at_link).await
    }

    /// Returns all the descendant ids of the specified id, _excluding_
    /// descendants displayed as links.
    ///
    /// Useful to get all descendants "in view", which are all children and
    /// recursively their children as long as these children will be shown as
    /// part of the whole subtree (and not just linked as a preview).
    ///
    /// A node will be shown as a link if the node must be displayed as a block
    /// inside a span context. The recursive traversal will thus collect and
    /// return all descendants as long as all nodes on the currently visited
    /// path are blocks, or if a span descendant is encountered as long as all
    /// its descendants are spans. The first block-inside-a-span will be
    /// considered a leaf node.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Layout, Node},
    ///     tx, Db, Result,
    /// };
    /// use assemblage_kv::{run, storage};
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let a_id = tx!(|db| db.add(Node::text("A")).await?);
    ///         let b_id = tx!(|db| db.add(Node::text("B")).await?);
    ///         let c_id = tx!(|db| db.add(Node::text("C")).await?);
    ///         let d_id = tx!(|db| db.add(Node::text("D")).await?);
    ///         let e_id = tx!(|db| db.add(Node::text("E")).await?);
    ///
    ///         let link_inside_tree_id = tx!(|db| {
    ///             db.add(Node::list(Layout::Page, vec![ // will be displayed as a link
    ///                 d_id, // inside a link, will not be found
    ///                 e_id, // inside a link, will not be found
    ///             ])).await?
    ///         });
    ///
    ///         let tree = Node::list(Layout::Page, vec![ // descendant 0
    ///             Node::list(Layout::Page, vec![ // descendant 1
    ///                 a_id, // block -> atom, descendant 2
    ///                 b_id, // block -> atom, descendant 3
    ///             ]),
    ///             Node::list(Layout::Chain, vec![ // descendant 4
    ///                 c_id, // block -> chain -> atom, descendant 5
    ///             ]),
    ///             Node::list(Layout::Chain, vec![ // descendant 6
    ///                 link_inside_tree_id, // descendant 7 (but its descendants will be ignored)
    ///             ]),
    ///         ]);
    ///
    ///         let tree_id = tx!(|db| db.add(tree).await?);
    ///         let mut descendants = tx!(|db| db.descendants_until_links(tree_id).await?);
    ///         assert_eq!(descendants.len(), 8);
    ///         assert!(descendants.contains(&a_id));
    ///         assert!(descendants.contains(&b_id));
    ///         assert!(descendants.contains(&c_id));
    ///         assert!(!descendants.contains(&d_id));
    ///         assert!(!descendants.contains(&e_id));
    ///         assert!(descendants.contains(&link_inside_tree_id));
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn descendants_until_links(&self, id: Id) -> Result<HashSet<Id>> {
        let stop_at_link = true;
        self.descendants_until(id, stop_at_link).await
    }

    async fn descendants_until(&self, id: Id, stop_at_link: bool) -> Result<HashSet<Id>> {
        let node = self
            .get(id)
            .await
            .ok_or_invalid(id, "descendants_until", "get main node")?;
        let mut candidates = vec![(node, id)];
        let mut descendants = HashSet::new();
        while let Some((node, id)) = candidates.pop() {
            if descendants.contains(&id) {
                continue;
            }
            descendants.insert(id);
            for child in node.children() {
                let id = child.id()?;
                let child_node =
                    self.get(id)
                        .await
                        .ok_or_invalid(id, "descendants_until", "get child")?;
                if stop_at_link && self.is_link(&child_node, &node).await? {
                    descendants.insert(id);
                } else {
                    candidates.push((child_node, id));
                }
            }
        }
        Ok(descendants)
    }

    /// Returns true if the node with the specified id or any of its descendants
    /// has multiple parents somewhere in its ancestry chain.
    ///
    /// In other words, returns true if the node or its descendants form a graph
    /// where some nodes are shared (as opposed to a tree where each descendant
    /// node always has a single parent).
    pub async fn has_shared_descendants_until_links(&self, id: Id) -> Result<bool> {
        let node = self
            .get(id)
            .await
            .ok_or_invalid(id, "descendants_until", "get main node")?;
        let mut candidates = vec![(node, id)];
        let mut descendants = HashSet::new();
        while let Some((node, id)) = candidates.pop() {
            if descendants.contains(&id) {
                continue;
            }
            descendants.insert(id);
            for child in node.children() {
                let id = child.id()?;
                let child_node =
                    self.get(id)
                        .await
                        .ok_or_invalid(id, "descendants_until", "get child")?;
                let parents = self.parents(id).await?;
                if parents.len() > 1 {
                    return Ok(true);
                }
                candidates.push((child_node, id));
            }
        }
        Ok(false)
    }
}

impl<S: Storage> DbSnapshot<'_, S> {
    /// Updates the children of the list node using the specified closure.
    ///
    /// Expects the node with the specified id to be a list node and returns
    /// [`Error::NodeError`] otherwise.
    pub async fn update<F>(&mut self, id: Id, f: F) -> Result<()>
    where
        F: FnOnce(&mut Vec<Child>),
    {
        let node = self.get(id).await.ok_or_invalid(id, "update", "get node")?;
        match node {
            Node::List(layout, mut children) => {
                f(&mut children);
                self.swap(id, Node::List(layout, children)).await
            }
            _ => Err(Error::NodeError(data::Error::WrongNodeType {
                expected: String::from("List"),
                actual: node,
            })),
        }
    }

    /// Removes the child at the specified index of the list node.
    ///
    /// Expects the node with the specified id to be a list node and returns
    /// [`Error::NodeError`] otherwise.
    pub async fn remove(&mut self, id: Id, index: u32) -> Result<()> {
        self.update(id, |elements| {
            elements.remove(index as usize);
        })
        .await
    }

    /// Replaces the child at the specified index of the list node with a new
    /// child.
    ///
    /// Expects the node with the specified id to be a list node and returns
    /// [`Error::NodeError`] otherwise.
    pub async fn replace<C: Into<Child>>(&mut self, id: Id, index: u32, child: C) -> Result<()> {
        self.update(id, |elements| {
            elements[index as usize] = child.into();
        })
        .await
    }

    /// Inserts the child at the specified index of the list node.
    ///
    /// Expects the node with the specified id to be a list node and returns
    /// [`Error::NodeError`] otherwise.
    pub async fn insert<C: Into<Child>>(&mut self, id: Id, index: u32, child: C) -> Result<()> {
        self.update(id, |elements| {
            elements.insert(index as usize, child.into());
        })
        .await
    }

    /// Pushes the child to the end of the list node.
    ///
    /// Expects the node with the specified id to be a list node and returns
    /// [`Error::NodeError`] otherwise.
    pub async fn push<C: Into<Child>>(&mut self, id: Id, child: C) -> Result<()> {
        self.update(id, |elements| {
            elements.push(child.into());
        })
        .await
    }
}

impl Child {
    /// Reads itself from the specified DB or just returns itself if no DB
    /// access is necessary.
    ///
    /// Useful as a postfix short form of [`DbSnapshot::get()`], but can also
    /// skip calls to the DB if all data is already available. For example,
    /// [Child](data::Child) implements this trait so that `child.of(&db)` can
    /// be used instead of `db.get(child.id()?)` and is more general than
    /// [`DbSnapshot::get()`] (since it also works for eager children).
    ///
    /// `None` can be used as the DB if the node and all its children are eager
    /// and the DB would thus not be necessary. If no DB is provided and the
    /// node or one of its children contains an id that needs to be read from
    /// the DB, a [`Error::IdNotFound`] error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Layout, Node},
    ///     tx, Db, Result,
    /// };
    /// use assemblage_kv::{run, storage};
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let node_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
    ///             Node::text("foo"),
    ///             Node::text("bar"),
    ///         ])).await?);
    ///
    ///         let node = tx!(|db| db.get(node_id).await?.unwrap());
    ///         let children = node.children();
    ///
    ///         tx!(|db| assert_eq!(children[0].of(&db).await?.str()?, "foo"));
    ///         tx!(|db| assert_eq!(children[1].of(&db).await?.str()?, "bar"));
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn of<'a, S: Storage + 'a>(
        &'a self,
        db: impl Into<Option<&'a DbSnapshot<'a, S>>>,
    ) -> Result<Node> {
        match (self, db.into()) {
            (Child::Eager(node), _) => Ok(node.clone()),
            (Child::Lazy(id), Some(db)) => {
                db.get(*id)
                    .await
                    .ok_or_invalid(*id, "of", "get lazy child of node")
            }
            (Child::Lazy(id), None) => Err(Error::IdNotFound {
                id: *id,
                operation: "of".to_string(),
                context: "get eager child of node".to_string(),
            }),
        }
    }
}

/// Removes boilerplate and constructs closure-like DB transactions.
///
/// # Examples
///
/// ```
/// use assemblage_db::{data::Node, tx, Db, Error, Result};
/// use assemblage_kv::{run, storage};
///
/// fn main() -> Result<()> {
///     run!(async |storage| {
///         let db = Db::open(storage).await?;
///
///         // instead of this:
///         let text1_id = {
///             let mut t = db.current().await;
///             let id = t.add(Node::text("some text")).await?;
///             t.commit().await?;
///             id
///         };
///
///         // you can just write this:
///         let text2_id = tx!(|db| db.add(Node::text("some text")).await?);
///
///         // or as a block:
///         let text3_id = tx!(|db| {
///             // do some other stuff here, then return the value of the block
///             db.add(Node::text("some text")).await?
///         });
///         Ok(())
///     })
/// }
/// ```
#[macro_export]
macro_rules! tx {
    (|$db:ident| $tx:expr) => {{
        use assemblage_db::{Db, DbSnapshot};
        let mut $db = $db.current().await;
        let ret = $tx;
        $db.commit().await?;
        ret
    }};

    (|$db:ident| -> Result<$r:ty, $e:ty> $tx:block) => {{
        use assemblage_db::{Db, DbSnapshot};
        let mut $db = $db.current().await;
        let ret = $tx;
        $db.commit().await?;
        ret
    }};
}

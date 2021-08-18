//! # Linearized view of AssemblageDB nodes.
//!
//! Nodes in an AssemblageDB form a directed, possibly cylic graph and cannot be
//! straightforwardly displayed without first "linearizing" them into a view
//! that shows only a _single subtree_ and renders all connected parents or
//! siblings as _links that branch off_. This crate provides such a
//! frontend-independent view model, which can be used as a high level,
//! linearized interface to an AssemblageDB by frontends on different platforms.
//!
//! A linearized view consists of 6 different levels of components:
//!
//!   - A single [Space](model::Space), which contains one or more tiles
//!     (usually arranged horizontally side by side).
//!   - [Tiles](model::Tile), a single node and its subtree (of descendants),
//!     which can contain zero or more sections.
//!   - [Sections](model::Section), a single editable subsections or an
//!     uneditable group of subsections that appear as children of multiple
//!     nodes.
//!   - [Subsections](model::Subsection), which contain a single block and one
//!     or more branches leading to other nodes before or after the block.
//!   - [Blocks](model::Block), descendants of the top-level node that must be
//!     displayed as blocks due to their layout or style.
//!   - [Spans](model::Span), the descendants of the block descendants that must
//!     be displayed as spans due to their layout or style.
//!
//! The following is an example of 3 different tiles arranged in a space:
//!
//! ```text
//! Tile 1:                     Tile 2:                     Tile 3:
//! +---------------------+     +---------------------+     +---------------------+
//! | Section 1:          |     | Section 1:          |     | Section 1:          |
//! | +-----------------+ |     | +-----------------+ |     | +-----------------+ |
//! | | Subsection 1:   | |     | | Subsection 1:   | |     | | Subsection 1:   | |
//! | | Before:         | |     | | Block:          | |     | | Block:          | |
//! | | +-------------+ | |     | | +-------------+ | |     | | +-------------+ | |
//! | | | Branch 1    | | |     | | | Span 1:     | | |     | | | Span 1:     | | |
//! | | +-------------+ | |     | | | +---------+ | | |     | | | +---------+ | | |
//! | | Block:          | |     | | | | Link    | | | |     | | | | "Text"  | | | |
//! | | +-------------+ | |     | | | +---------+ | | |     | | | +---------+ | | |
//! | | | Span 1:     | | |     | | | Span 2:     | | |     | | +-------------+ | |
//! | | | +---------+ | | |     | | | +---------+ | | |     | | After:          | |
//! | | | | "Text"  | | | |     | | | | Link    | | | |     | | +-------------+ | |
//! | | | +---------+ | | |     | | | +---------+ | | |     | | | Branch      | | |
//! | | | Span 2:     | | |     | | +-------------+ | |     | | +-------------+ | |
//! | | | +---------+ | | |     | | Subsection 2:   | |     | +-----------------+ |
//! | | | | Link    | | | |     | | Block:          | |     | Section 2:          |
//! | | | +---------+ | | |     | | +-------------+ | |     | +-----------------+ |
//! | | +-------------+ | |     | | | Span 1:     | | |     | | Subsection 1:   | |
//! | +-----------------+ |     | | | +---------+ | | |     | | Block:          | |
//! +---------------------+     | | | | "Text"  | | | |     | | +-------------+ | |
//!                             | | | +---------+ | | |     | | | Span 1:     | | |
//!                             | | +-------------+ | |     | | | +---------+ | | |
//!                             | +-----------------+ |     | | | | "Text"  | | | |
//!                             +---------------------+     | | | +---------+ | | |
//!                                                         | | +-------------+ | |
//!                                                         | +-----------------+ |
//!                                                         +---------------------+
//! ```
//!
//! While subtrees of AssemblageDB nodes are defined recursively and can be
//! arbitrarily deep, the nesting of a view is fixed:
//!
//!   - Spaces can _only contain tiles_, never other spaces.
//!   - Tiles can _only contain sections_, never other tiles.
//!   - Sections can _only contain subsections_, never other sections.
//!   - A subsection awlays contains _a single block_, never other subsections.
//!   - Blocks can _only contain spans_, never other blocks.
//!   - Spans _can only contain immediate content_, never other spans.
//!
//! As a result, frontends only have to follow a very simple document model,
//! which is always just 6 levels deep: A space contains tiles, which contain
//! sections, which contain subsections, which contain a single block, which
//! contain spans.
//!
//! By design, nested lists (as in Markdown) or multiple levels of headings are
//! not supported. Instead of a single, complex and hierarchical document,
//! Assemblage spaces favor a collection of relatively flat tiles. The richer
//! structure of an Assemblage space emerges as the result of the interplay of
//! these intertwined nodes.
//!
//! A space contains all the data necessary for displaying the currently viewed
//! node without the need to read from the DB again. DB access is necessary only
//! for dynamic UIs once the user navigates to another node.

#![deny(missing_docs)]
#![deny(broken_intra_doc_links)]
#![deny(unsafe_code)]

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

use assemblage_db::{
    broadcast::{Broadcast, BroadcastId},
    data::{Id, Layout, Node, Styles},
    DbSnapshot, PreviewedNode,
};
use assemblage_kv::storage::Storage;
use async_trait::async_trait;
use model::{Block, Branch, Lineage, PreviewLink, Section, Span, Subsection, Tile};
use std::collections::{BTreeSet, HashSet};

pub mod markup;
pub mod model;
pub mod bindings;

/// The error type for view operations.
#[derive(Debug)]
pub enum Error {
    /// A wrapper around an error caused by an AssemblageDB operation.
    DbError(assemblage_db::Error),
}

impl<E: Into<assemblage_db::Error>> From<E> for Error {
    fn from(e: E) -> Self {
        Error::DbError(e.into())
    }
}

#[cfg(target_arch = "wasm32")]
impl From<Error> for wasm_bindgen::JsValue {
    fn from(e: Error) -> Self {
        wasm_bindgen::JsValue::from_str(&format!("{:?}", e))
    }
}

trait AsIdNotFoundErrorWithContext<T> {
    fn with_context(self, id: Id, op: &str, context: &str) -> Result<T>;
}

impl<T> AsIdNotFoundErrorWithContext<T> for std::result::Result<Option<T>, assemblage_db::Error> {
    fn with_context(self, id: Id, op: &str, context: &str) -> Result<T> {
        match self {
            Ok(Some(n)) => Ok(n),
            Ok(None) => Err(Error::DbError(assemblage_db::Error::IdNotFound {
                id,
                operation: op.to_string(),
                context: context.to_string(),
            })),
            Err(e) => Err(Error::DbError(e)),
        }
    }
}

/// A specialized `Result` type for view operations.
pub type Result<T> = std::result::Result<T, Error>;

/// A linearized view of a DB that can display nodes in terms of a tile of
/// sections with subsections, spans and branches to other nodes. Acts as a
/// "view layer" and transforms the graph of nodes into a linearized
/// representation suitable for a human-readable UI.
#[async_trait(?Send)]
pub trait DbView {
    /// Returns the specified node viewed as inline spans.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Child, Layout, Node},
    ///     tx, Db,
    /// };
    /// use assemblage_view::{DbView, model::Span, Result};
    /// use assemblage_kv::run;
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let text_id = tx!(|db| {
    ///             db.add(Node::list(Layout::Chain, vec![Node::text("ab"), Node::text("cd")])).await?
    ///         });
    ///         let text_spans = db.current().await.spans(text_id, true).await?;
    ///         assert_eq!(text_spans, vec![Span::text("ab"), Span::text("cd")]);
    ///         Ok(())
    ///     })
    /// }
    /// ```
    async fn spans(&self, id: Id, follow_links: bool) -> Result<Vec<Span>>;

    /// Returns the specified node viewed as subsections (which each contain a
    /// block & possibly zero or more branches).
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Child, Layout, Node},
    ///     tx, Db,
    /// };
    /// use assemblage_view::{
    ///     model::{Subsection, Block, Span},
    ///     DbView, Result,
    /// };
    /// use assemblage_kv::run;
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let text_id = tx!(|db| {
    ///             db.add(Node::list(Layout::Chain, vec![Node::text("ab"), Node::text("cd")])).await?
    ///         });
    ///         let text_blocks = db.current().await.subsections(text_id, true).await?;
    ///
    ///         let expected_spans = vec![Span::text("ab"), Span::text("cd")];
    ///         assert_eq!(text_blocks, vec![
    ///             Subsection {
    ///                 id: text_id,
    ///                 block: Block::text(expected_spans),
    ///                 before: Vec::new(),
    ///                 after: Vec::new(),
    ///             },
    ///         ]);
    ///         Ok(())
    ///     })
    /// }
    /// ```
    async fn subsections(&self, id: Id, follow_links: bool) -> Result<Vec<Subsection>>;

    /// Returns the specified node viewed as sections, which are groups of
    /// related descendant blocks.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Child, Node},
    ///     tx, Db,
    /// };
    /// use assemblage_view::{
    ///     model::{Block, Section, Subsection, Span},
    ///     DbView, Result,
    /// };
    /// use assemblage_kv::run;
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///
    ///         let node_id = tx!(|db| {
    ///             db.add(Node::text("a line of text")).await?
    ///         });
    ///
    ///         let split_spans = true;
    ///         let sections = db.current().await.sections(node_id, split_spans).await?;
    ///
    ///         let expected_section = Section {
    ///             id: None,
    ///             subsections: vec![
    ///                 Subsection {
    ///                     id: node_id,
    ///                     block: Block::text(vec![Span::text("a line of text")]),
    ///                     before: Vec::new(),
    ///                     after: Vec::new(),
    ///                 }
    ///             ],
    ///             has_multiple_parents: false,
    ///         };
    ///         assert_eq!(sections, vec![expected_section]);
    ///         Ok(())
    ///     })
    /// }
    /// ```
    async fn sections(&self, id: Id, split_spans: bool) -> Result<Vec<Section>>;

    /// Returns the specified node viewed as a tile, with branches to other
    /// nodes injected between blocks.
    ///
    /// # Examples
    ///
    /// ```
    /// use assemblage_db::{
    ///     data::{Child, Layout, Node, Parent},
    ///     tx, Db,
    /// };
    /// use assemblage_view::{
    ///     model::{Block, Branch, Lineage, PreviewLink, Section, Subsection, Span, Tile},
    ///     DbView, Error, Result,
    /// };
    /// use assemblage_kv::run;
    ///
    /// fn main() -> Result<()> {
    ///     run!(async |storage| {
    ///         let db = Db::open(storage).await?;
    ///         let a_id = tx!(|db| db.add(Node::text("A")).await?);
    ///         let b_id = tx!(|db| db.add(Node::text("B")).await?);
    ///         let page1_id = tx!(|db| db.add(Node::list(Layout::Page, vec![a_id, b_id])).await?);
    ///
    ///         let x_id = tx!(|db| db.add(Node::text("X")).await?);
    ///         let page2_id = tx!(|db| db.add(Node::list(Layout::Page, vec![a_id, x_id])).await?);
    ///
    ///         tx!(|db| -> Result<_, Error> {
    ///             let tile = db.tile(page1_id).await?;
    ///             assert_eq!(tile.sections.len(), 2);
    ///         });
    ///         Ok(())
    ///     })
    /// }
    /// ```
    async fn tile(&self, id: Id) -> Result<Tile>;

    /// Returns the root node of the specified broadcast as a tile, subscribing
    /// and fetching the broadcast if no subscription exists yet.
    async fn tile_from_broadcast(&mut self, broadcast_id: &BroadcastId) -> Result<Tile>;
}

#[async_trait(?Send)]
impl<S: Storage> DbView for DbSnapshot<'_, S> {
    async fn spans(&self, id: Id, follow_links: bool) -> Result<Vec<Span>> {
        let node = self.get(id).await.with_context(id, "spans", "get node")?;
        Ok(match node {
            Node::Text(line) => vec![Span::text(line.into_string())],
            Node::List(Layout::Chain, children) => {
                let mut child_spans = Vec::new();
                for child in children {
                    child_spans.extend(self.spans(child.id()?, follow_links).await?);
                }
                child_spans
            }
            Node::List(Layout::Page, _) if follow_links => {
                vec![Span::link(lineage(self, id).await?)]
            }
            Node::List(Layout::Page, _) => shallow_lineage(id),
            Node::Styled(styles, child) => match styles {
                Styles::Block(_) if follow_links => vec![Span::link(lineage(self, id).await?)],
                Styles::Block(_) => shallow_lineage(id),
                Styles::Span(styles) => {
                    let spans = self.spans(child.id()?, follow_links).await?;
                    spans.into_iter().map(|s| s.styled_with(&styles)).collect()
                }
            },
        })
    }

    async fn subsections(&self, id: Id, follow_links: bool) -> Result<Vec<Subsection>> {
        let node = self
            .get(id)
            .await
            .with_context(id, "subsections", "get node")?;
        Ok(match node {
            Node::Text(_) => vec![Subsection {
                id,
                block: Block::text(self.spans(id, follow_links).await?),
                before: Vec::new(),
                after: Vec::new(),
            }],
            Node::List(Layout::Chain, children) => {
                let mut child_spans = Vec::new();
                for child in children {
                    child_spans.extend(self.spans(child.id()?, follow_links).await?);
                }
                vec![Subsection {
                    id,
                    block: Block::text(child_spans),
                    before: Vec::new(),
                    after: Vec::new(),
                }]
            }
            Node::List(Layout::Page, children) => {
                let mut child_blocks = Vec::new();
                for child in children {
                    child_blocks.extend(self.subsections(child.id()?, follow_links).await?);
                }
                child_blocks
            }
            Node::Styled(styles, child) => {
                let (block_styles, span_styles) = match styles {
                    Styles::Block(styles) => (styles, BTreeSet::new()),
                    Styles::Span(styles) => (BTreeSet::new(), styles),
                };
                let child_blocks = self.subsections(child.id()?, follow_links).await?;
                child_blocks
                    .into_iter()
                    .map(|s| s.styled_with(&block_styles, &span_styles))
                    .collect()
            }
        })
    }

    async fn sections(&self, id: Id, split_spans: bool) -> Result<Vec<Section>> {
        let node = self
            .get(id)
            .await
            .with_context(id, "sections", "get node")?;
        Ok(match (split_spans, node) {
            (_, Node::Text(_)) => {
                let has_multiple_parents = self.has_shared_descendants_until_links(id).await?;
                vec![Section {
                    id: None,
                    subsections: self.subsections(id, true).await?,
                    has_multiple_parents,
                }]
            }
            (false, Node::List(Layout::Chain, _)) => {
                let has_multiple_parents = self.has_shared_descendants_until_links(id).await?;
                let subsections = self.subsections(id, true).await?;
                let id = if has_multiple_parents { Some(id) } else { None };
                vec![Section {
                    id,
                    subsections,
                    has_multiple_parents,
                }]
            }
            (_, Node::List(_, children)) => {
                let mut sections = Vec::new();
                for child in children {
                    let id = child.id()?;
                    let parents = self.parents(id).await?;
                    if parents.len() > 1 {
                        let blocks = self.subsections(id, true).await?;
                        sections.push(Section {
                            id: Some(id),
                            subsections: blocks,
                            has_multiple_parents: true,
                        })
                    } else {
                        sections.extend(self.sections(id, false).await?);
                    }
                }
                sections
            }
            (_, Node::Styled(styles, child)) => {
                let id = child.id()?;
                let parents = self.parents(id).await?;
                let (block_styles, span_styles) = match styles {
                    Styles::Block(styles) => (styles, BTreeSet::new()),
                    Styles::Span(styles) => (BTreeSet::new(), styles),
                };
                let blocks = self
                    .subsections(id, true)
                    .await?
                    .into_iter()
                    .map(|s| s.styled_with(&block_styles, &span_styles))
                    .collect();
                if parents.len() > 1 {
                    vec![Section {
                        id: Some(id),
                        subsections: blocks,
                        has_multiple_parents: true,
                    }]
                } else {
                    let mut sections = Vec::new();
                    for section in self.sections(id, split_spans).await?.into_iter() {
                        sections.push(Section {
                            subsections: section
                                .subsections
                                .into_iter()
                                .map(|s| s.styled_with(&block_styles, &span_styles))
                                .collect(),
                            ..section
                        });
                    }
                    sections
                }
            }
        })
    }

    async fn tile(&self, id: Id) -> Result<Tile> {
        let broadcasts = self.list_broadcasts(id).await?;
        tile_in_store(self, id, broadcasts).await
    }

    async fn tile_from_broadcast(&mut self, broadcast_id: &BroadcastId) -> Result<Tile> {
        self.subscribe_to_broadcast(broadcast_id).await?;
        let namespaced = self.namespaced_id(broadcast_id, Id::root()).await?;
        tile_in_store(self, namespaced, BTreeSet::new()).await
    }
}

async fn tile_in_store<S: Storage>(
    db: &DbSnapshot<'_, S>,
    id: Id,
    broadcasts: BTreeSet<Broadcast>,
) -> Result<Tile> {
    let mut sections = db.sections(id, true).await?;
    let ids_in_view = db.descendants_until_links(id).await?;

    for section in sections.iter_mut() {
        for sub in section.subsections.iter_mut() {
            for is_before in vec![true, false].into_iter() {
                let siblings = if is_before {
                    db.before(sub.id).await?
                } else {
                    db.after(sub.id).await?
                };
                // Now we need to walk up the parent hierarchy for each sibling
                // to find the upmost ancestor that is still displayed as a
                // single block (so that for example for the sibling "C" in the
                // chain ["A", "B", "C"] we display "ABC" and not just "C").
                let mut sibling_blocks = HashSet::new();
                for mut sibling_id in siblings.into_iter() {
                    let mut sibling_node = db.get(sibling_id).await.with_context(
                        sibling_id,
                        "tile_in_store",
                        "get sibling node",
                    )?;
                    let mut visited = HashSet::new();
                    let sibling_id = loop {
                        let parents = db.parents(sibling_id).await?;
                        if parents.len() != 1
                            || visited.contains(&sibling_id)
                            || db.is_block(&sibling_node).await?
                        {
                            break sibling_id;
                        } else {
                            let parent_id = parents.into_iter().next().unwrap().id;
                            let parent_node = db.get(parent_id).await.with_context(
                                parent_id,
                                "tile_in_store",
                                "get parent node of sibling",
                            )?;
                            let is_link = db.is_link(&sibling_node, &parent_node).await?;
                            if is_link
                                || (db.is_block(&sibling_node).await?
                                    && db.is_block(&parent_node).await?)
                            {
                                break sibling_id;
                            }
                            visited.insert(sibling_id);
                            sibling_id = parent_id;
                            sibling_node = parent_node;
                        }
                    };
                    if !ids_in_view.contains(&sibling_id) {
                        sibling_blocks.insert(sibling_id);
                    }
                }
                let mut branches = Vec::new();
                for sibling in sibling_blocks.into_iter() {
                    let link = lineage(db, sibling).await?;
                    let timestamp = db
                        .versions(sibling)
                        .await?
                        .last()
                        .as_ref()
                        .map_or(0, |v| v.timestamp);
                    branches.push(Branch::Sibling { link, timestamp });
                }
                branches.sort();
                if is_before {
                    sub.before = branches;
                } else {
                    sub.after = branches;
                }
            }
        }
    }

    let node = db
        .get(id)
        .await
        .with_context(id, "tile_in_store", "get main node")?;
    let mut branches = Vec::new();
    for p in db.parents(id).await?.into_iter() {
        let parent_node =
            db.get(p.id)
                .await
                .with_context(p.id, "tile_in_store", "get parent node")?;
        if db.is_link(&node, &parent_node).await? {
            let descent = db.ancestor_path_until_link(p.id).await?;
            let descendant = preview(db, id).await?;
            let ancestor = if let Some(ancestor) = descent.first() {
                Some(preview(db, ancestor.id).await?)
            } else {
                None
            };
            let link = Lineage {
                descendant,
                ancestor,
                descent,
            };
            let timestamp = latest_version(db, id).await?;
            branches.push(Branch::Sibling { link, timestamp });
        }
    }
    branches.sort();

    let preview = preview(db, id).await?.block;
    Ok(Tile {
        id,
        preview,
        broadcasts,
        sections,
        branches,
    })
}

// Returns the first non-empty block of the specified node as a preview.
async fn preview<S: Storage>(db: &DbSnapshot<'_, S>, id: Id) -> Result<PreviewLink> {
    let preview = db.preview(id).await?;
    let block = match preview {
        PreviewedNode::Block(id, _) => db.subsections(id, false).await?.remove(0).block,
        PreviewedNode::Empty => Block::text(vec![Span::text("")]),
        PreviewedNode::Cyclic => Block::Cyclic,
    };
    Ok(PreviewLink { id, block })
}

// Returns the lineage of the specified node up to its "oldest" unique ancestor.
async fn lineage<S: Storage>(db: &DbSnapshot<'_, S>, id: Id) -> Result<Lineage> {
    let descendant = preview(db, id).await?;
    let descent = db.ancestor_path_until_link(id).await?;
    let ancestor = if let Some(ancestor) = descent.first() {
        let ancestor = preview(db, ancestor.id).await?;
        Some(ancestor)
    } else {
        None
    };
    Ok(Lineage {
        descendant,
        ancestor,
        descent,
    })
}

async fn latest_version<S: Storage>(db: &DbSnapshot<'_, S>, id: Id) -> Result<u64> {
    Ok(db
        .versions(id)
        .await?
        .last()
        .as_ref()
        .map_or(0, |v| v.timestamp))
}

fn shallow_lineage(id: Id) -> Vec<Span> {
    let l = Lineage {
        descendant: PreviewLink {
            id,
            block: Block::text(vec![Span::text("...")]),
        },
        ancestor: None,
        descent: vec![],
    };
    vec![Span::link(l)]
}

/// Creates a BTreeSet, useful for creating an ordered set of styles.
#[macro_export]
macro_rules! styles {
    ( $( $x:expr ),* ) => {
        {
            let mut styles = std::collections::BTreeSet::new();
            $(
                styles.insert($x);
            )*
            styles
        }
    };
}

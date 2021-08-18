//! Components that form an AssemblageDB view.
//!
//! All of the structs in this module are meant to be serializable to/from JSON
//! (or any other serde-supported format) and act as the interface exchange
//! format between frontends and AssemblageDB backends.
//!
//! See the [crate's main docs](crate) for more details.
use assemblage_db::{
    broadcast::Broadcast,
    data::{BlockStyle, Id, Parent, SpanStyle},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A single top-level view that displays multiple nodes side by side as tiles.
pub struct Space {
    /// Independent nodes, viewed side by side, each in their own tile.
    pub tiles: Vec<Tile>,
}

/// A high level, linearized view of a _single node and its subtree_, with links
/// to any connected nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Tile {
    /// The viewed node.
    pub id: Id,
    /// The first (non-empty) block of the tile that acts as a preview.
    pub preview: Block,
    /// The active broadcasts that contain the current tile.
    pub broadcasts: BTreeSet<Broadcast>,
    /// Groups of descendant blocks.
    pub sections: Vec<Section>,
    /// The parents that embed this tile as a link.
    pub branches: Vec<Branch>,
}

/// A group of descendant blocks of the viewed node (or the node itself if it is
/// atomic).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Section {
    /// The id of the subtree containing the group of blocks or `None` if the
    /// subtree should not be displayed as its own tile.
    pub id: Option<Id>,
    /// True if the section node or a descendant is the child of other parents
    /// in addition to the currently viewed node.
    #[serde(rename = "hasMultipleParents")]
    pub has_multiple_parents: bool,
    /// The descendant blocks and their branches that are grouped together in
    /// this section.
    pub subsections: Vec<Subsection>,
}

/// A descendant block of a section together with its branches (before and after
/// the block).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Subsection {
    /// The node that corresponds to the block of this subsection.
    pub id: Id,
    /// The node displayed as a standalone visual block.
    pub block: Block,
    /// Links to nodes that occur _before_ and "branch into" the currently
    /// viewed node such as siblings of another parent.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub before: Vec<Branch>,
    /// Links to nodes that occur _after_ and "branch out of" the currently
    /// viewed node such as siblings of another parent.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<Branch>,
}

impl Subsection {
    /// Applies the specified styles to the block of the subsection (_in
    /// addition_ to the current styles of the block).
    pub fn styled_with(mut self, b: &BTreeSet<BlockStyle>, s: &BTreeSet<SpanStyle>) -> Self {
        self.block = self.block.styled_with(b, s);
        self
    }
}

/// A descendant of a section, displayed as a block due to its layout or style.
#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub enum Block {
    /// A collection of textual spans displayed as a paragraph.
    Text {
        /// The styles that apply to this block and all spans in this block.
        #[serde(default)]
        #[serde(skip_serializing_if = "BTreeSet::is_empty")]
        styles: BTreeSet<BlockStyle>,
        /// The descendant spans that are contained in this block.
        spans: Vec<Span>,
    },
    /// A subtree of nodes that cannot be displayed due to cyclic dependencies.
    Cyclic,
}

impl Block {
    /// Constructs a new text block with the specified spans and without any
    /// block styles.
    pub fn text(spans: Vec<Span>) -> Self {
        Self::Text {
            spans,
            styles: BTreeSet::new(),
        }
    }

    /// Applies the specified styles to the block (_in addition_ to the current
    /// styles of the block).
    pub fn styled_with(self, b: &BTreeSet<BlockStyle>, s: &BTreeSet<SpanStyle>) -> Self {
        match self {
            Self::Text { mut styles, spans } => {
                styles.extend(b);
                Self::Text {
                    styles,
                    spans: spans.into_iter().map(|span| span.styled_with(s)).collect(),
                }
            }
            Self::Cyclic => self,
        }
    }
}

/// A link to a node that "branches off" from the currently viewed node and is
/// displayed before or after it.
///
/// In other words, a branch is a sibling or related node which precedes/follows
/// the block in the linearized view with different content than what
/// precedes/follows in the linearized view.
///
/// For example, if the currently viewed node is a list containing 3 nodes, `[A,
/// B, C]` and the node `B` is also contained in another list `[A, B, X]`, then
/// from the point of view of `[A, B, C]` the node `X` would be a "branch" after
/// `B`, because it follows `B` but continues with different content than what
/// is currently in view (where `C` would follow). The branch would be shown as
/// a link to `X` after the block of `B` to indicate that the flow of content
/// diverges after `B`. The node `A`, however, would not be considered a branch,
/// as the content that precedes `B` is `A` in both lists and so there is no
/// divergence.
///
/// Similarly, if the curently viewed node is a list containing 3 nodes, `[A, B,
/// C]` and the node `B` is also contained in another list `[Z, B, C]`, then `Z`
/// would be a branch _before_ `B` and shown as a link to `Z` before the block
/// of `B`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub enum Branch {
    /// A sibling that branches off before or after the current block.
    Sibling {
        /// A link to a node and possibly a line of ancestors that form its
        /// context.
        link: Lineage,
        /// The timestamp of the latest version related to this sibling.
        timestamp: u64,
    },
}

impl Ord for Branch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (
                Branch::Sibling {
                    link: l1,
                    timestamp: t1,
                },
                Branch::Sibling {
                    link: l2,
                    timestamp: t2,
                },
            ) => t1
                .cmp(t2)
                .then(l1.descendant.id.cmp(&l2.descendant.id))
                .then(l1.cmp(l2)),
        }
    }
}

impl PartialOrd for Branch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A link to another node which may include a line of ancestors that show the
/// descendant's context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Lineage {
    /// The linked node together with its preview.
    pub descendant: PreviewLink,
    /// The context in which the linked node occurs or `None` if the descendant
    /// is linked without ancestors.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ancestor: Option<PreviewLink>,
    /// The path from the ancestor to the descendant (with the immediate parent
    /// of the descendant as the last element).
    pub descent: Vec<Parent>,
}

/// A link to another node together with the first block of that node as a
/// preview.
#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PreviewLink {
    /// The id of the linked node.
    pub id: Id,
    /// The first (non-empty) block of the linked node.
    pub block: Block,
}

/// A descendant of a block, displayed as a span due to its layout or style.
#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub enum Span {
    /// A segment of a paragraph of text.
    Text {
        /// The styles that apply to this text.
        #[serde(default)]
        #[serde(skip_serializing_if = "BTreeSet::is_empty")]
        styles: BTreeSet<SpanStyle>,
        /// The text displayed
        text: String,
    },
    /// An inline link to another node.
    Link {
        /// The styles that apply to this link.
        #[serde(default)]
        #[serde(skip_serializing_if = "BTreeSet::is_empty")]
        styles: BTreeSet<SpanStyle>,
        /// The link displayed.
        link: Lineage,
    },
}

impl Span {
    /// Returns a span containing a text without newlines.
    ///
    /// Such a text span might be displayed in frontend implementations as
    /// multiple lines (if the text span is too long to fit into a single line),
    /// but it will never contain _a semantic_ newline which separates
    /// paragraphs.
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text {
            styles: BTreeSet::new(),
            text: s.into(),
        }
    }

    /// Returns a span that links to another node.
    pub fn link(link: Lineage) -> Self {
        Self::Link {
            styles: BTreeSet::new(),
            link,
        }
    }

    /// Applies the specified styles to the span (_in addition_ to the current
    /// styles of the span).
    pub fn styled_with(mut self, styles: &BTreeSet<SpanStyle>) -> Self {
        match &mut self {
            Self::Text { styles: s, .. } => s.extend(styles),
            Self::Link { styles: s, .. } => s.extend(styles),
        }
        self
    }
}

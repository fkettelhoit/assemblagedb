//! Data structures for AssemblageDB nodes, children, parents and siblings.
//!
//! [Nodes](Node) are the fundamental data types that are stored in and
//! retrieved from a DB. Whenever nodes are stored in a DB, they are assigned an
//! [id](Id). Nodes can contain other nodes as [children](Child) and every child
//! can have multiple [parents](Parent).
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min, Ordering},
    collections::{BTreeSet, HashSet},
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    hash::Hash,
};
use uuid::Uuid;

/// The error type for node operations.
#[derive(Debug, Clone)]
pub enum Error {
    /// The id is not a valid uuid.
    InvalidId(String),
    /// The node is of a different type than expected.
    WrongNodeType {
        /// The expected node type, such as "List" or "Text".
        expected: String,
        /// The node with the unexpected type.
        actual: Node,
    },
    /// The child is eager, but lazy was expected, or vice versa.
    WrongChildType {
        /// The expected child type, "Eager" or "Lazy".
        expected: String,
        /// The child with the unexpected type.
        actual: Child,
    },
    /// A different number of children was expected.
    ChildrenMismatch(Vec<Child>),
}

impl Error {
    fn wrong_node_type(expected: &str, actual: &Node) -> Self {
        Error::WrongNodeType {
            expected: String::from(expected),
            actual: actual.clone(),
        }
    }

    fn wrong_child_type(expected: &str, actual: &Child) -> Self {
        Error::WrongChildType {
            expected: String::from(expected),
            actual: actual.clone(),
        }
    }
}

/// A specialized `Result` type for node operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Unique identifier for a node in an AssemblageDB.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
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

/// A single line of text.
///
/// Encapsulates the text that it contains and can only be constructed using
/// [`Node::text()`] to guarantee that a line of text never contains a newline
/// character.
///
/// # Examples
///
/// ```
/// use assemblage_db::data::Node;
/// use serde_json;
///
/// let text_node = Node::text("a single line");
/// let text_node_json = "{\"Text\":\"a single line\"}";
///
/// let serialized = serde_json::to_string(&text_node);
/// assert_eq!(serialized.unwrap(), text_node_json);
///
/// let deserialized: Node =  serde_json::from_str(text_node_json).unwrap();
/// assert_eq!(deserialized, text_node);
///
/// let invalid_node_json = "{\"Text\":\"line1\\nline2\"}";
/// let error = serde_json::from_str::<Node>(invalid_node_json).unwrap_err();
/// assert_eq!(
///     format!("{}", error),
///     "Text nodes must not contain newlines, but found \"line1\nline2\"".to_string()
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "String")]
pub struct Line(String);

impl TryFrom<String> for Line {
    type Error = String;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        if s.contains('\n') {
            Err(format!(
                "Text nodes must not contain newlines, but found \"{}\"",
                s
            ))
        } else {
            Ok(Line(s))
        }
    }
}

impl Line {
    /// Returns true if the line only contains whitespace.
    pub fn is_blank(&self) -> bool {
        self.0.trim().is_empty()
    }

    /// Returns the line as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the line and returns it as a string.
    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// The fundamental data type that is stored in an AssemblageDB.
///
/// Nodes can be either _atoms_ (which cannot contain any children) or
/// _collections_ (which might contain zero or more children). Depending on their
/// content, nodes will be displayed either inline as _spans_ or as separate
/// _blocks_.
pub enum Node {
    /// A single line of text.
    Text(Line),
    /// A list of zero or more children with a layout that controls their
    /// positions relative to each other.
    List(Layout, Vec<Child>),
    /// A container that applies a set of styles to a single child.
    Styled(Styles, Box<Child>),
}

impl Node {
    /// Creates a text node if the string does not contain any newlines, or a
    /// page node of lines of text if it does.
    ///
    /// Because line breaks have semantic meaning in AssemblageDBs (they demarcate
    /// separate blocks), text nodes must never contain more than a single line
    /// of text. Multiple lines of text must be modeled as a list node,
    /// containing the individual lines as text nodes, together with a page
    /// layout, which will lay out the text nodes as vertically separated
    /// blocks.
    ///
    /// This constructor will split the string into separate lines. If it does
    /// not contain any newlines, no split is necessary and a text node is
    /// constructed directly from the string and returned. Otherwise, the lines
    /// will become the children of a list node with page layout and this list
    /// node is returned.
    pub fn text(s: impl Into<String>) -> Self {
        let s = s.into();
        if s.contains('\n') {
            let lines = s
                .split('\n')
                .map(|l| Child::Eager(Node::Text(Line(String::from(l)))))
                .collect();
            Node::List(Layout::Page, lines)
        } else {
            Node::Text(Line(s))
        }
    }

    /// Constructs a list node out of child nodes.
    pub fn list(layout: Layout, children: Vec<impl Into<Child>>) -> Self {
        Node::List(layout, children.into_iter().map(|c| c.into()).collect())
    }

    /// Constructs a styled node out of a child.
    pub fn styled(styles: impl Into<Styles>, child: impl Into<Child>) -> Self {
        let styles = styles.into();
        match (styles.is_empty(), child.into()) {
            (true, Child::Eager(node)) => node,
            (_, child) => Node::Styled(styles, Box::new(child)),
        }
    }

    /// Splits the node into the same node without children, and separately its
    /// children.
    ///
    /// Useful to transform the children of nodes generically without caring
    /// about the exact type of the node. Just split (and thereby consume) the
    /// node into a node-specific part and its children, apply the
    /// transformation to the children and join them again using [`Node::with`].
    ///
    /// For atomic nodes or empty collections the returned children will be
    /// empty.
    pub fn split(self) -> (Node, Vec<Child>) {
        match self {
            Node::List(l, children) => (Node::List(l, vec![]), children.into_iter().collect()),
            Node::Styled(s, child) => (
                Node::styled(s, Node::List(Layout::Chain, vec![])),
                vec![*child],
            ),
            Node::Text(t) => (Node::Text(t), vec![]),
        }
    }

    /// Replaces the children of the node with new ones, useful to join nodes
    /// after a split.
    ///
    /// Returns an error if the children of a node are replaced with a number
    /// that is incompatible with the node type. List nodes accept any number of
    /// children, but styled nodes accept only a single child and atomic nodes
    /// such as text nodes accept none at all.
    pub fn with(self, mut children: Vec<Child>) -> Result<Self> {
        match self {
            Node::Text(_) => {
                if children.is_empty() {
                    Ok(self)
                } else {
                    Err(Error::ChildrenMismatch(children))
                }
            }
            Node::List(layout, _) => Ok(Node::List(layout, children)),
            Node::Styled(styles, _) => {
                if children.len() != 1 {
                    Err(Error::ChildrenMismatch(children))
                } else {
                    Ok(Node::Styled(styles, Box::new(children.pop().unwrap())))
                }
            }
        }
    }

    /// Returns the children of the node (or an empty vector if the node is
    /// atomic).
    ///
    /// Useful for recursive node traversals that work generically for all node
    /// types. Atomic nodes are considered to be nodes with 0 children and an
    /// empty vector is returned.
    pub fn children(&self) -> Vec<&Child> {
        match self {
            Node::List(_, children) => children.iter().collect(),
            Node::Styled(_, child) => vec![child],
            _ => Vec::new(),
        }
    }

    /// Returns the single child of the node if it has exactly one child, an
    /// error otherwise.
    pub fn child(&self) -> Result<&Child> {
        match self {
            Node::List(_, children) if children.len() == 1 => Ok(children.iter().next().unwrap()),
            Node::Styled(_, child) => Ok(child),
            _ => Err(Error::ChildrenMismatch(
                self.children().into_iter().cloned().collect(),
            )),
        }
    }

    /// Returns true if the node can never contain any children.
    ///
    /// Empty list nodes are not atomic, because they could in principle contain
    /// children.
    ///
    /// See the [data model docs](super) for more details.
    pub fn is_atom(&self) -> bool {
        match &self {
            Node::Text(_) => true,
            Node::List(_, _) => false,
            Node::Styled(_, _) => false,
        }
    }

    /// Returns true if the node can contain children.
    ///
    /// Empty list nodes are collections, because they could in principle
    /// contain children.
    ///
    /// See the [data model docs](super) for more details.
    pub fn is_collection(&self) -> bool {
        !self.is_atom()
    }

    /// Returns the text of the node if it is a text node, an error otherwise.
    pub fn str(&self) -> Result<&str> {
        match self {
            Node::Text(Line(t)) => Ok(t.as_str()),
            _ => Err(Error::wrong_node_type("Text", self)),
        }
    }

    /// Returns the layout of the node if it is a list node, an error otherwise.
    pub fn layout(&self) -> Result<Layout> {
        match self {
            Node::List(layout, _) => Ok(*layout),
            _ => Err(Error::wrong_node_type("List", self)),
        }
    }

    /// Returns the styles of the node if it is a styled node, an error
    /// otherwise.
    pub fn styles(&self) -> Result<&Styles> {
        match self {
            Node::Styled(styles, _) => Ok(styles),
            _ => Err(Error::wrong_node_type("Styled", self)),
        }
    }
}

/// The positioning of a [list node's](Node::List) children relative to each
/// other.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Layout {
    /// Children are displayed inline, as spans, directly after one another.
    Chain,
    /// Children are displayed as separate blocks on a vertical axis.
    Page,
}

/// A set of either block styles or span styles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Styles {
    /// A set of block styles, such as heading and quote.
    Block(BTreeSet<BlockStyle>),
    /// A set of span styles, such as bold and italic.
    Span(BTreeSet<SpanStyle>),
}

impl Styles {
    fn is_empty(&self) -> bool {
        match self {
            Styles::Block(s) => s.is_empty(),
            Styles::Span(s) => s.is_empty(),
        }
    }
}

impl From<BlockStyle> for Styles {
    fn from(style: BlockStyle) -> Self {
        let mut styles = BTreeSet::new();
        styles.insert(style);
        Self::Block(styles)
    }
}

impl From<SpanStyle> for Styles {
    fn from(style: SpanStyle) -> Self {
        let mut styles = BTreeSet::new();
        styles.insert(style);
        Self::Span(styles)
    }
}

impl From<BTreeSet<BlockStyle>> for Styles {
    fn from(styles: BTreeSet<BlockStyle>) -> Self {
        Self::Block(styles)
    }
}

impl From<BTreeSet<SpanStyle>> for Styles {
    fn from(styles: BTreeSet<SpanStyle>) -> Self {
        Self::Span(styles)
    }
}

/// Inline styles that apply to one or more spans of text or other content.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum SpanStyle {
    /// Inline text shown in bold type, visually distinct from surrounding text.
    Bold,
    /// Inline text shown in italic or oblique type, to emphasize it.
    Italic,
    /// Inline text shown strikethrough or crossed out.
    Struck,
    /// Inline text shown verbatim in monospaced type.
    Mono,
    /// Inline text highlighted or marked to separate it from its context.
    Marked,
}

/// Block styles that apply to one or more whole blocks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum BlockStyle {
    /// Block shown with a larger type size or otherwise marked as a heading of
    /// the following blocks.
    Heading,
    /// Bulleted and indented block, usually applied to a list of blocks.
    List,
    /// Quoted block for text or other content.
    Quote,
    /// De-emphasized block shown indented slightly to the side or otherwise
    /// distinct from the main flow.
    Aside,
}

/// A node that is contained by a parent node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Child {
    /// A child that contains only an id and needs to load its contents from the
    /// DB.
    Lazy(Id),
    /// A child that directly contains the data of the node (but which may still
    /// contain lazy children).
    Eager(Node),
}

impl Child {
    /// Returns the id of the child if the child is lazy, an error otherwise.
    pub fn id(&self) -> Result<Id> {
        match self {
            Self::Lazy(id) => Ok(*id),
            _ => Err(Error::wrong_child_type("Lazy", self)),
        }
    }

    /// Returns the node of the child if the child is eager, an error otherwise.
    pub fn node(&self) -> Result<&Node> {
        match self {
            Self::Eager(n) => Ok(n),
            _ => Err(Error::wrong_child_type("Eager", self)),
        }
    }
}

impl From<Node> for Child {
    fn from(n: Node) -> Self {
        Self::Eager(n)
    }
}

impl From<Id> for Child {
    fn from(id: Id) -> Self {
        Self::Lazy(id)
    }
}

/// A node that contains a child node at the specified index.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
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

/// A set of [parents](Parent).
pub type Parents = HashSet<Parent>;

/// A search result matching a particular search term.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Overlap {
    /// The id of the node that contains the match
    pub id: Id,
    a: u8,
    b: u8,
    intersection: u8,
}

impl Overlap {
    pub(crate) fn new(id: Id, source_count: u32, match_count: u32, intersection: u32) -> Self {
        let max_count = max(source_count, match_count);
        Overlap {
            id,
            a: (255 * source_count / max_count) as u8,
            b: (255 * match_count / max_count) as u8,
            intersection: (255 * intersection / max_count) as u8,
        }
    }

    /// Returns the size of the source node in relation to the maximum of source
    /// node and match node.
    ///
    /// If an overlap is visualized as a Venn diagram, this function returns the
    /// area of the circle representing the source node in relation to the area
    /// of the bigger of the two circles.
    pub fn source_size(&self) -> f32 {
        self.a as f32 / 255.0
    }

    /// Returns the size of the match node in relation to the maximum of source
    /// node and match node.
    ///
    /// If an overlap is visualized as a Venn diagram, this function returns the
    /// area of the circle representing the match node in relation to the area
    /// of the bigger of the two circles.
    pub fn match_size(&self) -> f32 {
        self.b as f32 / 255.0
    }

    /// Returns the size of the intersection of source node and match node in
    /// relation to the maximum of source node and match node.
    ///
    /// If an overlap is visualized as a Venn diagram, this function returns the
    /// area of the intersection of the two circles in relation to the area of
    /// the bigger of the two circles.
    pub fn intersection_size(&self) -> f32 {
        self.intersection as f32 / 255.0
    }

    /// Returns the overlap between search term and match result as a score
    /// between 0 and 1 (0 if there is no overlap, 1 if the search node is
    /// completely contained in the match node or vice versa).
    ///
    /// Equal to `intersection / min(source_node, match_node)`.
    pub fn score(&self) -> f32 {
        self.intersection as f32 / (min(self.a, self.b) as f32)
    }

    // Constructs the reverse overlap for the specified node, which has the same
    // intersection but swaps source gram count and match gram count.
    pub(crate) fn reverse(&self, id: Id) -> Self {
        Self {
            id,
            a: self.b,
            b: self.a,
            intersection: self.intersection,
        }
    }
}

impl PartialOrd for Overlap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Overlap {
    fn cmp(&self, other: &Self) -> Ordering {
        self.intersection
            .cmp(&other.intersection)
            .reverse()
            .then(self.a.cmp(&other.a).reverse())
            .then(self.b.cmp(&other.b).reverse())
    }
}

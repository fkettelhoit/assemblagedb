# Distributed Document/Graph DB for Connected Pages

AssemblageDB is a transactional high-level database for connected webs of pages,
notes, texts and other media. Think of it like a _personal web_, but easily
editable, with more connections and better navigation than the web. It is
high-level in the sense that it defines a document model similar to HTML but
vastly simpler and with graph-like 2-way links instead of tree-like 1-way jump
links. The data model is both:

  - _document-oriented:_ supports nested documents without a fixed schema
  - _graph-based:_ documents can have multiple parents and form a directed,
    possibly cyclic graph

## Features

  - _versioned:_ old values remain accessible until merged
  - _transactional:_ snapshots are isolated through
    [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
  - _storage-agnostic:_ supports native and wasm targets
  - _indexed:_ maintains an automatic index for similarity/overlap search
  - _distributed:_ nodes can be published/subscribed as remote broadcasts

## Data Model

Nodes in an AssemblageDB can be either atomic (a line of text for example) or
nested, either in a _list_ containing multiple children or a _styled_ node
containing just a single child. `List` nodes have a _layout_, which controls how
children are laid out in relation to each other, while `Styled` nodes have zero
or more _block styles_ or _span styles_, that control how their (possibly
nested) child is displayed. Examples for layouts and styles are:

  - `Layout::Chain`: lays out children as a consecutive chain of inline spans.
    With 2 text children "foo" and "bar", the chain would be displayed as
    "foobar".
  - `Layout::Page`: lays out children as blocks on a page, separated vertically
    by a new line. With 2 text children "foo" and "bar", the page would be
    displayed as 2 lines, the first line containing "foo", the second line
    containing "bar".
  - `SpanStyle::Italic`: A span (inline) style that would display the child
    "foo" as "_foo_"
  - `BlockStyle::Heading`: A block style that would display the child "foo" in
    its own block with a larger font size.

A node is always either a _span_ or a _block_. Text nodes are considered to be
spans by default and remain spans if styled using span styles such as
`SpanStyle::Italic` or `SpanStyle::Bold`. However, a single block style (such as
`BlockStyle::Heading`) in a set of styles is always "contagious" and turns a
text node "foo" styled with both `SpanStyle::Italic` and `BlockStyle::Heading`
into a block. Similarly, layouts control whether a list is displayed as a span
or a block: `Layout::Chain` turns a list into a span, while `Layout::Page` turns
a list into a sequence of blocks.

## Obligatory Warning

AssemblageDB is still in an experimental stage. It should go without saying that
it is not a battle-tested production-ready database and could at any time eat
all of your data. **If you need to persist production data, use a real database
instead.**

## Example

```rust
let storage = FileStorage::open("db1").await?;
let db = Db::open(storage).await?;

// Nodes support layouts and styles, for example as a page of blocks...
let page1_id = tx!(|db| {
    db.add(Node::list(
        Layout::Page,
        vec![
            Node::styled(BlockStyle::Heading, Node::text("A Heading!")),
            Node::text("This is the first paragraph."),
            Node::text("Unsurprisingly this is the second one..."),
        ],
    ))
    .await?
});

// ...or as inline spans that are chained together:
let page2_id = tx!(|db| {
    db.add(Node::list(
        Layout::Page,
        vec![Node::list(
            Layout::Chain,
            vec![
                Node::text("And this is the "),
                Node::styled(SpanStyle::Italic, Node::text("last")),
                Node::text(" paragraph..."),
            ],
        )],
    ))
    .await?
});

// Documents can form a graph, with nodes keeping track of all parents:
tx!(|db| {
    db.add(Node::list(Layout::Page, vec![page1_id, page1_id])).await?;
    assert_eq!(db.parents(page1_id).await?.len(), 2);
    assert_eq!(db.parents(page2_id).await?.len(), 0);
});

// All text is indexed, the DB supports "overlap" similarity search:
tx!(|db| {
    let paragraph1_id = db.get(page1_id).await?.unwrap().children()[1].id()?;
    let paragraph3_id = db.get(page2_id).await?.unwrap().children()[0].id()?;
    let overlaps_of_p1 = db.overlaps(paragraph1_id).await?;
    assert_eq!(overlaps_of_p1.len(), 1);
    assert_eq!(overlaps_of_p1[0].id, paragraph3_id);
    assert!(overlaps_of_p1[0].score() > 0.5);
});

// Nodes (with all their descendants) can be published globally...
let broadcast = tx!(|db| { db.publish_broadcast(page1_id).await? });

// ...and the broadcast can then be fetched in another remote DB:
let other_storage = FileStorage::open("db2").await?;
let db2 = Db::open(other_storage).await?;
tx!(|db2| {
    let paragraph_id = db2
        .add(Node::text("This is the first paragraph, right?"))
        .await?;
    db2.add(Node::list(Layout::Page, vec![paragraph_id]))
        .await?;
    // The DB is empty except for this paragraph, so no overlaps:
    assert_eq!(db2.overlaps(paragraph_id).await?.len(), 0);
    // But when the broadcast paragraph is fetched, there is an overlap:
    db2.fetch_broadcast(&broadcast.broadcast_id).await?;
    assert_eq!(db2.overlaps(paragraph_id).await?.len(), 1);
});
```

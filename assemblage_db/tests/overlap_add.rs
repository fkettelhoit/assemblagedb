#![allow(clippy::float_cmp)]

use assemblage_db::{
    data::{Child, Layout, Node},
    tx, Db, Result,
};
use assemblage_kv::test;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn overlap_between_text_nodes(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("This is a sentence about foo.")).await?);
        let bar_id = tx!(|db| db.add(Node::text("This is a sentence about bar.")).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        let overlaps = db.current().await.overlaps(bar_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, foo_id);
        assert!(overlaps[0].score() < 1.0);
        assert!(overlaps[0].score() > 0.8);

        let overlaps = db.current().await.overlaps(foo_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, bar_id);
        assert!(overlaps[0].score() < 1.0);
        assert!(overlaps[0].score() > 0.8);
    }
}

test! {
    async fn overlap_between_children(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let t = "Here is some text that is used in multiple nodes";
        let t1_id = tx!(|db| db.add(Node::text(t)).await?);
        let t2_id = tx!(|db| db.add(Node::text(t)).await?);

        let page1_id = tx!(|db| db.add(Node::list(Layout::Page, vec![t1_id])).await?);
        let page2_id = tx!(|db| db.add(Node::list(Layout::Page, vec![t2_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![
            page1_id,
            page2_id,
        ])).await?);

        let overlaps = db.current().await.overlaps(t1_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, t2_id);
        assert_eq!(overlaps[0].score(), 1.0);

        let overlaps = db.current().await.overlaps(t2_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, t1_id);
        assert_eq!(overlaps[0].score(), 1.0);
    }
}

test! {
    async fn no_overlap_between_parent_and_child(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text_id = tx!(|db| db.add(Node::text("some text")).await?);
        let page_id = tx!(|db| db.add(Node::list(Layout::Page, vec![text_id])).await?);
        let link_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![page_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![link_id])).await?);

        assert_eq!(db.current().await.overlaps(text_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(link_id).await?.len(), 0);
    }
}

test! {
    async fn no_overlap_between_ancestor_and_child_span(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text_id = tx!(|db| db.add(Node::text("some text")).await?);
        let page_id = tx!(|db| db.add(Node::list(Layout::Page, vec![text_id])).await?);
        let link_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("Here is the link: "),
            Node::list(Layout::Chain, vec![page_id]),
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![link_id])).await?);

        assert_eq!(db.current().await.overlaps(text_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(link_id).await?.len(), 0);
    }
}

test! {
    async fn no_overlap_between_ancestor_and_child_block(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text_id = tx!(|db| db.add(Node::text("some text")).await?);
        let page_id = tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::list(Layout::Page, vec![text_id])
        ])).await?);
        let link_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("Here is the link: "),
            Node::list(Layout::Chain, vec![page_id]),
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![link_id])).await?);

        assert_eq!(db.current().await.overlaps(text_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(link_id).await?.len(), 0);
    }
}

test! {
    async fn overlap_between_two_paragraphs(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let paragraph1_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("This is the first paragraph that ends with the words: "),
            Node::text("foo, bar, baz, qux")
        ])).await?);

        let paragraph2_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("This is the second paragraph that ends with the words: "),
            Node::text("foo, bar, baz, qux")
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![paragraph1_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![paragraph2_id])).await?);

        let overlaps = db.current().await.overlaps(paragraph1_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, paragraph2_id);

        let overlaps = db.current().await.overlaps(paragraph2_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, paragraph1_id);

        let _chain_including_paragraph1_id = tx!(|db| db.add(Node::List(Layout::Page, vec![
            Child::Eager(Node::text("Some more text here...")),
            Child::Lazy(paragraph1_id),
        ])).await?);

        let overlaps = db.current().await.overlaps(paragraph1_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, paragraph2_id);

        let overlaps = db.current().await.overlaps(paragraph2_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, paragraph1_id);
    }
}

test! {
    async fn symmetric_overlap_between_nodes_with_different_gram_count(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let long_text = "This contains foo and also much more,\
            but it should still be found by the overlap search\
            no matter whether we search from long text to short text\
            or vice versa (overlaps are always symmetric)";

        let short_text = "This contains foo";

        let long_text_id = tx!(|db| db.add(Node::text(long_text)).await?);
        let short_text_id = tx!(|db| db.add(Node::text(short_text)).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![long_text_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![short_text_id])).await?);

        let long_text_overlaps = db.current().await.overlaps(long_text_id).await?;
        assert_eq!(long_text_overlaps.len(), 1);

        assert_eq!(long_text_overlaps[0].source_size(), 1.0);
        assert!(long_text_overlaps[0].score() > 0.8);
        assert!(long_text_overlaps[0].match_size() > 0.0);
        assert!(long_text_overlaps[0].match_size() < 1.0);
        assert!(long_text_overlaps[0].intersection_size() < long_text_overlaps[0].match_size());

        let short_text_overlaps = db.current().await.overlaps(short_text_id).await?;
        assert_eq!(short_text_overlaps.len(), 1);

        assert_eq!(short_text_overlaps[0].match_size(), 1.0);
        assert!(short_text_overlaps[0].score() > 0.8);
        assert!(short_text_overlaps[0].source_size() > 0.0);
        assert!(short_text_overlaps[0].source_size() < 1.0);
        assert!(short_text_overlaps[0].intersection_size() < short_text_overlaps[0].source_size());
    }
}

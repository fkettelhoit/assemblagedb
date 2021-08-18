#![allow(clippy::float_cmp)]

use assemblage_db::{
    data::{Id, Layout, Node},
    tx, Db, Result,
};
use assemblage_kv::test;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn overlap_after_swap(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("This is a sentence containing foo")).await?);
        let bar_id = tx!(|db| db.add(Node::text("This is a sentence containing bar")).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);

        tx!(|db| db.swap(foo_id, Node::text("something completely different")).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 0);

        tx!(|db| db.swap(foo_id, Node::text("sentence containing foo")).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);
    }
}

test! {
    async fn overlap_after_swap_of_nested_child(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text_foo_id = tx!(|db| db.add(Node::text("This is a sentence containing foo")).await?);
        let text_bar_id = tx!(|db| db.add(Node::text("This is a sentence containing bar")).await?);

        let foo_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![text_foo_id])).await?);
        let bar_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![text_bar_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);

        tx!(|db| db.swap(foo_id, Node::text("something completely different")).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 0);

        tx!(|db| db.swap(foo_id, Node::text("sentence containing foo")).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);
    }
}

test! {
    async fn overlap_after_push(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("Thi"),
        ])).await?);

        let bar_id = tx!(|db| db.add(Node::text("This is a sentence about bar")).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 0);

        tx!(|db| db.push(foo_id, Node::text("s a sentence about foo")).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);
    }
}

test! {
    async fn overlap_after_remove(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("foo"),
            Node::text("bar"),
            Node::text("baz"),
            Node::text("qux"),
        ])).await?);

        let bar_id = tx!(|db| db.add(Node::text("just fooqux")).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 0);

        tx!(|db| db.remove(foo_id, 2).await?);
        tx!(|db| db.remove(foo_id, 1).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);
    }
}

test! {
    async fn overlap_after_replacing_empty_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let shared_text = "This is a sentence about something";

        let foo_id = tx!(|db| db.add(Node::text("")).await?);
        let bar_id = tx!(|db| db.add(Node::text("")).await?);

        let page_foo_id = tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        let page_bar_id = tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 0);

        tx!(|db| db.replace(page_foo_id, 0, Node::text(shared_text)).await?);
        tx!(|db| db.replace(page_bar_id, 0, Node::text(shared_text)).await?);

        let foo_id = tx!(|db| db.get(page_foo_id).await?.unwrap().children()[0].id()?);
        let bar_id = tx!(|db| db.get(page_bar_id).await?.unwrap().children()[0].id()?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);
    }
}

test! {
    async fn overlap_after_incrementally_replacing_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("")).await?);
        let bar_id = tx!(|db| db.add(Node::text("")).await?);

        let page_foo_id = tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        let page_bar_id = tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 0);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 0);

        tx!(|db| db.replace(page_foo_id, 0, Node::text("This")).await?);
        tx!(|db| db.replace(page_foo_id, 0, Node::text("This is")).await?);
        tx!(|db| db.replace(page_foo_id, 0, Node::text("This is another")).await?);
        tx!(|db| db.replace(page_foo_id, 0, Node::text("This is another sentence")).await?);
        tx!(|db| db.replace(page_foo_id, 0, Node::text("This is another sentence about")).await?);
        tx!(|db| db.replace(page_foo_id, 0, Node::text("This is another sentence about foo")).await?);

        tx!(|db| db.replace(page_bar_id, 0, Node::text("Just")).await?);
        tx!(|db| db.replace(page_bar_id, 0, Node::text("Just another")).await?);
        tx!(|db| db.replace(page_bar_id, 0, Node::text("Just another sentence")).await?);
        tx!(|db| db.replace(page_bar_id, 0, Node::text("Just another sentence about")).await?);
        tx!(|db| db.replace(page_bar_id, 0, Node::text("Just another sentence about bar")).await?);

        let foo_id = tx!(|db| db.get(page_foo_id).await?.unwrap().children()[0].id()?);
        let bar_id = tx!(|db| db.get(page_bar_id).await?.unwrap().children()[0].id()?);

        assert_eq!(db.current().await.overlaps(foo_id).await?.len(), 1);
        assert_eq!(db.current().await.overlaps(bar_id).await?.len(), 1);
    }
}

test! {
    async fn no_overlap_between_ancestor_and_child_after_replace(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let ancestor_page_id = tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::list(Layout::Page, vec![Node::text("")]),
        ])).await?);

        let text1_id = tx!(|db| db.add(Node::text("some text")).await?);

        let page_id = tx!(|db| db.add(Node::list(Layout::Page, vec![text1_id])).await?);

        tx!(|db| db.insert(ancestor_page_id, 0, page_id).await?);

        assert_eq!(db.current().await.overlaps(text1_id).await?.len(), 0);

        let text2_id = tx!(|db| db.add(Node::text("some text")).await?);

        tx!(|db| db.remove(page_id, 0).await?);
        tx!(|db| db.insert(page_id, 0, text2_id).await?);
    }
}

test! {
    async fn overlaps_after_restoring_node(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text1_id = tx!(|db| {
            db.add(Node::text("this should be found as an overlap")).await?
        });
        let text2_id = tx!(|db| {
            db.add(Node::text("this should be found as an overlap...")).await?
        });

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![text1_id])).await?
        });

        tx!(|db| db.push(Id::root(), page1_id).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![text2_id])).await?);

        tx!(|db| {
            assert_eq!(db.overlaps(text1_id).await?.len(), 1);
            assert_eq!(db.overlaps(text1_id).await?[0].id, text2_id);
            assert_eq!(db.overlaps(text2_id).await?.len(), 1);
            assert_eq!(db.overlaps(text2_id).await?[0].id, text1_id);
        });

        tx!(|db| db.remove(Id::root(), 0).await?);

        tx!(|db| {
            assert_eq!(db.get(text1_id).await?, None);
            assert_eq!(db.overlaps(text2_id).await?.len(), 0);
            assert!(db.overlaps(text1_id).await.is_err());
        });

        tx!(|db| db.restore(page1_id).await?);

        tx!(|db| {
            assert!(db.get(text1_id).await?.is_some());
            assert_eq!(db.overlaps(text1_id).await?.len(), 1);
            assert_eq!(db.overlaps(text1_id).await?[0].id, text2_id);
            assert_eq!(db.overlaps(text2_id).await?.len(), 1);
            assert_eq!(db.overlaps(text2_id).await?[0].id, text1_id);
        });
    }
}

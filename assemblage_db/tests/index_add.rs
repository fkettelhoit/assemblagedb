#![allow(clippy::float_cmp)]

use assemblage_db::{
    data::{Id, Layout, Node, Overlap},
    tx, Db, Result,
};
use assemblage_kv::{storage, storage::Storage, test};
use std::collections::HashSet;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn index_text_nodes(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
        let bar_id = tx!(|db| db.add(Node::text("bar")).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 0);

        tx!(|db| db.add(Node::list(Layout::Chain, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Chain, vec![bar_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 0);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_id);
    }
}

test! {
    async fn index_fuzzy_text_nodes(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("This is the text foo")).await?);
        let bar_id = tx!(|db| db.add(Node::text("This is the text bar")).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_id);

        let matches: Vec<Overlap> = db.current().await.search("text foo").await?
            .into_iter()
            .filter(|m| m.score() > 0.7)
            .collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_id);

        let matches = db.current().await.search("This is the text foo").await?;
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].id, foo_id);
        assert_eq!(matches[0].score(), 1.0);
        assert_eq!(matches[1].id, bar_id);
        assert!(matches[1].score() < 1.0);

        let foo2_id = tx!(|db| db.add(Node::text("Another text that is the text foo")).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![foo2_id])).await?);

        let matches: Vec<Overlap> = db.current().await.search("text foo").await?
            .into_iter()
            .filter(|m| m.score() > 0.7)
            .collect();
        assert_eq!(matches.len(), 2);

        let matches = db.current().await.search("This is the text foo").await?;
        assert_eq!(matches.len(), 3);
    }
}

test! {
    async fn index_parent_chains_of_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let prefix_id = tx!(|db| db.add(Node::text("This is t")).await?);
        let foo_suffix_id = tx!(|db| db.add(Node::text("he text foo")).await?);
        let bar_suffix_id = tx!(|db| db.add(Node::text("he text bar")).await?);

        let foo_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![prefix_id, foo_suffix_id])).await?);
        let bar_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![prefix_id, bar_suffix_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

        let matches: Vec<Overlap> = db.current().await.search("text foo").await?
            .into_iter()
            .filter(|m| m.score() > 0.7)
            .collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_id);
        assert_eq!(matches[0].score(), 1.0);

        let matches: Vec<Overlap> = db.current().await.search("is the text").await?
            .into_iter()
            .filter(|m| m.score() > 0.7)
            .collect();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].score(), 1.0);
        assert_eq!(matches[1].score(), 1.0);
        let ids: HashSet<Id> = matches.into_iter().map(|m| m.id).collect();
        assert!(ids.contains(&foo_id));
        assert!(ids.contains(&bar_id));

        let matches: Vec<Overlap> = db.current().await.search("This is the text foo").await?
        .into_iter()
        .filter(|m| m.score() > 0.7)
        .collect();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].id, foo_id);
        assert_eq!(matches[0].score(), 1.0);
        assert_eq!(matches[1].id, bar_id);
        assert!(matches[1].score() < 1.0);
    }
}

test! {
    async fn index_both_direct_and_linked_children(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo1_id = tx!(|db| db.add(Node::text("foo")).await?);
        let foo2_id = tx!(|db| db.add(Node::text("foo")).await?);
        let page_foo1_id = tx!(|db| db.add(Node::list(Layout::Page, vec![foo1_id])).await?);
        let link_foo1_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![page_foo1_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo1_id);

        tx!(|db| db.add(Node::list(Layout::Page, vec![link_foo1_id, foo2_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 2);
        let ids: HashSet<Id> = matches.into_iter().map(|m| m.id).collect();
        assert!(ids.contains(&foo1_id));
        assert!(ids.contains(&foo2_id));
    }
}

test! {
    async fn index_long_word_built_out_of_children(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let chain_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("foo"),
            Node::text("bar"),
            Node::text("baz"),
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![chain_id])).await?);

        let matches = db.current().await.search("foobarbaz").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, chain_id);
        assert_eq!(matches[0].score(), 1.0);
    }
}

test! {
    async fn index_empty_chains(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::List(Layout::Chain, vec![]),
            Node::List(Layout::Chain, vec![]),
        ])).await?);

        let matches = db.current().await.search("foobarbaz").await?;
        assert_eq!(matches.len(), 0);

        tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::list(Layout::Page, vec![
                Node::List(Layout::Chain, vec![])
            ]),
            Node::List(Layout::Chain, vec![]),
            Node::list(Layout::Chain, vec![
                Node::text("foo"),
                Node::List(Layout::Chain, vec![]),
                Node::text("bar"),
            ]),
            Node::list(Layout::Chain, vec![
                Node::text("baz"),
            ]),
        ])).await?);

        let matches: Vec<Overlap> = db.current().await.search("foobar").await?
            .into_iter()
            .filter(|m| m.score() == 1.0)
            .collect();
        assert_eq!(matches.len(), 1);

        let matches: Vec<Overlap> = db.current().await.search("baz").await?
            .into_iter()
            .filter(|m| m.score() == 1.0)
            .collect();
        assert_eq!(matches.len(), 1);

        let matches: Vec<Overlap> = db.current().await.search("foobarbaz").await?
            .into_iter()
            .filter(|m| m.score() == 1.0)
            .collect();
        assert_eq!(matches.len(), 0);

        let matches: Vec<Overlap> = db.current().await.search("foo").await?
            .into_iter()
            .filter(|m| m.score() == 1.0)
            .collect();
        assert_eq!(matches.len(), 0);
    }
}

test! {
    async fn index_and_store_index(storage) -> Result<()> {
        let storage_name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let foo_id = {
            let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
            let bar_id = tx!(|db| db.add(Node::text("bar")).await?);

            tx!(|db| db.add(Node::list(Layout::Page, vec![foo_id])).await?);
            tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);

            foo_id
        };

        db.merge().await?;
        let storage = storage::open(&storage_name).await?;
        let db = Db::open(storage).await?;

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_id);
    }
}

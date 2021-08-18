#![allow(clippy::float_cmp)]

use assemblage_db::{
    data::{Layout, Node, Overlap},
    tx, Db, Result,
};
use assemblage_kv::test;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn index_after_pushing_node_to_chain(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
        let foo_chain_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![foo_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![foo_chain_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_chain_id);

        tx!(|db| db.push(foo_chain_id, Node::text("bar")).await?);

        let matches = db.current().await.search("foobar").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, foo_chain_id);
        assert_eq!(matches[0].score(), 1.0);

        let matches: Vec<Overlap> = db.current().await.search("foo").await?
            .into_iter()
            .filter(|m| m.score() > 0.7)
            .collect();
        assert_eq!(matches.len(), 0);
    }
}

test! {
    async fn index_cyclic_structure(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
        let chain_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![foo_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![chain_id])).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, chain_id);

        tx!(|db| db.push(chain_id, chain_id).await?);

        let matches = db.current().await.search("foo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, chain_id);
    }
}

test! {
    async fn index_after_inserting_nodes(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let chain_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("foo"),
            Node::text("qux"),
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![chain_id])).await?);

        let matches = db.current().await.search("fooqux").await?;
        assert_eq!(matches.len(), 1);

        tx!(|db| db.insert(chain_id, 1, Node::text("barbaz")).await?);

        let matches = db.current().await.search("fooqux").await?;
        assert_eq!(matches.len(), 0);

        let matches = db.current().await.search("foobarbazqux").await?;
        assert_eq!(matches.len(), 1);
    }
}

test! {
    async fn index_after_swaps(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let prefix_id = tx!(|db| db.add(Node::text("This is a sentence containing f")).await?);
        let suffix_id = tx!(|db| db.add(Node::text("ooo")).await?);
        let chain_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![prefix_id, suffix_id])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![chain_id])).await?);

        let matches = db.current().await.search("fooo").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].score(), 1.0);

        tx!(|db| db.swap(prefix_id, Node::text("just b")).await?);
        tx!(|db| db.swap(suffix_id, Node::text("arr")).await?);

        let matches = db.current().await.search("fooo").await?;
        assert_eq!(matches.len(), 0);

        let matches = db.current().await.search("barr").await?;
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].score(), 1.0);
    }
}

test! {
    async fn index_after_replacing_a_link(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
        let bar_id = tx!(|db| db.add(Node::text("bar")).await?);
        let link_to_bar_id = tx!(|db| db.add(Node::list(Layout::Page, vec![bar_id])).await?);
        tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::list(Layout::Chain, vec![
                foo_id,
                link_to_bar_id
            ])
        ])).await?);

        let _to_avoid_orphan_deletion_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![bar_id])).await?);

        let matches = db.current().await.search("bar").await?;
        assert_eq!(matches.len(), 1);

        let matches = db.current().await.search("qux").await?;
        assert_eq!(matches.len(), 0);

        // link texts are never indexed, only the children they point to
        let matches = db.current().await.search("foobar").await?;
        assert_eq!(matches.len(), 0);

        let matches = db.current().await.search("fooqux").await?;
        assert_eq!(matches.len(), 0);

        tx!(|db| db.replace(link_to_bar_id, 0, Node::text("qux")).await?);

        let matches = db.current().await.search("bar").await?;
        assert_eq!(matches.len(), 0);

        let matches = db.current().await.search("qux").await?;
        assert_eq!(matches.len(), 1);

        let matches = db.current().await.search("foobar").await?;
        assert_eq!(matches.len(), 0);

        // link texts are never indexed, only the children they point to
        let matches = db.current().await.search("fooqux").await?;
        assert_eq!(matches.len(), 0);
    }
}

test! {
    async fn index_after_removing_node(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let chain_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("foo"),
            Node::text("bar"),
            Node::text("baz"),
            Node::text("qux"),
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![chain_id])).await?);

        let matches = db.current().await.search("foobarbazqux").await?;
        assert_eq!(matches.len(), 1);

        let matches = db.current().await.search("fooqux").await?;
        assert_eq!(matches.len(), 0);

        tx!(|db| db.remove(chain_id, 2).await?);
        tx!(|db| db.remove(chain_id, 1).await?);

        let matches = db.current().await.search("foobarbazqux").await?;
        assert_eq!(matches.len(), 0);

        let matches = db.current().await.search("fooqux").await?;
        assert_eq!(matches.len(), 1);
    }
}

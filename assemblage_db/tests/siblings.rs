use assemblage_db::{
    data::{BlockStyle, Child, Layout, Node},
    tx, Db, Result,
};
use assemblage_kv::test;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn siblings_of_text_nodes(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (first_id, foo_id, bar_id, baz_id, last_id) = tx!(|db| {
            let first_id = db.add(Node::text("first")).await?;
            let foo_id = db.add(Node::text("foo")).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let baz_id = db.add(Node::text("baz")).await?;
            let last_id = db.add(Node::text("last")).await?;
            (first_id, foo_id, bar_id, baz_id, last_id)
        });

        tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Page, vec![first_id, foo_id]),
                Node::list(Layout::Page, vec![Child::Lazy(bar_id)]),
                Node::list(Layout::Page, vec![baz_id, last_id]),
            ])).await?
        });

        tx!(|db| {
            assert_eq!(db.before(first_id).await?.len(), 0);

            assert_eq!(db.before(bar_id).await?.len(), 1);
            assert_eq!(db.before(bar_id).await?.into_iter().next().unwrap(), foo_id);

            assert_eq!(db.after(bar_id).await?.len(), 1);
            assert_eq!(db.after(bar_id).await?.into_iter().next().unwrap(), baz_id);

            assert_eq!(db.after(last_id).await?.len(), 0);
        });
    }
}

// This test is similar to the one above, except we use pages as links:
test! {
    async fn siblings_of_nested_pages(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (first_id, foo_id, bar_id, baz_id, last_id) = tx!(|db| {
            let first_id = db.add(Node::text("first")).await?;
            let foo_id = db.add(Node::text("foo")).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let baz_id = db.add(Node::text("baz")).await?;
            let last_id = db.add(Node::text("last")).await?;
            (first_id, foo_id, bar_id, baz_id, last_id)
        });

        let foobarbaz_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Page, vec![first_id, foo_id]),
                Node::list(Layout::Page, vec![bar_id]),
                Node::list(Layout::Page, vec![baz_id, last_id]),
            ])).await?
        });

        // The whole foobarbaz page is inside a _chain_ inside a document,
        // so it will be displayed as a link span, not a block. As a
        // consequence, the sibling search should never cross these page
        // "boundaries" and last_id is not considered a before-sibling of
        // first_id or vice versa.
        tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Chain, vec![foobarbaz_id]),
                Node::list(Layout::Chain, vec![foobarbaz_id]),
            ])).await?
        });

        tx!(|db| {
            assert_eq!(db.before(first_id).await?.len(), 0);

            assert_eq!(db.before(bar_id).await?.len(), 1);
            assert_eq!(db.before(bar_id).await?.into_iter().next().unwrap(), foo_id);

            assert_eq!(db.after(bar_id).await?.len(), 1);
            assert_eq!(db.after(bar_id).await?.into_iter().next().unwrap(), baz_id);

            assert_eq!(db.after(last_id).await?.len(), 0);
        });
    }
}

test! {
    async fn siblings_of_linked_page(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foobar_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("foo"),
                Node::text("bar"),
            ])).await?
        });

        let text_id = tx!(|db| db.add(Node::text("text after foobar")).await?);

        tx!(|db| {
            db.add(Node::List(Layout::Page, vec![
                Child::Eager(Node::list(Layout::Chain, vec![foobar_id])),
                Child::Lazy(text_id),
            ])).await?
        });

        let before_text = tx!(|db| db.before(text_id).await?);
        assert_eq!(before_text.len(), 1);
        assert_eq!(before_text.into_iter().next().unwrap(), foobar_id);
    }
}

test! {
    async fn blank_siblings(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (first_id, foo_id, bar_id, baz_id, qux_id, last_id) = tx!(|db| {
            let first_id = db.add(Node::text("first")).await?;
            let foo_id = db.add(Node::text("")).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let baz_id = db.add(Node::text("   ")).await?;
            let qux_id = db.add(Node::text("   ")).await?;
            let last_id = db.add(Node::text("last")).await?;
            (first_id, foo_id, bar_id, baz_id, qux_id, last_id)
        });

        tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Page, vec![first_id]),
                Node::list(Layout::Page, vec![foo_id]),
                Node::list(Layout::Page, vec![bar_id]),
                Node::list(Layout::Page, vec![baz_id, qux_id, last_id]),
            ])).await?
        });

        tx!(|db| {
            assert_eq!(db.before(first_id).await?.len(), 0);
            assert_eq!(db.after(first_id).await?.len(), 1);
            assert_eq!(db.after(first_id).await?.into_iter().next().unwrap(), bar_id);

            assert_eq!(db.before(foo_id).await?.len(), 0);
            assert_eq!(db.after(foo_id).await?.len(), 0);

            assert_eq!(db.before(bar_id).await?.len(), 1);
            assert_eq!(db.before(bar_id).await?.into_iter().next().unwrap(), first_id);
            assert_eq!(db.after(bar_id).await?.len(), 1);
            assert_eq!(db.after(bar_id).await?.into_iter().next().unwrap(), last_id);

            assert_eq!(db.before(baz_id).await?.len(), 0);
            assert_eq!(db.after(baz_id).await?.len(), 0);

            assert_eq!(db.before(qux_id).await?.len(), 0);
            assert_eq!(db.after(qux_id).await?.len(), 0);

            assert_eq!(db.before(last_id).await?.len(), 1);
            assert_eq!(db.before(last_id).await?.into_iter().next().unwrap(), bar_id);
            assert_eq!(db.after(last_id).await?.len(), 0);
        });
    }
}

test! {
    async fn siblings_with_skipped_asides(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (first_id, foo_id, bar_id, baz_id, last_id, aside_id) = tx!(|db| {
            let first_id = db.add(Node::text("first")).await?;
            let foo_id = db.add(Node::text("foo")).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let baz_id = db.add(Node::text("baz")).await?;
            let last_id = db.add(Node::text("last")).await?;
            let aside_id = db.add(Node::styled(BlockStyle::Aside, Node::text("aside"))).await?;
            (first_id, foo_id, bar_id, baz_id, last_id, aside_id)
        });

        tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Page, vec![aside_id, first_id, foo_id, aside_id, aside_id]),
                Node::list(Layout::Page, vec![Child::Lazy(bar_id)]),
                Node::list(Layout::Page, vec![aside_id, aside_id, baz_id, last_id, aside_id]),
            ])).await?
        });

        tx!(|db| {
            assert_eq!(db.before(first_id).await?.len(), 0);

            assert_eq!(db.before(bar_id).await?.len(), 1);
            assert_eq!(db.before(bar_id).await?.into_iter().next().unwrap(), foo_id);

            assert_eq!(db.after(bar_id).await?.len(), 1);
            assert_eq!(db.after(bar_id).await?.into_iter().next().unwrap(), baz_id);

            assert_eq!(db.after(last_id).await?.len(), 0);
        });
    }
}

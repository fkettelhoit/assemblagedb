use assemblage_db::{
    data::{
        Child, Layout, Node,
        SpanStyle::{Bold, Italic},
        Styles,
    },
    tx, Db, PreviewedNode, Result,
};
use assemblage_kv::test;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn page_preview(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::styled(Italic, Node::text("foo")),
                Node::text("bar"),
            ])).await?
        });

        tx!(|db| {
            match db.preview(page_id).await? {
                PreviewedNode::Block(_, node) => {
                    assert_eq!(node.styles()?, &Styles::from(Italic));
                    assert_eq!(node.child()?.of(&db).await?.str()?, "foo");
                }
                p => panic!("Expected a block as preview, but found {:?}", p)
            }
        });
    }
}

test! {
    async fn chain_preview(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| {
            db.add(Node::list(Layout::Chain, vec![
                Node::styled(Bold, Node::text("foo")),
                Node::text("bar"),
            ])).await?
        });

        tx!(|db| {
            match db.preview(page_id).await? {
                PreviewedNode::Block(_, Node::List(Layout::Chain, children)) => {
                    assert_eq!(children.len(), 2);
                    let child_foo = children[0].of(&db).await?;
                    assert_eq!(child_foo.styles()?, &Styles::from(Bold));
                    assert_eq!(child_foo.child()?.of(&db).await?.str()?, "foo");
                    assert_eq!(children[1].of(&db).await?.str()?, "bar");
                },
                p => panic!("Expected a block as preview, but found {:?}", p)
            }
        });
    }
}

test! {
    async fn empty_page_preview(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Chain, vec![
                    Node::List(Layout::Page, vec![])
                ]),
                Node::List(Layout::Chain, vec![])
            ])).await?
        });

        tx!(|db| {
            match db.preview(page_id).await? {
                PreviewedNode::Empty => (),
                p => panic!("Expected an empty preview, but found {:?}", p)
            }
        });
    }
}

test! {
    async fn blank_text_preview(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text_id = tx!(|db| {
            db.add(Node::text("     ")).await?
        });

        tx!(|db| {
            match db.preview(text_id).await? {
                PreviewedNode::Empty => (),
                p => panic!("Expected an empty preview, but found {:?}", p)
            }
        });
    }
}

test! {
    async fn cyclic_preview(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| db.add(Node::List(Layout::Page, vec![])).await?);

        tx!(|db| {
            let cyclic = Node::List(Layout::Chain, vec![
                Child::Lazy(page_id),
                Child::Eager(Node::text("foo")),
            ]);
            db.push(page_id, Child::Eager(cyclic)).await?;
        });

        tx!(|db| {
            match db.preview(page_id).await? {
                PreviewedNode::Cyclic => (),
                p => panic!("Expected a cyclic preview, but found {:?}", p)
            }
        });
    }
}

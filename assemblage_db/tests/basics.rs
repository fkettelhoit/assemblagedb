use assemblage_db::{
    data::{Child, Layout, Node, Parent, SpanStyle, Styles},
    tx, Db, Result,
};
use assemblage_kv::{storage, storage::Storage, test};
use std::collections::HashSet;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn add_and_get_text_nodes(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text1 = Node::text("foo");
        let id1 = tx!(|db| db.add(text1).await?);
        tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

        let text2 = Node::text("foobar");
        let id2 = tx!(|db| db.add(text2).await?);
        tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "foobar"));
    }
}

test! {
    async fn add_and_get_text_with_newlines(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let lines = Node::text("Text\nwith\n\nnewlines!\n");
        let id = tx!(|db| db.add(lines).await?);

        tx!(|db| {
            let node = db.get(id).await?.unwrap();
            let layout = node.layout()?;
            let children = node.children();
            assert_eq!(layout, Layout::Page);
            assert_eq!(children.len(), 5);

            let lines = vec!["Text", "with", "", "newlines!", ""];
            for (child, line) in children.into_iter().zip(lines) {
                assert_eq!(db.get(child.id()?).await?.unwrap().str()?, line);
            }
        });
    }
}

test! {
    async fn add_and_get_chains(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text1 = Node::text("Some text1");
        let space = Node::text(" ");
        let text2 = Node::text("Some text2");

        let chain = Node::list(Layout::Chain, vec![text1.clone(), space.clone(), text2.clone()]);
        let chain_id = tx!(|db| db.add(chain).await?);

        let styled_chain = Node::list(
            Layout::Chain,
            vec![
                Node::styled(SpanStyle::Italic, text1.clone()),
                space.clone(),
                Node::styled(SpanStyle::Bold, text2.clone()),
            ],
        );
        let styled_chain_id = tx!(|db| db.add(styled_chain).await?);

        tx!(|db| {
            let chain = db.get(chain_id).await?.unwrap();
            let styled_chain = db.get(styled_chain_id).await?.unwrap();

            assert_eq!(chain.layout()?, Layout::Chain);
            assert_eq!(styled_chain.layout()?, Layout::Chain);

            let children = chain.children();
            assert_eq!(children.len(), 3);
            assert_eq!(children[0].of(&db).await?.str()?, text1.str()?);
            assert_eq!(children[1].of(&db).await?.str()?, space.str()?);
            assert_eq!(children[2].of(&db).await?.str()?, text2.str()?);

            let children = styled_chain.children();
            assert_eq!(children.len(), 3);
            assert_eq!(children[0].of(&db).await?.styles()?, &Styles::from(SpanStyle::Italic));
            assert_eq!(children[1].of(&db).await?.str()?, space.str()?);
            assert_eq!(children[2].of(&db).await?.styles()?, &Styles::from(SpanStyle::Bold));
        });
    }
}

test! {
    async fn add_and_get_parent_chains(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text1 = Node::text("foo");
        let parent1 = Node::list(Layout::Chain, vec![text1]);
        let parent1_id = tx!(|db| db.add(parent1).await?);

        let text1_id = tx!(|db| {
            let parent1 = db.get(parent1_id).await?.unwrap();
            let children = parent1.children();
            assert_eq!(children.len(), 1);

            let text1_id = children[0].id()?;
            let parents = db.parents(text1_id).await?;
            assert_eq!(parents.len(), 1);

            let Parent {id, index} = parents.into_iter().next().unwrap();
            assert_eq!(id, parent1_id);
            assert_eq!(index, 0);

            text1_id
        });

        let parent2 = Node::list(Layout::Chain, vec![parent1_id, text1_id]);
        let parent2_id = tx!(|db| db.add(parent2).await?);

        tx!(|db| {
            let parents = db.parents(text1_id).await?;
            assert_eq!(parents.len(), 2);

            let mut expected_parents = HashSet::new();
            expected_parents.insert(Parent::new(parent1_id, 0));
            expected_parents.insert(Parent::new(parent2_id, 1));
            assert_eq!(parents, expected_parents);

            let parents = db.parents(parent1_id).await?;
            assert_eq!(parents.len(), 1);

            let Parent {id, index} = parents.into_iter().next().unwrap();
            assert_eq!(id, parent2_id);
            assert_eq!(index, 0);

            assert_eq!(db.parents(parent2_id).await?.len(), 0);
        });
    }
}

test! {
    async fn add_parent_that_contains_same_child_multiple_times(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text1_and_3 = Node::text("foo");
        let text2 = Node::text("bar");

        let text1_and_3_id = tx!(|db| db.add(text1_and_3).await?);

        let chain = Node::List(Layout::Chain, vec![
            Child::Lazy(text1_and_3_id),
            Child::Eager(text2),
            Child::Lazy(text1_and_3_id),
        ]);

        let chain_id = tx!(|db| db.add(chain).await?);

        tx!(|db| {
            let chain = db.get(chain_id).await?.unwrap();
            let children = chain.children();
            assert_eq!(children.len(), 3);

            let text2_id = children[1].id()?;
            let parents = db.parents(text2_id).await?;
            assert_eq!(parents.len(), 1);

            let Parent {id, index} = parents.into_iter().next().unwrap();
            assert_eq!(id, chain_id);
            assert_eq!(index, 1);

            let parents = db.parents(text1_and_3_id).await?;
            assert_eq!(parents.len(), 2);

            let mut expected_parents = HashSet::new();
            expected_parents.insert(Parent::new(chain_id, 0));
            expected_parents.insert(Parent::new(chain_id, 2));
            assert_eq!(parents, expected_parents);
        });
    }
}

test! {
    async fn persist_nodes(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let text1 = Node::text("foo");
        let text2 = Node::text("bar");
        let chain = Node::list(Layout::Chain, vec![text1.clone(), text2.clone()]);

        let (text1_id, text2_id) = tx!(|db| {
            let chain_id = db.add(chain).await?;
            let chain = db.get(chain_id).await?.unwrap();
            let children = chain.children();
            (children[0].id()?, children[1].id()?)
        });

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| {
            assert_eq!(db.get(text1_id).await?.unwrap().str()?, text1.str()?);
            assert_eq!(db.get(text2_id).await?.unwrap().str()?, text2.str()?);
        });
    }
}

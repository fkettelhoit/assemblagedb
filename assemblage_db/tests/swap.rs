use assemblage_db::{
    data::{Child, Id, Layout, Node},
    tx, Db, Error, RestoredNode, Result,
};
use assemblage_kv::{storage, storage::Storage, test};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn persist_root(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let root = tx!(|db| db.get(Id::root()).await?.unwrap());
        assert_eq!(root.children().len(), 0);

        let text1 = Node::text("foo");
        let text1_id = tx!(|db| db.add(text1).await?);

        let replacement = Node::list(Layout::Page, vec![
            Node::list(Layout::Chain, vec![text1_id])
        ]);
        tx!(|db| db.swap(Id::root(), replacement).await?);

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| {
            let root = db.get(Id::root()).await?.unwrap();
            assert_eq!(root.children().len(), 1);

            let chain = root.children()[0].of(&db).await?;
            assert_eq!(chain.children().len(), 1);

            assert_eq!(chain.children()[0].id()?, text1_id);
            let text = chain.children()[0].of(&db).await?;
            assert_eq!(text.str()?, "foo");
        });
    }
}

test! {
    async fn push_to_chain(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let chain_id = tx!(|db| {
            let chain = Node::list(Layout::Chain, vec![Node::text("foo"), Node::text("bar")]);
            db.add(chain).await?
        });

        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 2));

        let qux_id = tx!(|db| {
            db.push(chain_id, Child::Eager(Node::text("baz"))).await?;
            let qux = Node::text("qux");
            let qux_id = db.add(qux).await?;
            db.push(chain_id, Child::Lazy(qux_id)).await?;
            qux_id
        });

        tx!(|db| {
            assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 4);
            assert_eq!(db.parents(qux_id).await?.len(), 1);
        });

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| db.push(chain_id, Child::Eager(Node::text("foobar"))).await?);
        tx!(|db| {
            assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 5);
            assert_eq!(db.get(chain_id).await?.unwrap().children().last().unwrap().of(&db).await?.str()?, "foobar");
        });
    }
}

test! {
    async fn push_lazy_child_to_root(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let text_node_id = tx!(|db| {
            let root_id = Id::root();
            let text_node = Node::text("foo");
            let text_node_id = db.add(text_node).await?;
            db.push(root_id, Child::Lazy(text_node_id)).await?;
            text_node_id
        });

        tx!(|db| {
            assert_eq!(db.parents(text_node_id).await?.len(), 1);
        })
    }
}

test! {
    async fn push_eager_child_to_root(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let root_id = tx!(|db| {
            let root_id = Id::root();
            db.push(root_id, Child::Eager(Node::text("foo"))).await?;
            root_id
        });

        tx!(|db| {
            assert_eq!(db.get(root_id).await?.unwrap().children().len(), 1);
            let child_id = db.get(root_id).await?.unwrap().children()[0].id()?;
            assert_eq!(db.parents(child_id).await?.len(), 1);
        })
    }
}

test! {
    async fn insert_into_chain(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let chain_id = tx!(|db| {
            let chain = Node::list(Layout::Chain, vec![Node::text("foo"), Node::text("qux")]);
            db.add(chain).await?
        });

        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 2));

        tx!(|db| {
            db.insert(chain_id, 1, Child::Eager(Node::text("bar"))).await?;
            db.insert(chain_id, 2, Child::Eager(Node::text("baz"))).await?;
        });

        tx!(|db| {
            assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 4);
            let mut texts = Vec::new();
            for child in db.get(chain_id).await?.unwrap().children() {
                let child = child.of(&db).await?;
                texts.push(String::from(child.str()?));
            }
            assert_eq!(texts, vec!["foo", "bar", "baz", "qux"]);
        });
    }
}

test! {
    async fn remove_from_chain(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (foo_id, chain_id) = tx!(|db| {
            let chain = Node::list(Layout::Chain, vec![
                Node::text("foo"),
                Node::text("bar")
            ]);
            let chain_id = db.add(chain).await?;
            let foo_id = db.get(chain_id).await?.unwrap().children()[0].id()?;
            (foo_id, chain_id)
        });

        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 2));

        tx!(|db| db.remove(chain_id, 0).await?);

        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 1));

        // foo remains accessible until we "empty the trash" by merging the DB
        tx!(|db| assert_eq!(db.get_in_trash(foo_id).await?.unwrap().str()?, "foo"));
        tx!(|db| assert_eq!(db.get(foo_id).await?, None));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| assert_eq!(db.get_in_trash(foo_id).await?, None));
        assert_not_found(&db, foo_id).await?;

        tx!(|db| {
            assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 1);
            let child = db.get(chain_id).await?.unwrap().children()[0].of(&db).await?;
            assert_eq!(child.str()?, "bar");
        });
    }
}

test! {
    async fn replace_in_chain(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (foo_id, chain_id) = tx!(|db| {
            let chain = Node::list(Layout::Chain, vec![
                Node::text("foo"),
                Node::text("bar")
            ]);
            let chain_id = db.add(chain).await?;
            let foo_id = db.get(chain_id).await?.unwrap().children()[0].id()?;
            (foo_id, chain_id)
        });

        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 2));

        tx!(|db| db.replace(chain_id, 0, Child::Eager(Node::text("baz"))).await?);

        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 2));

        // foo remains accessible until we "empty the trash" by merging the DB
        tx!(|db| assert_eq!(db.get_in_trash(foo_id).await?.unwrap().str()?, "foo"));
        tx!(|db| assert_eq!(db.get(foo_id).await?, None));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        assert_not_found(&db, foo_id).await?;

        tx!(|db| {
            assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 2);
            let mut texts = Vec::new();
            for child in db.get(chain_id).await?.unwrap().children() {
                let child = child.of(&db).await?;
                texts.push(String::from(child.str()?));
            }
            assert_eq!(texts, vec!["baz", "bar"]);
        });
    }
}

test! {
    async fn remove_orphaned_text_after_replace(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (text1_id, chain_id) = tx!(|db| {
            let chain = Node::list(Layout::Chain, vec![Node::text("foo"), Node::text("bar")]);
            let chain_id = db.add(chain).await?;
            let text1_id = db.get(chain_id).await?.unwrap().children()[0].id()?;
            (text1_id, chain_id)
        });

        tx!(|db| assert_eq!(db.get(text1_id).await?.unwrap().str()?, "foo"));

        tx!(|db| db.replace(chain_id, 0, Child::Eager(Node::text("baz"))).await?);

        tx!(|db| assert_eq!(db.get_in_trash(text1_id).await?.unwrap().str()?, "foo"));
        tx!(|db| assert_eq!(db.get(text1_id).await?, None));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        assert_not_found(&db, text1_id).await?;
    }
}

test! {
    async fn keep_child_if_other_parent_exists_after_replace(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (text1_id, chain_id) = tx!(|db| {
            let chain = Node::list(Layout::Chain, vec![Node::text("foo"), Node::text("bar")]);
            let chain_id = db.add(chain).await?;
            let text1_id = db.get(chain_id).await?.unwrap().children()[0].id()?;
            (text1_id, chain_id)
        });

        tx!(|db| {
            let other_parent = Node::list(Layout::Chain, vec![text1_id]);
            db.add(other_parent).await?;
        });

        tx!(|db| assert_eq!(db.get(text1_id).await?.unwrap().str()?, "foo"));
        tx!(|db| assert_eq!(db.parents(text1_id).await?.len(), 2));

        tx!(|db| db.replace(chain_id, 0, Child::Eager(Node::text("baz"))).await?);

        tx!(|db| assert_eq!(db.get(text1_id).await?.unwrap().str()?, "foo"));
        tx!(|db| assert_eq!(db.parents(text1_id).await?.len(), 1));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| assert_eq!(db.parents(text1_id).await?.len(), 1));
    }
}

test! {
    async fn keep_root_node(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let root_id = Id::root();
        let root = tx!(|db| db.get(root_id).await?.unwrap());
        assert_eq!(format!("{}", root_id), "00000000-0000-0000-0000-000000000000");
        assert_eq!(root.children().len(), 0);
        tx!(|db| assert_eq!(db.parents(root_id).await?.len(), 0));

        tx!(|db| db.push(root_id, Child::Lazy(root_id)).await?);

        let root = tx!(|db| db.get(root_id).await?.unwrap());
        assert_eq!(format!("{}", root_id), "00000000-0000-0000-0000-000000000000");
        assert_eq!(root.children().len(), 1);
        assert_eq!(root.children()[0].id()?, root_id);

        tx!(|db| db.replace(root_id, 0, Child::Eager(Node::text("foo"))).await?);

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        let root = tx!(|db| db.get(root_id).await?.unwrap());
        assert_eq!(format!("{}", root_id), "00000000-0000-0000-0000-000000000000");
        assert_eq!(root.children().len(), 1);

        assert_ne!(root.children()[0].id()?, root_id);
    }
}

test! {
    async fn remove_orphaned_text_that_occurs_multiple_times_as_a_child(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (f_id, o_id, foo_id) = tx!(|db| {
            let f_id = db.add(Node::text("f")).await?;
            let o_id = db.add(Node::text("o")).await?;
            let foo_id = db.add(Node::list(Layout::Chain, vec![f_id, o_id, o_id])).await?;
            (f_id, o_id, foo_id)
        });

        let chain_id = tx!(|db| {
            db.add(Node::List(Layout::Chain, vec![
                Child::Lazy(foo_id),
                Child::Eager(Node::text("bar"))
            ])).await?
        });

        tx!(|db| db.replace(chain_id, 0, Child::Eager(Node::text("foo"))).await?);

        tx!(|db| assert_eq!(db.get_in_trash(f_id).await?.unwrap().str()?, "f"));
        tx!(|db| assert_eq!(db.get_in_trash(o_id).await?.unwrap().str()?, "o"));
        tx!(|db| assert_eq!(db.get(f_id).await?, None));
        tx!(|db| assert_eq!(db.get(o_id).await?, None));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        assert_not_found(&db, f_id).await?;
        assert_not_found(&db, o_id).await?;
        assert_not_found(&db, foo_id).await?;
    }
}

test! {
    async fn remove_orphaned_child(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (foo_id, foo_chain_id, bar_id, foobar_id) = tx!(|db| {
            let foo_id = db.add(Node::text("foo")).await?;
            let foo_chain_id = db.add(Node::list(Layout::Chain, vec![foo_id])).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let foobar_id = db.add(Node::list(Layout::Chain, vec![foo_chain_id, bar_id])).await?;
            (foo_id, foo_chain_id, bar_id, foobar_id)
        });

        let foobarbaz_id = tx!(|db| {
            db.add(Node::List(Layout::Chain, vec![
                Child::Lazy(foobar_id),
                Child::Eager(Node::text("baz"))
            ])).await?
        });

        tx!(|db| {
            let foobar_text = Child::Eager(Node::text("foobar"));
            db.replace(foobarbaz_id, 0, foobar_text).await?
        });

        tx!(|db| assert_eq!(db.get_in_trash(foobar_id).await?.unwrap().children().len(), 2));
        tx!(|db| assert_eq!(db.get_in_trash(foo_chain_id).await?.unwrap().children().len(), 1));
        tx!(|db| assert_eq!(db.get_in_trash(foo_id).await?.unwrap().str()?, "foo"));
        tx!(|db| assert_eq!(db.get_in_trash(bar_id).await?.unwrap().str()?, "bar"));

        assert_not_found(&db, foobar_id).await?;
        assert_not_found(&db, foo_chain_id).await?;
        assert_not_found(&db, foo_id).await?;
        assert_not_found(&db, bar_id).await?;

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| assert_eq!(db.get_in_trash(foobar_id).await?, None));
        tx!(|db| assert_eq!(db.get_in_trash(foo_chain_id).await?, None));
        tx!(|db| assert_eq!(db.get_in_trash(foo_id).await?, None));
        tx!(|db| assert_eq!(db.get_in_trash(bar_id).await?, None));

        assert_not_found(&db, foobar_id).await?;
        assert_not_found(&db, foo_chain_id).await?;
        assert_not_found(&db, foo_id).await?;
        assert_not_found(&db, bar_id).await?;
    }
}

test! {
    async fn restore_orphaned_child(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (empty_chain_id, foo_id, bar_id, chain_id) = tx!(|db| {
            let empty_chain_id = db.add(Node::List(Layout::Chain, vec![])).await?;
            let foo_id = db.add(Node::text("foo")).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let chain_id = db.add(Node::list(Layout::Chain, vec![
                empty_chain_id,
                foo_id,
                bar_id,
            ])).await?;
            (empty_chain_id, foo_id, bar_id, chain_id)
        });

        tx!(|db| assert_eq!(db.parents(empty_chain_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));

        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 3));

        tx!(|db| {
            db.remove(chain_id, 1).await?;
            db.remove(chain_id, 0).await?;
        });

        tx!(|db| assert_eq!(db.get(chain_id).await?.unwrap().children().len(), 1));

        tx!(|db| assert!(db.parents(empty_chain_id).await.is_err()));
        tx!(|db| assert!(db.parents(foo_id).await.is_err()));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));

        tx!(|db| db.insert(chain_id, 0, Child::Lazy(foo_id)).await?);

        tx!(|db| assert!(db.parents(empty_chain_id).await.is_err()));
        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        assert_not_found(&db, empty_chain_id).await?;
        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));

        tx!(|db| assert_eq!(db.get(bar_id).await?.unwrap().str()?, "bar"));
        tx!(|db| assert_eq!(db.get(foo_id).await?.unwrap().str()?, "foo"));
    }
}

test! {
    async fn remove_orphaned_parent_of_link(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("foo")
            ])).await?
        });

        let (link_to_page1_text_id, parent_of_page1_id) = tx!(|db| {
            let link_to_page1_text_id = db.add(Node::text("Link to page 1:")).await?;

            let parent_of_page1_id = db.add(Node::List(Layout::Page, vec![
                Child::Lazy(link_to_page1_text_id),
                Child::Eager(Node::list(Layout::Chain, vec![page1_id])),
            ])).await?;

            (link_to_page1_text_id, parent_of_page1_id)
        });

        tx!(|db| {
            let page2 = Node::list(Layout::Page, vec![
                Node::text("bar")
            ]);
            let parent_of_page1_and_page2 = Node::list(Layout::Page, vec![
                Node::text("Link to page 1:"),
                Node::list(Layout::Chain, vec![page1_id]),
                Node::text("Link to page 2:"),
                Node::list(Layout::Chain, vec![page2]),
            ]);
            db.swap(parent_of_page1_id, parent_of_page1_and_page2).await?
        });

        tx!(|db| assert!(db.parents(link_to_page1_text_id).await.is_err()));
        tx!(|db| assert_eq!(db.parents(page1_id).await?.len(), 1));
    }
}

test! {
    async fn restore_removed_subtree(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let (subtree_id, foo_id, bar_id) = tx!(|db| {
            let foo_id = db.add(Node::text("foo")).await?;
            let bar_id = db.add(Node::text("bar")).await?;
            let subtree_id = db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Chain, vec![foo_id]),
                Node::list(Layout::Chain, vec![
                    Node::list(Layout::Chain, vec![bar_id]),
                ]),
            ])).await?;
            (subtree_id, foo_id, bar_id)
        });

        let parent_page_id = tx!(|db| db.add(Node::list(Layout::Page, vec![subtree_id])).await?);

        tx!(|db| assert_eq!(db.get(subtree_id).await?.unwrap().children().len(), 2));
        tx!(|db| assert_eq!(db.parents(subtree_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));

        tx!(|db| db.remove(parent_page_id, 0).await?);

        tx!(|db| assert_eq!(db.get_in_trash(subtree_id).await?.unwrap().children().len(), 2));
        tx!(|db| assert_eq!(db.get(subtree_id).await?, None));
        tx!(|db| assert!(db.parents(subtree_id).await.is_err()));
        tx!(|db| assert!(db.parents(foo_id).await.is_err()));
        tx!(|db| assert!(db.parents(bar_id).await.is_err()));

        let restored = tx!(|db| db.restore(subtree_id).await?);
        match restored {
            RestoredNode::Restored(_) => (),
            _ => panic!("Expected node to be restored!")
        };

        tx!(|db| assert_eq!(db.get(subtree_id).await?.unwrap().children().len(), 2));
        tx!(|db| assert_eq!(db.parents(subtree_id).await?.len(), 0));
        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        tx!(|db| assert_eq!(db.get(subtree_id).await?.unwrap().children().len(), 2));
        tx!(|db| assert_eq!(db.parents(subtree_id).await?.len(), 0));
        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(bar_id).await?.len(), 1));
    }
}

test! {
    async fn remove_one_branch_of_diamond_link_dependencies(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
        let (path1_id, path2_id, diamond_id) = tx!(|db| {
            let path1_id = db.add(Node::list(Layout::Chain, vec![foo_id])).await?;
            let path2_id = db.add(Node::list(Layout::Chain, vec![foo_id])).await?;
            let diamond_id = db.add(Node::list(Layout::Chain, vec![path1_id, path2_id])).await?;

            (path1_id, path2_id, diamond_id)
        });

        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 2));
        tx!(|db| assert_eq!(db.parents(path1_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(path2_id).await?.len(), 1));

        tx!(|db| db.remove(diamond_id, 1).await?);

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let mut db = Db::open(storage).await?;

        tx!(|db| assert_eq!(db.parents(foo_id).await?.len(), 1));
        tx!(|db| assert_eq!(db.parents(path1_id).await?.len(), 1));
        assert_not_found(&db, path2_id).await?;

        tx!(|db| db.remove(diamond_id, 0).await?);

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        assert_not_found(&db, foo_id).await?;
        assert_not_found(&db, path1_id).await?;
        assert_not_found(&db, path2_id).await?;
    }
}

test! {
    async fn remove_all_branches_of_diamond_link_dependencies(storage) -> Result<()> {
        let name = String::from(storage.name());
        let mut db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::text("foo")).await?);
        let (path1_id, path2_id, page_of_diamond_id) = tx!(|db| {
            let path1_id = db.add(Node::list(Layout::Chain, vec![foo_id])).await?;
            let path2_id = db.add(Node::list(Layout::Chain, vec![foo_id])).await?;
            let diamond_id = db.add(Node::list(Layout::Chain, vec![path1_id, path2_id])).await?;
            let page_of_diamond_id = db.add(Node::list(Layout::Page, vec![diamond_id])).await?;
            (path1_id, path2_id, page_of_diamond_id)
        });

        tx!(|db| db.remove(page_of_diamond_id, 0).await?);

        db.merge().await?;
        let storage = storage::open(&name).await?;
        let db = Db::open(storage).await?;

        assert_not_found(&db, foo_id).await?;
        assert_not_found(&db, path1_id).await?;
        assert_not_found(&db, path2_id).await?;
    }
}

test! {
    async fn move_descendant_in_subtree(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (descendant_id, doc_id) = tx!(|db| {
            let descendant_id = db.add(Node::text("descendant in subtree")).await?;
            let child1_id = db.add(Node::list(Layout::Page, vec![descendant_id])).await?;
            let child2_id = db.add(Node::list(Layout::Page, vec![Node::text("...")])).await?;
            let doc_id = db.add(Node::list(Layout::Page, vec![child1_id, child2_id])).await?;
            (descendant_id, doc_id)
        });

        tx!(|db| {
            db.swap(doc_id, Node::List(Layout::Page, vec![
                Child::Eager(Node::text("descendant should be moved in the subtree")),
                Child::Lazy(descendant_id),
            ])).await?;

            assert!(db.get(descendant_id).await?.is_some())
        });
    }
}

#[cfg(not(feature = "graft-db-tests"))]
async fn assert_not_found<S: Storage>(db: &Db<S>, id: Id) -> Result<()> {
    tx!(|db| assert_eq!(db.get(id).await?, None));
    tx!(|db| assert!(matches!(
        db.parents(id).await.unwrap_err(),
        Error::IdNotFound { id: not_found_id, .. } if not_found_id == id
    )));
    Ok(())
}

#[cfg(feature = "graft-db-tests")]
async fn assert_not_found<S: Storage>(db: &LockedGraftDb<S>, id: Id) -> Result<()> {
    tx!(|db| assert!(matches!(
        db.get(id).await.unwrap_err(),
        Error::NodeNotFound(not_found_id) if not_found_id == id
    )));
    tx!(|db| assert!(matches!(
        db.parents(id).await.unwrap_err(),
        Error::NodeNotFound(not_found_id) if not_found_id == id
    )));
    Ok(())
}

use assemblage_db::{
    data::{Layout, Node, Parent, SpanStyle},
    tx, Db,
};
use assemblage_kv::test;
use assemblage_view::{
    model::{Block, Branch, PreviewLink, Span},
    styles, DbView, Result,
};
use std::collections::HashSet;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn tile_with_shared_blocks(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let a_id = tx!(|db| db.add(Node::text("A")).await?);
        let b_id = tx!(|db| db.add(Node::text("B")).await?);
        let c_id = tx!(|db| db.add(Node::text("C")).await?);

        let shared_parent_of_b = tx!(|db| db.add(Node::list(Layout::Chain, vec![b_id])).await?);

        let ancestor1_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![shared_parent_of_b])).await?);
        let _ancestor2_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![shared_parent_of_b])).await?);

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![a_id, ancestor1_id, c_id])).await?
        });

        let snapshot = &db.current().await;
        let ancestors = snapshot.ancestor_path(b_id).await?;
        assert_eq!(ancestors.len(), 1);

        let t = db.current().await.tile(page1_id).await?;

        assert_eq!(t.sections.len(), 3);
        assert!(!t.sections[0].has_multiple_parents);
        assert!(t.sections[1].has_multiple_parents);
        assert!(!t.sections[2].has_multiple_parents);
    }
}

test! {
    async fn tile_with_link_as_first_block(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let a_id = tx!(|db| db.add(Node::text("A")).await?);
        let page_of_a_id = tx!(|db| db.add(Node::list(Layout::Page, vec![a_id])).await?);
        let link_of_a_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![page_of_a_id])).await?);

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![link_of_a_id])).await?
        });

        let t = db.current().await.tile(page1_id).await?;

        assert_eq!(t.sections.len(), 1);
        assert_eq!(t.sections[0].subsections.len(), 1);
        match &t.sections[0].subsections[0].block {
            Block::Text { styles: _, spans } => {
                assert_eq!(spans.len(), 1);
                match &spans[0] {
                    Span::Link { styles: _, link } => {
                        assert_eq!(link.descendant, PreviewLink {
                            id: page_of_a_id,
                            block: Block::text(vec![Span::text("A")]),
                        });
                    }
                    _ => panic!("Expected link span, found: {:?}", spans[0]),
                };
            },
            b => panic!("Unexpected block: {:?}", b)
        }
    }
}

test! {
    async fn tile_with_forks(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let a_id = tx!(|db| db.add(Node::text("A")).await?);
        let b_id = tx!(|db| db.add(Node::text("B")).await?);
        let c_id = tx!(|db| db.add(Node::text("C")).await?);

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![a_id, b_id, c_id])).await?
        });

        let x_id = tx!(|db| db.add(Node::text("X")).await?);

        let page2_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![a_id, b_id, x_id])).await?
        });

        tx!(|db| -> Result<_, Error> {
            let tile = db.tile(page1_id).await?;
            assert_eq!(tile.sections.len(), 3);

            for i in 0..2 {
                let section = &tile.sections[i];
                assert_eq!(section.subsections.len(), 1);

                let block = &section.subsections[0];
                assert_eq!(block.before.len(), 0);
                if i == 1 {
                    assert_eq!(block.after.len(), 1);
                    match &block.after[0] {
                        Branch::Sibling { link, .. } => {
                            assert_eq!(link.descendant.id, page2_id);
                        }
                    };
                    let parents_of_b = db.parents(block.id).await?;
                    assert_eq!(parents_of_b.len(), 2);
                    let mut expected_parents = HashSet::new();
                    expected_parents.insert(Parent::new(page1_id, 1));
                    expected_parents.insert(Parent::new(page2_id, 1));
                    assert_eq!(parents_of_b, expected_parents);
                } else {
                    assert_eq!(block.after.len(), 0);
                }
            }
        })
    }
}

test! {
    async fn tile_with_multiple_same_children(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let a_id = tx!(|db| db.add(Node::text("A")).await?);
        let b_id = tx!(|db| db.add(Node::text("B")).await?);
        let c_id = tx!(|db| db.add(Node::text("C")).await?);

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![a_id, b_id, a_id, c_id])).await?
        });

        tx!(|db| {
            db.add(Node::list(Layout::Page, vec![a_id, b_id, a_id, c_id])).await?
        });

        tx!(|db| -> Result<_, Error> {
            let tile = db.tile(page1_id).await?;
            assert_eq!(tile.sections.len(), 4);

            for i in 0..3 {
                let section = &tile.sections[i];
                assert_eq!(section.subsections.len(), 1);

                let block = &section.subsections[0];
                assert_eq!(block.before.len(), 0);
                assert_eq!(block.after.len(), 0);
            }
        });
    }
}

test! {
    async fn tile_with_skipped_blank_siblings_as_before_branch(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let a_id = tx!(|db| db.add(Node::text("A")).await?);
        let b_id = tx!(|db| db.add(Node::text("B")).await?);
        let c_id = tx!(|db| db.add(Node::text("C")).await?);

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![a_id, b_id, c_id])).await?
        });

        let blank1_id = tx!(|db| db.add(Node::List(Layout::Chain, vec![])).await?);
        let blank2_id = tx!(|db| db.add(Node::text("   ")).await?);
        let x_id = tx!(|db| db.add(Node::text("X")).await?);

        let page2_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![x_id, blank1_id, blank2_id, a_id, b_id])).await?
        });

        tx!(|db| -> Result<_, Error> {
            let tile = db.tile(page1_id).await?;
            assert_eq!(tile.sections.len(), 3);

            for i in 0..2 {
                let section = &tile.sections[i];
                assert_eq!(section.subsections.len(), 1);

                let block = &section.subsections[0];
                assert_eq!(block.after.len(), 0);
                if i == 0 {
                    assert_eq!(block.before.len(), 1);
                    match &block.before[0] {
                        Branch::Sibling { link, .. } => {
                            assert_eq!(link.descendant.id, page2_id);
                        }
                    };
                    let parents_of_a = db.parents(block.id).await?;
                    assert_eq!(parents_of_a.len(), 2);
                    let mut expected_parents = HashSet::new();
                    expected_parents.insert(Parent::new(page1_id, 0));
                    expected_parents.insert(Parent::new(page2_id, 3));
                    assert_eq!(parents_of_a, expected_parents);
                } else {
                    assert_eq!(block.before.len(), 0);
                }
            }
        })
    }
}

test! {
    async fn tile_with_links_as_branches(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        for layout in vec![Layout::Chain, Layout::Page].into_iter() {
            let a_id = tx!(|db| db.add(Node::text("A")).await?);
            let b_id = tx!(|db| db.add(Node::text("B")).await?);
            let c_id = tx!(|db| db.add(Node::text("C")).await?);
            let x_id = tx!(|db| db.add(Node::text("X")).await?);
            let x_as_block_id = tx!(|db| db.add(Node::list(Layout::Page, vec![x_id])).await?);
            let x_wrapped_id = tx!(|db| db.add(Node::list(layout, vec![x_as_block_id])).await?);

            let page1_id = tx!(|db| {
                db.add(Node::list(Layout::Page, vec![a_id, b_id, c_id, x_wrapped_id])).await?
            });

            tx!(|db| {
                db.add(Node::list(Layout::Page, vec![x_id, b_id, c_id])).await?
            });

            tx!(|db| -> Result<_, Error> {
                let tile = db.tile(page1_id).await?;
                assert_eq!(tile.sections.len(), 4);

                for i in 0..3 {
                    let section = &tile.sections[i];
                    assert_eq!(section.subsections.len(), 1);

                    let block = &section.subsections[0];
                    assert_eq!(block.after.len(), 0);

                    if i == 1 && layout == Layout::Chain {
                        assert_eq!(block.before.len(), 1);
                        match &block.before[0] {
                            Branch::Sibling { link, .. } => {
                                assert_eq!(link.descendant.id, x_id);
                            }
                        };
                    } else {
                        assert_eq!(block.before.len(), 0);
                    }
                }
            });
        }
    }
}

test! {
    async fn tile_with_chain_sibling(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let foo_id = tx!(|db| db.add(Node::styled(SpanStyle::Bold, Node::text("foo"))).await?);
        let bar_id = tx!(|db| db.add(Node::text("bar")).await?);

        let chain_id = tx!(|db| {
            db.add(Node::list(Layout::Chain, vec![foo_id, bar_id])).await?
        });

        let shared_text_id = tx!(|db| db.add(Node::text("shared")).await?);

        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![shared_text_id])).await?
        });

        let page2_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![chain_id, shared_text_id])).await?
        });

        let unrelated_text_id = tx!(|db| db.add(Node::text("unrelated")).await?);

        // The sibling search should walk up only until a block is found and no
        // further. This is why the following page should not be the branch,
        // even though it is a unique ancestor of the chain that contains foo
        // and bar.
        let page_containing_page2_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![unrelated_text_id, page2_id])).await?
        });

        let current = db.current().await;
        let t = current.tile(page1_id).await?;

        assert_eq!(t.sections.len(), 1);
        assert_eq!(t.sections[0].subsections.len(), 1);
        assert_eq!(t.sections[0].subsections[0].before.len(), 1);

        let before = &t.sections[0].subsections[0].before[0];

        match before {
            Branch::Sibling { link, .. } => {
                assert_ne!(link.descendant.id, page_containing_page2_id);
                assert_eq!(link.descendant.id, page2_id);
                assert_eq!(
                    link.descendant.block,
                    Block::text(vec![
                        Span::Text {
                            styles: styles![SpanStyle::Bold],
                            text: "foo".to_string(),
                        },
                        Span::text("bar"),
                    ])
                );
            }
        }
    }
}

test! {
    async fn tile_with_parent_branches(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("some text")
            ])).await?
        });

        let parent_of_page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("Parent page"),
                Node::list(Layout::Chain, vec![page_id])
            ])).await?
        });

        let current = db.current().await;
        let t = current.tile(page_id).await?;

        assert_eq!(t.branches.len(), 1);
        match t.branches.first().unwrap() {
            Branch::Sibling { link, .. } => {
                assert_eq!(link.ancestor.as_ref().unwrap().id, parent_of_page_id);
            }
        }
    }
}

test! {
    async fn tile_with_parent_branches_up_until_link(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("some text"),
            ])).await?
        });

        let parent_of_page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("Parent page"),
                Node::list(Layout::Chain, vec![page_id]),
            ])).await?
        });

        let _parent_of_parent_of_page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("Parent of parent page"),
                Node::list(Layout::Chain, vec![parent_of_page_id]),
            ])).await?
        });

        let current = db.current().await;
        let t = current.tile(page_id).await?;

        assert_eq!(t.branches.len(), 1);
        match t.branches.first().unwrap() {
            Branch::Sibling { link, .. } => {
                assert_eq!(link.ancestor.as_ref().unwrap().id, parent_of_page_id);
            }
        }
    }
}

test! {
    async fn tile_with_ancestor_branches_up_until_link(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("some text"),
            ])).await?
        });

        let parent1_of_page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                page_id,
            ])).await?
        });

        let parent2_of_page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("Parent page"),
                Node::list(Layout::Page, vec![page_id]),
            ])).await?
        });

        let _parent_of_parent_of_page_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::text("Parent of parent page"),
                Node::list(Layout::Chain, vec![parent2_of_page_id]),
            ])).await?
        });

        let current = db.current().await;
        let t = current.tile(parent1_of_page_id).await?;

        assert_eq!(t.sections.len(), 1);
        assert_eq!(t.sections[0].subsections.len(), 1);
        assert_eq!(t.sections[0].subsections[0].before.len(), 1);

        let before = &t.sections[0].subsections[0].before[0];

        match before {
            Branch::Sibling { link, .. } => {
                assert_eq!(link.ancestor, None);
                assert_eq!(link.descendant.id, parent2_of_page_id);
            }
        }
    }
}

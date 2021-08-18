use assemblage_db::{
    data::{Layout, Node, SpanStyle},
    tx, Db,
};
use assemblage_kv::test;
use assemblage_view::{
    model::{Block, Section, Span, Subsection},
    styles, DbView, Result,
};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn sections_of_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        let node_id = tx!(|db| db.add(Node::text("a single line of text")).await?);

        let sections = db.current().await.sections(node_id, true).await?;

        let expected_section = Section {
            id: None,
            subsections: vec![
                Subsection {
                    id: node_id,
                    block: Block::text(vec![
                        Span::text("a single line of text")
                    ]),
                    before: Vec::new(),
                    after: Vec::new(),
                }
            ],
            has_multiple_parents: false,
        };
        assert_eq!(sections, vec![expected_section]);
    }
}

test! {
    async fn sections_of_list(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        for layout in &[Layout::Chain, Layout::Page] {

            let (line1_id, line2_id, node_id) = tx!(|db| {
                let line1_id = db.add(Node::text("this line")).await?;
                let line2_id = db.add(Node::text("continues here")).await?;
                let node_id = db.add(Node::list(*layout, vec![line1_id, line2_id])).await?;
                (line1_id, line2_id, node_id)
            });

            let sections = db.current().await.sections(node_id, true).await?;

            let expected = vec![
                Section {
                    id: None,
                    subsections: vec![
                        Subsection {
                            id: line1_id,
                            block: Block::text(vec![
                                Span::text("this line")
                            ]),
                            before: Vec::new(),
                            after: Vec::new(),
                        }
                    ],
                    has_multiple_parents: false,
                },
                Section {
                    id: None,
                    subsections: vec![
                        Subsection {
                            id: line2_id,
                            block: Block::text(vec![
                                Span::text("continues here")
                            ]),
                            before: Vec::new(),
                            after: Vec::new(),
                        }
                    ],
                    has_multiple_parents: false,
                },
            ];
            assert_eq!(sections, expected);
        }
    }
}

test! {
    async fn sections_of_list_of_chains_of_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        for layout in &[Layout::Chain, Layout::Page] {
            let (chain_of_text_id, node_id) = tx!(|db| {
                let chain_id = db.add(Node::list(Layout::Chain, vec![
                    Node::text("this line"),
                    Node::text("continues here"),
                ])).await?;
                let node_id = db.add(Node::list(*layout, vec![chain_id])).await?;
                (chain_id, node_id)
            });

            let sections = db.current().await.sections(node_id, true).await?;

            let expected_section = Section {
                id: None,
                subsections: vec![
                    Subsection {
                        id: chain_of_text_id,
                        block: Block::text(vec![
                            Span::text("this line"),
                            Span::text("continues here"),
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    }
                ],
                has_multiple_parents: false,
            };
            assert_eq!(sections, vec![expected_section]);
        }
    }
}

test! {
    async fn sections_of_list_of_pages_of_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        for layout in &[Layout::Chain, Layout::Page] {
            let (line1_id, line2_id, node_id) = tx!(|db| {
                let line1_id = db.add(Node::text("this line")).await?;
                let line2_id = db.add(Node::text("continues here")).await?;
                let page_id = db.add(Node::list(Layout::Page, vec![line1_id, line2_id])).await?;
                let node_id = db.add(Node::list(*layout, vec![page_id])).await?;
                (line1_id, line2_id, node_id)
            });

            let sections = db.current().await.sections(node_id, true).await?;

            let expected = vec![
                Section {
                    id: None,
                    subsections: vec![
                        Subsection {
                            id: line1_id,
                            block: Block::text(vec![
                                Span::text("this line")
                            ]),
                            before: Vec::new(),
                            after: Vec::new(),
                        },
                    ],
                    has_multiple_parents: false,
                },
                Section {
                    id: None,
                    subsections: vec![
                        Subsection {
                            id: line2_id,
                            block: Block::text(vec![
                                Span::text("continues here")
                            ]),
                            before: Vec::new(),
                            after: Vec::new(),
                        },
                    ],
                    has_multiple_parents: false,
                },
            ];
            assert_eq!(sections, expected);
        }
    }
}

test! {
    async fn sections_of_styled_pages_of_text(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        let (line1_id, line2_id, node_id) = tx!(|db| {
            let line1_id = db.add(Node::text("this line")).await?;
            let line2_id = db.add(Node::text("continues here")).await?;
            let page_id = db.add(Node::list(Layout::Page, vec![line1_id, line2_id])).await?;
            let node_id = db.add(Node::styled(SpanStyle::Italic, page_id)).await?;
            (line1_id, line2_id, node_id)
        });

        let sections = db.current().await.sections(node_id, true).await?;

        let expected = vec![
            Section {
                id: None,
                subsections: vec![
                    Subsection {
                        id: line1_id,
                        block: Block::text(vec![
                            Span::Text {
                                styles: styles![SpanStyle::Italic],
                                text: "this line".to_string(),
                            }
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: false,
            },
            Section {
                id: None,
                subsections: vec![
                    Subsection {
                        id: line2_id,
                        block: Block::text(vec![
                            Span::Text {
                                styles: styles![SpanStyle::Italic],
                                text: "continues here".to_string(),
                            }
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: false,
            },
        ];
        assert_eq!(sections, expected);
    }
}

test! {
    async fn sections_of_children_with_multiple_parents(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (block1_id, block2_id, block3_id, block4_id, page1_id) = tx!(|db| {
            let block1_id = db.add(Node::text("This is paragraph 1.")).await?;
            let block2_id = db.add(Node::text("This is paragraph 2.")).await?;
            let block3_id = db.add(Node::text("This is paragraph 3.")).await?;
            let text1_id = db.add(Node::text("This paragraph is ")).await?;
            let text2_id = db.add(Node::text("split")).await?;
            let text3_id = db.add(Node::text(" into multiple text nodes.")).await?;
            let block4_id = db.add(Node::list(Layout::Chain, vec![text1_id, text2_id, text3_id])).await?;

            let page1_id = db.add(Node::list(Layout::Page, vec![block1_id, block2_id, block3_id, block4_id])).await?;

            let _other_parent1_id = db.add(Node::list(Layout::Page, vec![block2_id, block3_id])).await?;
            let _other_parent2_id = db.add(Node::list(Layout::Page, vec![text2_id])).await?;

            (block1_id, block2_id, block3_id, block4_id, page1_id)
        });

        let sections = db.current().await.sections(page1_id, true).await?;

        let expected = vec![
            Section {
                id: None,
                subsections: vec![
                    Subsection {
                        id: block1_id,
                        block: Block::text(vec![
                            Span::text("This is paragraph 1.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: false,
            },
            Section {
                id: Some(block2_id),
                subsections: vec![
                    Subsection {
                        id: block2_id,
                        block: Block::text(vec![
                            Span::text("This is paragraph 2.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: true,
            },
            Section {
                id: Some(block3_id),
                subsections: vec![
                    Subsection {
                        id: block3_id,
                        block: Block::text(vec![
                            Span::text("This is paragraph 3.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: true,
            },
            Section {
                id: Some(block4_id),
                subsections: vec![
                    Subsection {
                        id: block4_id,
                        block: Block::text(vec![
                            Span::text("This paragraph is "),
                            Span::text("split"),
                            Span::text(" into multiple text nodes.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: true,
            },
        ];
        assert_eq!(sections.len(), expected.len());
        for (section, expected) in sections.into_iter().zip(expected.into_iter()) {
            assert_eq!(section, expected);
        }
    }
}

test! {
    async fn sections_of_descendants_with_multiple_parents(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let (block1_id, block2_id, block3_id, subpage1_id, page1_id) = tx!(|db| {
            let block1_id = db.add(Node::text("This is paragraph 1.")).await?;
            let block2_id = db.add(Node::text("This is paragraph 2.")).await?;
            let block3_id = db.add(Node::text("This is paragraph 3.")).await?;

            let subpage1_id = db.add(Node::list(Layout::Page, vec![block2_id, block3_id])).await?;
            let page1_id = db.add(Node::list(Layout::Page, vec![block1_id, subpage1_id])).await?;

            let _other_parent1_id = db.add(Node::list(Layout::Page, vec![subpage1_id])).await?;

            (block1_id, block2_id, block3_id, subpage1_id, page1_id)
        });

        let sections = db.current().await.sections(page1_id, true).await?;

        let expected = vec![
            Section {
                id: None,
                subsections: vec![
                    Subsection {
                        id: block1_id,
                        block: Block::text(vec![
                            Span::text("This is paragraph 1.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: false,
            },
            Section {
                id: Some(subpage1_id),
                subsections: vec![
                    Subsection {
                        id: block2_id,
                        block: Block::text(vec![
                            Span::text("This is paragraph 2.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                    Subsection {
                        id: block3_id,
                        block: Block::text(vec![
                            Span::text("This is paragraph 3.")
                        ]),
                        before: Vec::new(),
                        after: Vec::new(),
                    },
                ],
                has_multiple_parents: true,
            },
        ];
        assert_eq!(sections, expected);
    }
}

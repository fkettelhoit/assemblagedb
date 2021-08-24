#![cfg(feature = "assemblage-broadcast-integration-tests")]
use assemblage_db::{
    broadcast::Broadcast,
    data::{Id, Layout, Node},
    tx, Db,
};
use assemblage_kv::{storage::MemoryStorage, test};
use assemblage_view::{
    model::{Block, Section, Span, Subsection, Tile},
    DbView, Result,
};
use std::{collections::BTreeSet, iter::FromIterator};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn broadcast_tile(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        let root_id = Id::root();
        let (id1, id2, broadcast, last_updated) = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, id1).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

            let text2 = Node::text("foobar");
            let id2 = tx!(|db| db.add(text2).await?);
            tx!(|db| db.push(root_id, id2).await?);
            tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "foobar"));

            let last_updated = tx!(|db| db.last_updated().await?.unwrap());
            let broadcast = tx!(|db| db.publish_broadcast(root_id).await?);

            (id1, id2, broadcast, last_updated)
        };

        assert!(broadcast.expiration.unwrap_or_default() >= last_updated + 60 * 60 * 24);

        let current = db.current().await;
        let tile = current.tile(root_id).await?;

        let expected = Tile {
            id: root_id,
            preview: Block::Text {
                styles: BTreeSet::new(),
                spans: vec![
                    Span::Text {
                        styles: BTreeSet::new(),
                        text: "foo".to_string(),
                    }
                ]
            },
            broadcasts: BTreeSet::from_iter(vec![
                Broadcast {
                    broadcast_id: broadcast.broadcast_id.clone(),
                    node_id: root_id,
                    last_updated,
                    expiration: broadcast.expiration,
                }
            ]),
            sections: vec![
                Section {
                    id: None,
                    has_multiple_parents: false,
                    subsections: vec![
                        Subsection {
                            id: id1,
                            block: Block::Text {
                                styles: BTreeSet::new(),
                                spans: vec![
                                    Span::Text {
                                        styles: BTreeSet::new(),
                                        text: "foo".to_string(),
                                    }
                                ]
                            },
                            before: vec![],
                            after: vec![],
                        }
                    ]
                },
                Section {
                    id: None,
                    has_multiple_parents: false,
                    subsections: vec![
                        Subsection {
                            id: id2,
                            block: Block::Text {
                                styles: BTreeSet::new(),
                                spans: vec![
                                    Span::Text {
                                        styles: BTreeSet::new(),
                                        text: "foobar".to_string(),
                                    }
                                ]
                            },
                            before: vec![],
                            after: vec![],
                        }
                    ]
                }
            ],
            branches: vec![],
        };

        assert_eq!(tile, expected);

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;

        let mut current = other.current().await;
        let other_tile = current.tile_from_broadcast(&broadcast.broadcast_id).await?;
        let namespaced = current.namespaced_id(&broadcast.broadcast_id.into(), root_id).await?;
        assert_eq!(other_tile.id, namespaced);
        assert_eq!(other_tile.preview, expected.preview);
        assert_eq!(other_tile.broadcasts, BTreeSet::new());
        assert_eq!(other_tile.branches, expected.branches);
        assert_eq!(other_tile.sections.len(), expected.sections.len());
        for (mut found, mut expected) in other_tile.sections.into_iter().zip(expected.sections) {
            assert_eq!(found.id, None);
            assert_eq!(expected.id, None);
            for (found, expected) in found.subsections.iter_mut().zip(expected.subsections.iter_mut()) {
                let found_id = std::mem::replace(&mut found.id, Id::root());
                let expected_id = std::mem::replace(&mut expected.id, Id::root());
                let namespaced = current.namespaced_id(&broadcast.broadcast_id.into(), expected_id).await?;
                assert_eq!(found_id, namespaced);
            }
            assert_eq!(found, expected);
        }
    }
}

test! {
    async fn multi_broadcast_tile(storage) -> Result<()> {
        let db = Db::open(storage).await?;
        let root_id = Id::root();
        let (id2, broadcast1, last_updated1, broadcast2, last_updated2) = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, id1).await?);

            let text2 = Node::text("foobar");
            let id2 = tx!(|db| db.add(Node::list(Layout::Page, vec![text2])).await?);
            tx!(|db| db.push(root_id, id2).await?);

            let last_updated1 = tx!(|db| db.last_updated().await?.unwrap());
            let broadcast1 = tx!(|db| db.publish_broadcast(root_id).await?);

            let last_updated2 = tx!(|db| db.last_updated().await?.unwrap());
            let broadcast2 = tx!(|db| db.publish_broadcast(id2).await?);

            (id2, broadcast1, last_updated1, broadcast2, last_updated2)
        };

        let current = db.current().await;
        let tile = current.tile(root_id).await?;

        assert_eq!(tile.broadcasts, BTreeSet::from_iter(vec![
            Broadcast {
                broadcast_id: broadcast1.broadcast_id.clone(),
                node_id: root_id,
                last_updated: last_updated1,
                expiration: broadcast1.expiration,
            }
        ]));

        let tile = current.tile(id2).await?;

        assert_eq!(tile.broadcasts, BTreeSet::from_iter(vec![
            Broadcast {
                broadcast_id: broadcast2.broadcast_id.clone(),
                node_id: id2,
                last_updated: last_updated2,
                expiration: broadcast2.expiration,
            },
            Broadcast {
                broadcast_id: broadcast1.broadcast_id.clone(),
                node_id: root_id,
                last_updated: last_updated1,
                expiration: broadcast1.expiration,
            }
        ]));

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;
        let mut current = other.current().await;
        let broadcast1_id = broadcast1.broadcast_id;
        let broadcast2_id = broadcast2.broadcast_id;

        let other_tile = current.tile_from_broadcast(&broadcast1_id).await?;
        let namespaced_root_id1 = current.namespaced_id(&broadcast1_id, root_id).await?;
        assert_eq!(other_tile.id, namespaced_root_id1);
        assert_eq!(other_tile.broadcasts, BTreeSet::new());

        let other_tile = current.tile_from_broadcast(&broadcast2_id).await?;
        let namespaced_root_id2 = current.namespaced_id(&broadcast2_id, root_id).await?;
        assert_eq!(other_tile.id, namespaced_root_id2);
        assert_eq!(other_tile.broadcasts, BTreeSet::new());
        assert_eq!(other_tile.branches, vec![]);
        assert_eq!(other_tile.sections.len(), 1);
        assert_eq!(other_tile.sections[0].subsections.len(), 1);
        let other_tile_root_sections = other_tile.sections;
        let namespaced = current.namespaced_id(&broadcast2_id, id2).await?;
        let other_tile = current.tile(namespaced).await?;
        assert_eq!(other_tile.id, namespaced);
        assert_eq!(other_tile.broadcasts, BTreeSet::new());
        assert_eq!(other_tile.branches, vec![]);
        assert_eq!(other_tile.sections.len(), 1);
        assert_eq!(other_tile.sections[0].subsections.len(), 1);
        assert_eq!(
            other_tile_root_sections[0].subsections[0],
            other_tile.sections[0].subsections[0]
        );
    }
}

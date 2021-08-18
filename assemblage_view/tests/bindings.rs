#![cfg(feature = "assemblage-broadcast-integration-tests")]
use std::{collections::BTreeSet, iter::FromIterator};

use assemblage_db::{
    broadcast::Broadcast,
    data::{Id, Layout, Node},
    tx, Db,
};
use assemblage_kv::{
    storage::{self, Storage},
    test,
};
use assemblage_view::{
    model::{Block, Span, Tile},
    bindings::{open, DbContainer, RefreshError, SyncError, SyncedSection, SyncedSubsection},
};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn sync_refresh_broadcast_tile(storage) -> Result<(), RefreshError> {
        let db = Db::open(storage).await?;
        let root_id = Id::root();
        let (_id1, _id2, broadcast) = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, id1).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

            let text2 = Node::text("foobar");
            let id2 = tx!(|db| db.add(text2).await?);
            tx!(|db| db.push(root_id, id2).await?);
            tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "foobar"));

            let broadcast = tx!(|db| db.publish_broadcast(root_id).await?);

            (id1, id2, broadcast)
        };

        let store_name = "store_in_container".to_string();
        let db = open(store_name.clone()).await.expect("Could not open DB container");

        let tile = refresh(&db, format!("broadcast:{}", broadcast.broadcast_id))
            .await
            .expect("Could not refresh tile");

        let expected_preview = Block::Text {
            styles: BTreeSet::new(),
            spans: vec![
                Span::Text {
                    styles: BTreeSet::new(),
                    text: "foo".to_string(),
                }
            ]
        };
        assert_eq!(tile.preview, expected_preview);

        storage::purge(store_name).await?;
        storage::purge(broadcast.broadcast_id.to_string()).await?;
    }
}

test! {
    async fn sync_tile_with_broadcast(storage) -> Result<(), SyncError> {
        let store_name = storage.name().to_string();
        let db = Db::open(storage).await?;
        let root_id = Id::root();
        let (_id1, id2, broadcast, last_updated) = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, id1).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

            let text2 = Node::text("foo");
            let text3 = Node::text("bar");
            let id2 = tx!(|db| db.add(Node::list(Layout::Page, vec![text2, text3])).await?);
            tx!(|db| db.push(root_id, id2).await?);

            let last_updated = tx!(|db| db.last_updated().await?.unwrap());
            let broadcast = tx!(|db| db.publish_broadcast(root_id).await?);

            (id1, id2, broadcast, last_updated)
        };

        let db = open(store_name).await.expect("Could not open DB container");
        let tile = sync(&db, Some(Id::root().to_string()), vec![
            SyncedSection::Edited {
                blocks: vec![
                    SyncedSubsection::Text {
                        markup: "baz".to_string()
                    }
                ]
            },
            SyncedSection::Existing { id: id2 },
        ]).await.expect("Could not sync edited sections");

        assert_eq!(tile.id, root_id);

        let expected_preview = Block::Text {
            styles: BTreeSet::new(),
            spans: vec![
                Span::Text {
                    styles: BTreeSet::new(),
                    text: "baz".to_string(),
                }
            ]
        };
        assert_eq!(tile.preview, expected_preview);

        let expected_broadcast = Broadcast {
            broadcast_id: broadcast.broadcast_id.clone(),
            node_id: root_id,
            last_updated,
            expiration: broadcast.expiration,
        };
        assert_eq!(tile.broadcasts, BTreeSet::from_iter(vec![expected_broadcast]));

        assert_eq!(tile.sections[0].id, None);
        assert_eq!(tile.sections[0].subsections.len(), 1);
        let section1_id = tile.sections[0].subsections[0].id;
        let tile = sync(&db, Some(root_id.to_string()), vec![
            SyncedSection::Existing { id: id2 },
            SyncedSection::Existing { id: section1_id },
        ]).await.expect("Could not sync moved existing sections");

        let expected_preview = Block::Text {
            styles: BTreeSet::new(),
            spans: vec![
                Span::Text {
                    styles: BTreeSet::new(),
                    text: "foo".to_string(),
                }
            ]
        };
        assert_eq!(tile.preview, expected_preview);

        let subsection2_id = tile.sections[1].subsections[0].id;
        let tile = sync(&db, Some(root_id.to_string()), vec![
            SyncedSection::Linked { id: subsection2_id },
            SyncedSection::Existing { id: id2 },
        ]).await.expect("Could not sync linked sections");

        let expected_preview = Block::Text {
            styles: BTreeSet::new(),
            spans: vec![
                Span::Text {
                    styles: BTreeSet::new(),
                    text: "bar".to_string(),
                }
            ]
        };
        assert_eq!(tile.preview, expected_preview);
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn refresh(db: &DbContainer, id: String) -> Result<Tile, RefreshError> {
    db.refresh(id).await
}

#[cfg(target_arch = "wasm32")]
async fn refresh(db: &DbContainer, id: String) -> Result<Tile, JsValue> {
    let promise = db.refresh(id);
    let result = wasm_bindgen_futures::JsFuture::from(promise).await?;
    let tile: Tile = result.into_serde().expect("Invalid result tile");
    Ok(tile)
}

#[cfg(not(target_arch = "wasm32"))]
async fn sync(
    db: &DbContainer,
    id: Option<String>,
    sections: Vec<SyncedSection>,
) -> Result<Tile, SyncError> {
    db.sync(id, sections).await
}

#[cfg(target_arch = "wasm32")]
async fn sync(
    db: &DbContainer,
    id: Option<String>,
    sections: Vec<SyncedSection>,
) -> Result<Tile, JsValue> {
    let sections_as_js = JsValue::from_serde(&sections).expect("Invalid synced sections");
    let promise = db.sync(id, sections_as_js);
    let result = wasm_bindgen_futures::JsFuture::from(promise).await?;
    let tile: Tile = result.into_serde().expect("Invalid result tile");
    Ok(tile)
}

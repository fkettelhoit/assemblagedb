#![cfg(feature = "assemblage-broadcast-integration-tests")]
use assemblage_db::{
    data::{BlockStyle, Child, Id, Layout, Node, SpanStyle},
    tx, Db, Result,
};
use assemblage_kv::{
    storage::{self, MemoryStorage},
    test,
};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const MILLIS_TO_WAIT_FOR_CHANGES_TO_PROPAGATE: u64 = 60000;

test! {
    async fn broadcast_and_subscribe_to_broadcast(storage) -> Result<()> {
        let (id1, id2, last_updated, broadcast) = {
            let db = Db::open(storage).await?;

            let root_id = Id::root();

            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id1)).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

            let text2 = Node::text("foobar");
            let id2 = tx!(|db| db.add(text2).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id2)).await?);
            tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "foobar"));

            let mut current = db.current().await;
            let last_updated = current.last_updated().await?.unwrap_or_default();
            let broadcast = current.publish_broadcast(root_id).await?;

            (id1, id2, last_updated, broadcast)
        };

        let broadcast_id = broadcast.broadcast_id;
        assert!(broadcast.expiration.unwrap_or_default() >= last_updated + 60 * 60 * 24);

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;

        let mut current = other.current().await;
        current.fetch_broadcast(&broadcast_id).await?;

        let namespaced_id1 = current.namespaced_id(&broadcast_id, id1).await?;
        let namespaced_id2 = current.namespaced_id(&broadcast_id, id2).await?;

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "foo");
        assert_eq!(current.get(namespaced_id2).await?.unwrap().str()?, "foobar");
    }
}

test! {
    async fn broadcast_and_subscribe_to_updates(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let root_id = Id::root();

        let (id1, id2, broadcast_id) = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id1)).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

            let text2 = Node::text("foobar");
            let id2 = tx!(|db| db.add(text2).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id2)).await?);
            tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "foobar"));

            let broadcast_id = tx!(|db| db.publish_broadcast(root_id).await?).broadcast_id;

            (id1, id2, broadcast_id)
        };

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;
        let mut current = other.current().await;
        let bytes_received = current.subscribe_to_broadcast(&broadcast_id).await?;
        assert!(bytes_received > 0);
        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);

        let namespaced_id1 = current.namespaced_id(&broadcast_id, id1).await?;
        let namespaced_id2 = current.namespaced_id(&broadcast_id, id2).await?;

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "foo");
        assert_eq!(current.get(namespaced_id2).await?.unwrap().str()?, "foobar");

        sleep(1001).await;

        {
            tx!(|db| db.swap(id1, Node::text("baz")).await?);
            tx!(|db| db.publish_broadcast(root_id).await?);
        }

        // wait 60 seconds to ensure changes have been propagated to all edge
        // locations
        sleep(MILLIS_TO_WAIT_FOR_CHANGES_TO_PROPAGATE).await;

        let bytes_received = current.subscribe_to_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);
        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert!(bytes_received > 0);
        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "baz");
        assert_eq!(current.get(namespaced_id2).await?.unwrap().str()?, "foobar");
    }
}

test! {
    async fn broadcast_updates(storage) -> Result<()> {
        // nearly identical to the previous test, but calls
        // `db.update_broadcasts(root_id)` instead, with root_id not being
        // explicitly published.
        let db = Db::open(storage).await?;

        let root_id = Id::root();

        let (id1, id2, broadcast_id) = {
            let page_id = tx!(|db| db.add(Node::List(Layout::Page, vec![])).await?);
            tx!(|db| db.push(root_id, Child::Lazy(page_id)).await?);

            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(page_id, Child::Lazy(id1)).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));

            let text2 = Node::text("foobar");
            let id2 = tx!(|db| db.add(text2).await?);
            tx!(|db| db.push(page_id, Child::Lazy(id2)).await?);
            tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "foobar"));

            let broadcast_id = tx!(|db| db.publish_broadcast(page_id).await?).broadcast_id;

            (id1, id2, broadcast_id)
        };

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;
        let mut current = other.current().await;
        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert!(bytes_received > 0);
        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);

        let namespaced_id1 = current.namespaced_id(&broadcast_id, id1).await?;
        let namespaced_id2 = current.namespaced_id(&broadcast_id, id2).await?;

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "foo");
        assert_eq!(current.get(namespaced_id2).await?.unwrap().str()?, "foobar");

        sleep(1001).await;

        {
            tx!(|db| db.swap(id1, Node::text("baz")).await?);
            tx!(|db| db.update_broadcasts(root_id).await?);
        }

        // wait 60 seconds to ensure changes have been propagated to all edge
        // locations
        sleep(MILLIS_TO_WAIT_FOR_CHANGES_TO_PROPAGATE).await;

        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert!(bytes_received > 0);
        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "baz");
        assert_eq!(current.get(namespaced_id2).await?.unwrap().str()?, "foobar");
    }
}

test! {
    async fn broadcast_and_fetch_updates(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let root_id = Id::root();

        let id1 = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id1)).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "foo"));
            id1
        };

        let mut current = db.current().await;
        let broadcast_id = current.publish_broadcast(root_id).await?.broadcast_id;
        current.commit().await?;

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;
        let mut current = other.current().await;

        assert!(current.list_broadcasts(id1).await?.is_empty());

        current.fetch_broadcast(&broadcast_id).await?;

        let namespaced_id1 = current.namespaced_id(&broadcast_id, id1).await?;

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "foo");

        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);
        current.commit().await?;

        sleep(1001).await;

        let id2 = {
            let text1 = Node::text("bar");
            tx!(|db| db.swap(id1, text1).await?);
            tx!(|db| assert_eq!(db.get(id1).await?.unwrap().str()?, "bar"));

            let text2 = Node::text("baz");
            let id2 = tx!(|db| db.add(text2).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id2)).await?);
            tx!(|db| assert_eq!(db.get(id2).await?.unwrap().str()?, "baz"));

            id2
        };

        let mut current = db.current().await;
        let broadcast_id_after_update = current.publish_broadcast(root_id).await?.broadcast_id;
        current.commit().await?;

        // wait 60 seconds to ensure changes have been propagated to all edge
        // locations
        sleep(MILLIS_TO_WAIT_FOR_CHANGES_TO_PROPAGATE).await;

        assert_eq!(broadcast_id_after_update, broadcast_id);

        let mut current = other.current().await;

        let namespaced_id2 = current.namespaced_id(&broadcast_id, id2).await?;

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "foo");
        assert_eq!(current.get(namespaced_id2).await?, None);

        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert!(bytes_received > 0);

        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert_eq!(bytes_received, 0);

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "bar");
        assert_eq!(current.get(namespaced_id2).await?.unwrap().str()?, "baz");
    }
}

test! {
    async fn broadcast_and_find_overlaps(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        let root_id = Id::root();

        let id1 = {
            let text1 = Node::text("foo");
            let id1 = tx!(|db| db.add(text1).await?);
            tx!(|db| db.push(root_id, Child::Lazy(id1)).await?);

            id1
        };

        let mut current = db.current().await;
        let broadcast_id = current.publish_broadcast(root_id).await?.broadcast_id;
        current.commit().await?;

        let storage = MemoryStorage::new();
        let other = Db::open(storage).await?;

        let other_id1 = {
            let mut current = other.current().await;

            let text1 = Node::text("This is some text in the other DB");
            let text1_id = current.add(text1).await?;
            current.push(root_id, text1_id).await?;

            assert!(current.list_broadcasts(id1).await?.is_empty());

            current.commit().await?;
            text1_id
        };

        let mut current = other.current().await;
        current.fetch_broadcast(&broadcast_id).await?;

        let namespaced_id1 = current.namespaced_id(&broadcast_id, id1).await?;

        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "foo");
        assert_eq!(current.overlaps(other_id1).await?, vec![]);
        current.commit().await?;

        sleep(1001).await;

        {
            let text1 = Node::text("This is some text in the broadcast DB");
            tx!(|db| db.swap(id1, text1).await?);
        };

        let mut current = db.current().await;
        let broadcast_id_after_update = current.publish_broadcast(root_id).await?.broadcast_id;
        current.commit().await?;

        // wait 60 seconds to ensure changes have been propagated to all edge
        // locations
        sleep(MILLIS_TO_WAIT_FOR_CHANGES_TO_PROPAGATE).await;

        assert_eq!(broadcast_id_after_update, broadcast_id);

        let mut current = other.current().await;

        let bytes_received = current.fetch_broadcast(&broadcast_id).await?;
        assert!(bytes_received > 0);
        assert_eq!(current.get(namespaced_id1).await?.unwrap().str()?, "This is some text in the broadcast DB");

        let overlaps_external = current.overlaps(namespaced_id1).await?;
        assert_eq!(overlaps_external.len(), 1);
        assert_eq!(overlaps_external[0].id, other_id1);

        let overlaps = current.overlaps(other_id1).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, namespaced_id1);
        current.commit().await?;
    }
}

test! {
    async fn broadcast_non_root_node(storage) -> Result<()> {
        let db = Db::open(storage).await?;

        // Nodes support layouts and styles, for example as a page of blocks...
        let page1_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::styled(BlockStyle::Heading, Node::text("A Heading!")),
                Node::text("This is the first paragraph."),
                Node::text("Unsurprisingly this is the second one..."),
            ])).await?
        });

        // ...or as inline spans that are chained together:
        let page2_id = tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                Node::list(Layout::Chain, vec![
                    Node::text("And this is the "),
                    Node::styled(SpanStyle::Italic, Node::text("last")),
                    Node::text(" paragraph...")
                ])
            ])).await?
        });

        // Documents can form a graph, with nodes keeping track of all parents:
        tx!(|db| {
            db.add(Node::list(Layout::Page, vec![
                page1_id,
                page1_id,
            ])).await?;

            assert_eq!(db.parents(page1_id).await?.len(), 2);
            assert_eq!(db.parents(page2_id).await?.len(), 0);
        });

        // All text is indexed, the DB supports "overlap" similarity search:
        tx!(|db| {
            let paragraph1_id = db.get(page1_id).await?.unwrap().children()[1].id()?;
            let paragraph3_id = db.get(page2_id).await?.unwrap().children()[0].id()?;

            let overlaps_of_p1 = db.overlaps(paragraph1_id).await?;
            assert_eq!(overlaps_of_p1.len(), 1);
            assert_eq!(overlaps_of_p1[0].id, paragraph3_id);
            assert!(overlaps_of_p1[0].score() > 0.5);
        });

        // Nodes (with all their descendants) can be published globally...
        let broadcast = tx!(|db| {
            db.publish_broadcast(page1_id).await?
        });

        // ...and the broadcast can then be fetched in another remote DB:
        let other_storage_name = "storage_for_import";
        let other_storage = storage::open(other_storage_name).await?;
        let db2 = Db::open(other_storage).await?;
        tx!(|db2| {
            let paragraph_id = db2.add(Node::text("This is the first paragraph, right?")).await?;
            db2.add(Node::list(Layout::Page, vec![paragraph_id])).await?;
            // The DB is empty except for this paragraph, so no overlaps:
            assert_eq!(db2.overlaps(paragraph_id).await?.len(), 0);
            // But when the broadcast paragraph is fetched, there is an overlap:
            db2.fetch_broadcast(&broadcast.broadcast_id).await?;
            assert_eq!(db2.overlaps(paragraph_id).await?.len(), 1);
        });
        storage::purge(other_storage_name).await?;
    }
}

#[cfg(target_arch = "wasm32")]
async fn sleep(millis: u64) {
    let promise = js_sys::Promise::new(&mut |yes, _| {
        let win = web_sys::window().unwrap();
        win.set_timeout_with_callback_and_timeout_and_arguments_0(&yes, millis as i32)
            .unwrap();
    });
    let js_fut = wasm_bindgen_futures::JsFuture::from(promise);
    js_fut.await.unwrap();
}

#[cfg(not(target_arch = "wasm32"))]
async fn sleep(millis: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
}

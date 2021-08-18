use assemblage_db::{
    data::{Id, Layout, Node, Parent},
    tx, Db, Result,
};
use assemblage_kv::{
    storage::{self, Storage},
    test,
};
use std::{collections::HashSet, iter::FromIterator};
use storage::MemoryStorage;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn export_and_import_nodes(storage) -> Result<()> {
        let storage_name = storage.name().to_string();
        let db = Db::open(storage).await?;

        let not_exported1_id = tx!(|db| db.add(Node::text("not exported")).await?);

        let exported_child1_id = tx!(|db| db.add(Node::text("should be exported")).await?);

        let exported_descendant_id = tx!(|db| db.add(Node::text("exported")).await?);

        let exported_child2_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("should "),
            Node::text("also "),
            Node::text("be "),
            Node::list(Layout::Chain, vec![exported_descendant_id]),
        ])).await?);

        let exported_id = tx!(|db| db.add(Node::list(Layout::Page, vec![
            exported_child1_id,
            exported_child2_id,
        ])).await?);

        let not_exported_parent_id = tx!(|db| db.add(Node::list(Layout::Page, vec![
            exported_child2_id
        ])).await?);

        let not_exported2_id = tx!(|db| db.add(Node::text("also not exported")).await?);

        let current = db.current().await;
        assert_eq!(current.parents(exported_id).await?.len(), 0);
        assert_eq!(current.parents(exported_child1_id).await?.len(), 1);
        assert_eq!(current.parents(exported_child2_id).await?.len(), 2);

        let (exported_bytes, ids) = current.export(exported_id).await?;
        assert!(ids.contains(&exported_id));
        assert!(ids.contains(&exported_child1_id));
        assert!(ids.contains(&exported_child2_id));
        assert!(ids.contains(&exported_descendant_id));
        assert!(!ids.contains(&not_exported1_id));
        assert!(!ids.contains(&not_exported2_id));
        assert!(!ids.contains(&not_exported_parent_id));

        let size_before_export = db.size().await?;

        storage::purge(&storage_name).await?;
        let storage = storage::open(&storage_name).await?;
        let db = Db::open(storage).await?;

        let size_before_import = db.size().await?;
        assert!(size_before_import < size_before_export);

        let mut current = db.current().await;
        current.import(exported_bytes.as_slice(), Id::root()).await?;
        current.commit().await?;

        let size_after_import = db.size().await?;

        assert!(size_after_import > size_before_import);
        assert!(size_after_import < size_before_export);

        let current = db.current().await;

        assert_eq!(current.get(exported_id).await?.unwrap().children().len(), 2);
        assert_eq!(current.get(exported_child1_id).await?.unwrap().str()?, "should be exported");
        assert_eq!(current.get(exported_child2_id).await?.unwrap().children().len(), 4);
        assert_eq!(current.get(exported_descendant_id).await?.unwrap().str()?, "exported");
        assert_eq!(current.get(not_exported1_id).await?, None);
        assert_eq!(current.get(not_exported2_id).await?, None);
        assert_eq!(current.get(not_exported_parent_id).await?, None);
        assert_eq!(current.parents(exported_id).await?.len(), 1);
        assert_eq!(
            current.parents(exported_id).await?,
            HashSet::from_iter(vec![Parent::new(Id::root(), 0)])
        );
        assert_eq!(current.parents(exported_child1_id).await?.len(), 1);
        assert_eq!(current.parents(exported_child2_id).await?.len(), 1);
    }
}

test! {
    async fn export_and_import_overlaps(storage) -> Result<()> {
        let storage_for_export = MemoryStorage::new();
        let db = Db::open(storage_for_export).await?;

        tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::text("not exported, some paragraph of text")
        ])).await?);

        let exported_child1_id = tx!(|db| db.add(Node::text("is exported, some paragraph of text")).await?);

        let exported_child2_id = tx!(|db| db.add(Node::list(Layout::Chain, vec![
            Node::text("this "),
            Node::list(Layout::Chain, vec![exported_child1_id]),
        ])).await?);

        let exported_id = tx!(|db| db.add(Node::list(Layout::Page, vec![
            exported_child1_id,
            exported_child2_id
        ])).await?);

        tx!(|db| db.add(Node::list(Layout::Page, vec![
            Node::text("also not exported, some paragraph of text")
        ])).await?);

        let current = db.current().await;
        let overlaps = current.overlaps(exported_child1_id).await?;
        assert_eq!(overlaps.len(), 3);
        let ids: HashSet<Id> = overlaps.iter().map(|o| o.id).collect();
        assert!(ids.contains(&exported_child2_id));

        let (exported_bytes, ids) = current.export(exported_id).await?;
        assert!(ids.contains(&exported_id));
        assert!(ids.contains(&exported_child1_id));
        assert!(ids.contains(&exported_child2_id));

        let db = Db::open(storage).await?;
        let imported_store_name = "imported_store";
        let mut current = db.current().await;
        current.import(exported_bytes.as_slice(), Id::root()).await?;

        let overlaps = current.overlaps(exported_child1_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, exported_child2_id);

        storage::purge(imported_store_name).await?;
    }
}

test! {
    async fn export_and_import_overlaps_incrementally(storage) -> Result<()> {
        let db_for_export = Db::open(storage).await?;
        let mut current = db_for_export.current().await;

        let root_id = Id::root();
        let id1 = current.add(Node::text("foo")).await?;
        current.push(root_id, id1).await?;
        current.commit().await?;

        let current = db_for_export.current().await;
        let (exported_bytes, ids) = current.export(root_id).await?;
        assert_eq!(ids, HashSet::from_iter(vec![root_id, id1]));

        let storage_for_import = MemoryStorage::new();
        let db = Db::open(storage_for_import).await?;
        let mut other = db.current().await;
        other.import(exported_bytes.as_slice(), Id::root()).await?;

        let other_id1 = {
            let root_id = Id::root();
            let other_id1 = other.add(Node::text("some text with overlap")).await?;
            other.push(root_id, other_id1).await?;

            assert_eq!(other.overlaps(other_id1).await?, vec![]);
            other.commit().await?;
            other_id1
        };

        let last_exported = current.last_updated().await?.unwrap();
        let (exported_bytes, ids) = current.export_since(root_id, last_exported).await?;
        assert_eq!(ids, HashSet::from_iter(vec![root_id, id1]));
        assert_eq!(exported_bytes.len(), 0);

        // wait a bit and then create a new transaction to ensure that
        // last_updated > last_exported
        sleep(10).await;
        let mut current = db_for_export.current().await;
        current.swap(id1, Node::text("some text with overlap")).await?;

        let (exported_bytes, ids) = current.export_since(root_id, last_exported).await?;
        assert_eq!(ids, HashSet::from_iter(vec![root_id, id1]));
        assert!(!exported_bytes.is_empty());

        let mut other = db.current().await;
        other.import(exported_bytes.as_slice(), Id::root()).await?;

        {
            assert_eq!(other.get(id1).await?.unwrap().str()?, "some text with overlap");

            let overlaps = other.overlaps(id1).await?;
            assert_eq!(overlaps.len(), 1);
            assert_eq!(overlaps[0].id, other_id1);

            let overlaps = other.overlaps(other_id1).await?;
            assert_eq!(overlaps.len(), 1);
            assert_eq!(overlaps[0].id, id1);
        }
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

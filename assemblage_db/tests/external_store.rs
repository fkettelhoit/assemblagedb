use assemblage_db::{
    data::{Id, Layout, Node},
    tx, Db, Result,
};
use assemblage_kv::{
    storage::{self, Storage},
    test,
};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn import_from_external_store(storage) -> Result<()> {
        let ext_store_name = "ext_for_import";
        let ext_storage = storage::open(ext_store_name).await?;
        let ext_db = Db::open(ext_storage).await?;

        let ext_id = tx!(|ext_db| {
            ext_db.add(Node::text("some text in the external database")).await?
        });

        let mut ext_storage = ext_db.into_storage()?;

        let own_db = Db::open(storage).await?;
        let bytes = ext_storage.read(0, ext_storage.len() as u32).await?;
        let mut current = own_db.current().await;
        current.import(bytes.as_slice(), Id::root()).await?;

        assert_eq!(current.get(ext_id).await?.unwrap().str()?, "some text in the external database");

        storage::purge(ext_store_name).await?;
    }
}

test! {
    async fn index_and_search_external_store(storage) -> Result<()> {
        let ext_store_name = "ext_for_search";
        let ext_storage = storage::open(ext_store_name).await?;
        let ext_db = Db::open(ext_storage).await?;

        {
            tx!(|ext_db| ext_db.add(Node::list(Layout::Page, vec![
                Node::text("some text in the external database")
            ])).await?);

            let matches = ext_db.current().await.search("some text in the external database").await?;
            assert_eq!(matches.len(), 1);
        }

        let mut ext_storage = ext_db.into_storage()?;

        let own_db = Db::open(storage).await?;
        let bytes = ext_storage.read(0, ext_storage.len() as u32).await?;
        let mut snapshot = own_db.current().await;
        snapshot.import(bytes.as_slice(), Id::root()).await?;

        let matches = snapshot.search("some text in the external database").await?;
        assert_eq!(matches.len(), 1);

        let ext = snapshot.get(matches[0].id).await?.unwrap();
        assert_eq!(ext.str()?, "some text in the external database");

        storage::purge(ext_store_name).await?;
    }
}

test! {
    async fn overlap_with_external_store(storage) -> Result<()> {
        let ext_store_name = "ext_for_overlap";
        storage::purge(ext_store_name).await?;
        let ext_storage = storage::open(ext_store_name).await?;
        let ext_db = Db::open(ext_storage).await?;

        let ext_text_id = tx!(|ext_db| {
            let text_id = ext_db.add(Node::text("some text in the external database")).await?;
            ext_db.add(Node::list(Layout::Page, vec![text_id])).await?;
            text_id
        });

        let mut ext_storage = ext_db.into_storage()?;

        let own_db = Db::open(storage).await?;

        let bytes = ext_storage.read(0, ext_storage.len() as u32).await?;

        // We need to use a different name, otherwise the existing external
        // store will be used (with all its contents) and the imported bytes
        // will be appended to the existing external store. This will then break
        // any overlap calculation because the existing external store is a
        // Db store (without any overlap information).
        let orign_ext_store_name = ext_store_name;
        let ext_store_name = "ext_store";
        let mut current = own_db.current().await;
        current.import(bytes.as_slice(), Id::root()).await?;
        current.commit().await?;

        let overlapping_text_id = tx!(|own_db| own_db.add(Node::text("some text in the own database")).await?);
        tx!(|own_db| own_db.add(Node::list(Layout::Page, vec![overlapping_text_id])).await?);

        let snapshot = own_db.current().await;
        let overlaps = snapshot.overlaps(overlapping_text_id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, ext_text_id);

        let overlaps = snapshot.overlaps(overlaps[0].id).await?;
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].id, overlapping_text_id);

        storage::purge(ext_store_name).await?;
        storage::purge(orign_ext_store_name).await?;
    }
}

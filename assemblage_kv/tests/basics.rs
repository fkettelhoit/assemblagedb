use assemblage_kv::{storage, storage::Storage, test, KvStore, Result};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const SLOT_0: u8 = 0;

test! {
    async fn insert_and_get_int_keys(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, 3, "foo")?;
        t.insert(SLOT_0, 5, "bar")?;

        assert_eq!(t.get::<_, String>(SLOT_0, &3).await?.unwrap(), "foo");
        assert_eq!(t.get::<_, String>(SLOT_0, &5).await?.unwrap(), "bar");

        assert_eq!(t.get::<_, String>(SLOT_0, &1000).await?, None);
        t.commit().await?;
    }
}

test! {
    async fn insert_and_get_string_keys(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, "key foo", "foo")?;
        t.insert(SLOT_0, "key bar", "bar")?;

        assert_eq!(
            t.get::<_, String>(SLOT_0, &"key foo").await?.unwrap(),
            "foo"
        );
        assert_eq!(
            t.get::<_, String>(SLOT_0, &"key bar").await?.unwrap(),
            "bar"
        );

        assert_eq!(
            t.get::<_, String>(SLOT_0, &"key does not exist").await?,
            None
        );
        t.commit().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, "key foo", "foo")?;
        t.insert(SLOT_0, "key bar", "bar")?;

        assert_eq!(
            t.get::<_, String>(SLOT_0, &"key foo").await?.unwrap(),
            "foo"
        );
        assert_eq!(
            t.get::<_, String>(SLOT_0, &"key bar").await?.unwrap(),
            "bar"
        );

        assert_eq!(
            t.get::<_, String>(SLOT_0, &"key does not exist").await?,
            None
        );
        t.commit().await?;
    }
}

test! {
    async fn insert_and_delete_ints_and_strings(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;

        {
            let mut t = store.current().await;
            t.insert(SLOT_0, 3, "foo")?;
            assert_eq!(t.get::<_, String>(SLOT_0, &3).await?.unwrap(), "foo");
            t.commit().await?;
        }

        {
            let mut t = store.current().await;
            t.remove(SLOT_0, 3)?;

            assert_eq!(t.get::<_, String>(SLOT_0, &3).await?, None);
            assert_eq!(t.get_unremoved::<_, String>(SLOT_0, &3).await?.unwrap(), "foo");

            t.insert(SLOT_0, "key foo", "foo")?;
            assert_eq!(
                t.get::<_, String>(SLOT_0, &"key foo").await?.unwrap(),
                "foo"
            );
            t.commit().await?;
        }

        {
            let mut t = store.current().await;
            t.remove(SLOT_0, "key foo")?;

            assert_eq!(t.get::<_, String>(SLOT_0, &"key foo").await?, None);
            assert_eq!(
                t.get_unremoved::<_, String>(SLOT_0, &"key foo").await?.unwrap(),
                "foo"
            );
            t.commit().await?;
        }
    }
}

test! {
    async fn insert_and_get_zero_and_empty_string(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let current = store.current().await;
        assert_eq!(current.get::<_, ()>(SLOT_0, &0).await?, None);
        assert_eq!(current.get::<_, ()>(SLOT_0, &"").await?, None);

        let mut t = store.current().await;
        t.insert(SLOT_0, &0, "foo")?;
        t.insert(SLOT_0, &"", "bar")?;
        t.commit().await?;

        let current = store.current().await;
        assert_eq!(current.get::<_, String>(SLOT_0, &0).await?.unwrap(), "foo");
        assert_eq!(current.get::<_, String>(SLOT_0, &"").await?.unwrap(), "bar");
    }
}

test! {
    async fn put_get_and_remove(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        assert_eq!(store.name(), store_name);

        let empty_key: Vec<u8> = vec![];
        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];

        let key2 = vec![4, 5];
        let value2 = vec![6, 7];

        let value3 = vec![8, 9, 10, 10];

        let mut t = store.current().await;
        t.insert(SLOT_0, key2.clone(), vec![0])?;
        t.insert(SLOT_0, key1.clone(), value1.clone())?;
        t.insert(SLOT_0, key2.clone(), value2.clone())?;
        t.insert(SLOT_0, key1.clone(), value3.clone())?;
        t.insert::<Vec<u8>, _>(SLOT_0, vec![], value3.clone())?;
        t.commit().await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &empty_key).await?.unwrap(), value3);

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);

        let mut t = store.current().await;
        t.remove(SLOT_0, key1.clone())?;
        t.remove(SLOT_0, key2.clone())?;
        assert_eq!(t.get::<_, Vec<u8>>(SLOT_0, &key1).await?, None);
        assert_eq!(t.get::<_, Vec<u8>>(SLOT_0, &key2).await?, None);
        assert_eq!(t.get_unremoved::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);
        assert_eq!(t.get_unremoved::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);
        t.commit().await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?, None);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?, None);
        assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        {
            let snapshot = store.current().await;
            assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?, None);
            assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?, None);
            assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);
            assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);
        }

        store.merge().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);

        let snapshot = store.current().await;
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?, None);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?, None);
        assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key1).await?, None);
        assert_eq!(snapshot.get_unremoved::<_, Vec<u8>>(SLOT_0, &key2).await?, None);
    }
}

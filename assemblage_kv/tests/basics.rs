use assemblage_kv::{storage, storage::Storage, test, KvStore, Result};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn insert_and_get_int_keys(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(vec![3], "foo".into())?;
        t.insert(vec![5], "bar".into())?;

        assert_eq!(t.get(&[3]).await?, Some("foo".into()));
        assert_eq!(t.get(&[5]).await?, Some("bar".into()));

        assert_eq!(t.get(&[200]).await?, None);
        t.commit().await?;
    }
}

test! {
    async fn insert_and_get_string_keys(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert("key foo".into(), "foo".into())?;
        t.insert("key bar".into(), "bar".into())?;

        assert_eq!(t.get("key foo".as_bytes()).await?, Some("foo".into()));
        assert_eq!(t.get("key bar".as_bytes()).await?, Some("bar".into()));
        assert_eq!(t.get("key does not exist".as_bytes()).await?, None);
        t.commit().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        t.insert("key foo".into(), vec![1, 2])?;
        t.insert("key bar".into(), vec![3, 4])?;

        assert_eq!(t.get("key foo".as_bytes()).await?, Some(vec![1, 2]));
        assert_eq!(t.get("key bar".as_bytes()).await?, Some(vec![3, 4]));
        assert_eq!(t.get("key does not exist".as_bytes()).await?, None);
        t.commit().await?;
    }
}

test! {
    async fn insert_and_delete_ints_and_strings(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;

        {
            let mut t = store.current().await;
            t.insert(vec![3], "foo".into())?;
            assert_eq!(t.get(&[3]).await?, Some("foo".into()));
            t.commit().await?;
        }

        {
            let mut t = store.current().await;
            t.remove(vec![3])?;

            assert_eq!(t.get(&[3]).await?, None);
            assert_eq!(t.get_unremoved(&[3]).await?, Some("foo".into()));

            t.insert("key foo".into(), "foo".into())?;
            assert_eq!(t.get("key foo".as_bytes()).await?, Some("foo".into()));
            t.commit().await?;
        }

        {
            let mut t = store.current().await;
            t.remove("key foo".into())?;

            assert_eq!(t.get("key foo".as_bytes()).await?, None);
            assert_eq!(t.get_unremoved("key foo".as_bytes()).await?, Some("foo".into()));
            t.commit().await?;
        }
    }
}

test! {
    async fn insert_and_get_empty_key(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let current = store.current().await;
        assert_eq!(current.get(&[]).await?, None);

        let mut t = store.current().await;
        t.insert(vec![], "foo".into())?;
        t.commit().await?;

        let current = store.current().await;
        assert_eq!(current.get(&[]).await?, Some("foo".into()));
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
        t.insert(key2.clone(), vec![0])?;
        t.insert(key1.clone(), value1.clone())?;
        t.insert(key2.clone(), value2.clone())?;
        t.insert(key1.clone(), value3.clone())?;
        t.insert(vec![], value3.clone())?;
        t.commit().await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get(&key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get(&key2).await?.unwrap(), value2);
        assert_eq!(snapshot.get(&empty_key).await?.unwrap(), value3);

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get(&key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get(&key2).await?.unwrap(), value2);

        let mut t = store.current().await;
        t.remove(key1.clone())?;
        t.remove(key2.clone())?;
        assert_eq!(t.get(&key1).await?, None);
        assert_eq!(t.get(&key2).await?, None);
        assert_eq!(t.get_unremoved(&key1).await?.unwrap(), value3);
        assert_eq!(t.get_unremoved(&key2).await?.unwrap(), value2);
        t.commit().await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get(&key1).await?, None);
        assert_eq!(snapshot.get(&key2).await?, None);
        assert_eq!(snapshot.get_unremoved(&key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get_unremoved(&key2).await?.unwrap(), value2);

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        {
            let snapshot = store.current().await;
            assert_eq!(snapshot.get(&key1).await?, None);
            assert_eq!(snapshot.get(&key2).await?, None);
            assert_eq!(snapshot.get_unremoved(&key1).await?.unwrap(), value3);
            assert_eq!(snapshot.get_unremoved(&key2).await?.unwrap(), value2);
        }

        store.merge().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        assert_eq!(snapshot.get_unremoved(&key1).await?.unwrap(), value3);
        assert_eq!(snapshot.get_unremoved(&key2).await?.unwrap(), value2);

        let snapshot = store.current().await;
        assert_eq!(snapshot.get(&key1).await?, None);
        assert_eq!(snapshot.get(&key2).await?, None);
        assert_eq!(snapshot.get_unremoved(&key1).await?, None);
        assert_eq!(snapshot.get_unremoved(&key2).await?, None);
    }
}

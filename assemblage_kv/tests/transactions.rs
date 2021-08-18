use assemblage_kv::{storage, storage::Storage, test, Error, KvStore, Result};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const SLOT_0: u8 = 0;

test! {
    async fn merge_and_init1(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let mut store = KvStore::open(storage).await?;
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5];

        let value1 = vec![6, 7];
        let value2 = vec![8, 9, 10];
        let value3 = vec![11, 12, 13];

        let mut t = store.current().await;
        t.insert(SLOT_0, key1.clone(), value1.clone())?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, key1.clone(), value2.clone())?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, key2.clone(), value3.clone())?;
        t.commit().await?;

        store.merge().await?;
        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value2);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value3);
    }
}

test! {
    async fn merge_and_init2(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let mut store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, &3, "will be overwritten")?;
        t.insert(SLOT_0, &"key foo", "should remain")?;
        t.insert(SLOT_0, &"key bar", "will be removed")?;
        t.insert(SLOT_0, &"key baz", "will be overwritten")?;
        t.commit().await?;

        let current = store.current().await;
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"key bar").await?.unwrap(),
            "will be removed"
        );
        drop(current);

        let mut t = store.current().await;
        t.insert(SLOT_0, &"key baz", "should remain")?;
        t.insert(SLOT_0, &3, "should remain")?;
        t.insert(SLOT_0, &5, "should remain")?;
        t.remove(SLOT_0, &"key bar")?;
        t.commit().await?;

        let current = store.current().await;
        assert_eq!(
            current.get::<_, String>(SLOT_0, &3).await?.unwrap(),
            "should remain"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &5).await?.unwrap(),
            "should remain"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"key foo").await?.unwrap(),
            "should remain"
        );
        assert_eq!(current.get::<_, String>(SLOT_0, &"key bar").await?, None);
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"key baz").await?.unwrap(),
            "should remain"
        );
        drop(current);

        store.merge().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;
        let current = store.current().await;

        assert_eq!(
            current.get::<_, String>(SLOT_0, &3).await?.unwrap(),
            "should remain"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &5).await?.unwrap(),
            "should remain"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"key foo").await?.unwrap(),
            "should remain"
        );
        assert_eq!(current.get::<_, String>(SLOT_0, &"key bar").await?, None);
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"key baz").await?.unwrap(),
            "should remain"
        );
    }
}

test! {
    async fn discard_corrupt_write1(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5];

        let value1 = vec![6, 7];
        let value2 = vec![8, 9, 10];
        let value3 = vec![11, 12, 13];

        let mut t = store.current().await;
        t.insert(SLOT_0, key1.clone(), value1.clone())?;
        t.insert(SLOT_0, key2.clone(), value2.clone())?;

        assert_eq!(t.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value1);
        assert_eq!(t.get_unremoved::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);

        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, key1.clone(), value3.clone())?;
        assert_eq!(t.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value3);

        t.commit().await?;

        corrupt_last_bytes(store.into_storage()?, 1).await?;

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let snapshot = store.current().await;

        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value1);

        drop(snapshot);
        store.merge().await?;
    }
}

test! {
    async fn discard_corrupt_write2(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, &"foo", "foo v1")?;
        t.insert(SLOT_0, &"bar", "bar")?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, &"foo", "foo v2")?;
        t.commit().await?;

        let current = store.current().await;

        assert_eq!(
            current.get::<_, String>(SLOT_0, &"foo").await?.unwrap(),
            "foo v2"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"bar").await?.unwrap(),
            "bar"
        );

        drop(current);
        corrupt_last_bytes(store.into_storage()?, 10).await?;

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let current = store.current().await;

        assert_eq!(
            current.get::<_, String>(SLOT_0, &"foo").await?.unwrap(),
            "foo v1"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"bar").await?.unwrap(),
            "bar"
        );

        drop(current);
        store.merge().await?;
    }
}

test! {
    async fn overwrite_corrupt_data_with_next_writes(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, &"foo", "foo v1")?;
        t.insert(SLOT_0, &"bar", "bar")?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, &"foo", "foo v2")?;
        t.commit().await?;

        let current = store.current().await;

        assert_eq!(
            current.get::<_, String>(SLOT_0, &"foo").await?.unwrap(),
            "foo v2"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"bar").await?.unwrap(),
            "bar"
        );

        drop(current);
        corrupt_last_bytes(store.into_storage()?, 10).await?;

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, &"foo", "foo v2")?;
        t.commit().await?;

        let current = store.current().await;

        assert_eq!(
            current.get::<_, String>(SLOT_0, &"foo").await?.unwrap(),
            "foo v2"
        );
        assert_eq!(
            current.get::<_, String>(SLOT_0, &"bar").await?.unwrap(),
            "bar"
        );

        drop(current);
        store.merge().await?;
    }
}

test! {
    async fn remove_key_in_transaction(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, &5, "foo")?;
        t.commit().await?;

        let current = store.current().await;
        assert_eq!(current.get::<_, String>(SLOT_0, &5).await?.unwrap(), "foo");

        let mut t = store.current().await;
        t.remove(SLOT_0, &5)?;

        assert_eq!(t.get::<_, String>(SLOT_0, &5).await?, None);
        assert_eq!(t.get_unremoved::<_, String>(SLOT_0, &5).await?.unwrap(), "foo");

        assert_eq!(current.get::<_, String>(SLOT_0, &5).await?.unwrap(), "foo");
        assert_eq!(current.get_unremoved::<_, String>(SLOT_0, &5).await?.unwrap(), "foo");

        t.commit().await?;

        let current = store.current().await;
        assert_eq!(current.get::<_, String>(SLOT_0, &5).await?, None);
        assert_eq!(current.get_unremoved::<_, String>(SLOT_0, &5).await?.unwrap(), "foo");
    }
}

test! {
    async fn roll_back_transaction_on_abort(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, 2, "foo v1")?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, 2, "foo v2")?;
        t.insert(SLOT_0, 3, "bar")?;

        t.abort().await?;

        let current = store.current().await;
        assert_eq!(
            current.get::<_, String>(SLOT_0, &2).await?.unwrap(),
            "foo v1"
        );
        assert_eq!(current.get::<_, String>(SLOT_0, &3).await?, None);
    }
}

test! {
    async fn roll_back_transaction_after_data_corruption(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(SLOT_0, 2, "foo v1")?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, 2, "foo v2")?;
        t.insert(SLOT_0, 3, "bar")?;

        t.commit().await?;

        corrupt_last_bytes(store.into_storage()?, 1).await?;
        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let current = store.current().await;
        assert_eq!(
            current.get::<_, String>(SLOT_0, &2).await?.unwrap(),
            "foo v1"
        );
        assert_eq!(current.get::<_, String>(SLOT_0, &3).await?, None);
    }
}

test! {
    async fn merge_transaction_results(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let mut store = KvStore::open(storage).await?;
        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];

        let key2 = vec![4, 5];
        let value2 = vec![6, 7];

        let mut t = store.current().await;
        t.insert(SLOT_0, key1.clone(), value1.clone())?;
        t.insert(SLOT_0, key2.clone(), value2.clone())?;
        t.commit().await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, key2.clone(), value1.clone())?;
        t.commit().await?;

        store.merge().await?;
        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let snapshot = store.current().await;
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value1);
        assert_eq!(snapshot.get::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value1);
    }
}

test! {
    async fn abort_unfinished_transactions(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5];
        let value1 = vec![6, 7];
        let value2 = vec![8, 9, 10];

        let mut t = store.current().await;
        t.insert(SLOT_0, key1.clone(), value1.clone())?;
        t.insert(SLOT_0, key2.clone(), value2.clone())?;
        assert_eq!(t.get::<_, Vec<u8>>(SLOT_0, &key1).await?.unwrap(), value1);
        assert_eq!(t.get::<_, Vec<u8>>(SLOT_0, &key2).await?.unwrap(), value2);
        t.commit().await?;

        corrupt_last_bytes(store.into_storage()?, 1).await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;
        let len1 = store.len().await;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;
        let len2 = store.len().await;

        assert_eq!(len1, len2);
    }
}

test! {
    async fn concurrent_transactions_without_conflict(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, "foo", 0)?;
        t.insert(SLOT_0, "bar", 0)?;
        t.commit().await?;

        {
            let mut t = store.current().await;
            let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();

            {
                let mut t = store.current().await;
                let val_bar = t.get::<_, u32>(SLOT_0, &"bar").await?.unwrap();
                t.insert(SLOT_0, "bar", val_bar + 10)?;
                t.commit().await?;
            }

            t.insert(SLOT_0, "foo", val_foo + 1)?;
            t.commit().await?;
        }

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 1);
        let val_bar = t.get::<_, u32>(SLOT_0, &"bar").await?.unwrap();
        assert_eq!(val_bar, 10);

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 1);
        let val_bar = t.get::<_, u32>(SLOT_0, &"bar").await?.unwrap();
        assert_eq!(val_bar, 10);

        drop(t);
        store.merge().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 1);
        let val_bar = t.get::<_, u32>(SLOT_0, &"bar").await?.unwrap();
        assert_eq!(val_bar, 10);
    }
}

test! {
    async fn concurrent_transactions_with_conflict(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        t.insert(SLOT_0, "foo", 0)?;
        t.commit().await?;

        {
            let mut t = store.current().await;
            let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();

            {
                let mut t = store.current().await;
                let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
                t.insert(SLOT_0, "foo", val_foo + 10)?;
                t.commit().await?;
            }

            t.insert(SLOT_0, "foo", val_foo + 1)?;
            match t.commit().await {
                Err(Error::TransactionConflict) => {},
                instead => panic!("Expected a transaction conflict, but found {:?}", instead),
            };
        }


        let mut t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 10);
        t.insert(SLOT_0, "foo", val_foo + 1)?;
        t.commit().await?;

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 11);

        drop(t);
        store.merge().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 11);
    }
}

test! {
    async fn concurrent_transactions_with_conflicting_removed_entries(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;

        {
            let mut t = store.current().await;
            let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap_or_default();

            {
                let mut t = store.current().await;
                let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap_or_default();
                t.insert(SLOT_0, "foo", val_foo + 10)?;
                t.commit().await?;
            }

            t.insert(SLOT_0, "foo", val_foo + 1)?;
            match t.commit().await {
                Err(Error::TransactionConflict) => {},
                instead => panic!("Expected a transaction conflict, but found {:?}", instead),
            };
        }


        let mut t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 10);
        t.insert(SLOT_0, "foo", val_foo + 1)?;
        t.commit().await?;

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 11);

        drop(t);
        store.merge().await?;

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let t = store.current().await;
        let val_foo = t.get::<_, u32>(SLOT_0, &"foo").await?.unwrap();
        assert_eq!(val_foo, 11);
    }
}

async fn corrupt_last_bytes<S: Storage>(mut storage: S, bytes: u8) -> storage::Result<()> {
    let bytes = bytes as u64;
    let len = storage.len();
    let mut corrupted = storage.read(len - bytes, bytes as u32).await?;
    for byte in corrupted.iter_mut() {
        *byte = !*byte;
    }
    storage.truncate(len - bytes).await?;
    storage.write(&corrupted).await?;
    Ok(())
}

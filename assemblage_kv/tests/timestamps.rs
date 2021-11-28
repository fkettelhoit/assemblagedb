use assemblage_kv::{
    storage, storage::Storage, test, timestamp::timestamp_now_monotonic, Error, KvStore, Result,
};
use crc32fast::Hasher;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn timestamps(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert("key foo".into(), "foo".into())?;
        t.commit().await?;

        sleep(1).await;

        let mut t = store.current().await;
        t.insert("key bar".into(), "foo".into())?;
        t.commit().await?;

        let current = store.current().await;
        let t1 = current.versions("key foo".as_bytes()).await?.last().unwrap().timestamp;
        let t2 = current.versions("key bar".as_bytes()).await?.last().unwrap().timestamp;

        assert!(t1 < t2);

        assert_eq!(
            current.versions("key does not exist".as_bytes()).await?.last(),
            None
        );

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;
        let current = store.current().await;

        assert_eq!(
            current.versions("key foo".as_bytes()).await?.last().unwrap().timestamp,
            t1
        );
        assert_eq!(
            current.versions("key bar".as_bytes()).await?.last().unwrap().timestamp,
            t2
        );

        assert_eq!(
            current.versions("key does not exist".as_bytes()).await?.last(),
            None
        );
    }
}

test! {
    async fn get_versions(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let mut store = KvStore::open(storage).await?;

        {
            let mut current = store.current().await;
            current.insert("key foo".into(), vec![1])?;
            current.commit().await?;
        }

        sleep(10).await;

        {
            let mut current = store.current().await;
            current.insert("key foo".into(), vec![2])?;
            current.commit().await?;
        }

        sleep(10).await;

        {
            let mut current = store.current().await;
            current.remove("key foo".into())?;
            current.commit().await?;
        }

        sleep(10).await;

        {
            let mut current = store.current().await;
            current.insert("key foo".into(), vec![4])?;
            current.commit().await?;
        }

        {
            let current = store.current().await;
            let versions = current.versions("key foo".as_bytes()).await?;
            assert_eq!(versions.len(), 4);
            assert_eq!(current.get_version("key foo".as_bytes(), versions[0]).await?, Some(vec![1]));
            assert_eq!(current.get_version("key foo".as_bytes(), versions[1]).await?, Some(vec![2]));
            assert_eq!(current.get_version("key foo".as_bytes(), versions[2]).await?, None);
            assert_eq!(current.get_version("key foo".as_bytes(), versions[3]).await?, Some(vec![4]));
        }

        store.merge().await?;
        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        {
            let current = store.current().await;
            let versions = current.versions("key foo".as_bytes()).await?;
            assert_eq!(versions.len(), 1);
            assert_eq!(current.get("key foo".as_bytes()).await?, Some(vec![4]));
        }
    }
}

test! {
    async fn most_recent_keys(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let store = KvStore::open(storage).await?;
        let current = store.current().await;
        assert_eq!(current.last_updated().await?, None);

        let mut t = store.current().await;
        t.insert("key foo".into(), "foo".into())?;
        let t_foo = t.versions("key foo".as_bytes()).await?.last().unwrap().timestamp;
        assert_eq!(t.last_updated().await?.unwrap(), t_foo);

        t.insert("key bar".into(), "bar".into())?;
        let t_bar = t.versions("key bar".as_bytes()).await?.last().unwrap().timestamp;
        assert_eq!(t.last_updated().await?.unwrap(), t_bar);
        assert_eq!(t_foo, t_bar);

        t.commit().await?;

        let current = store.current().await;
        let t_foo1 = current.versions("key foo".as_bytes()).await?.last().unwrap().timestamp;
        assert_eq!(current.last_updated().await?.unwrap(), t_foo1);

        let mut t = store.current().await;
        t.insert("key foo".into(), "foo".into())?;
        let t_foo = t.versions("key foo".as_bytes()).await?.last().unwrap().timestamp;
        assert_eq!(t.last_updated().await?.unwrap(), t_foo);

        t.commit().await?;

        let current = store.current().await;
        let t_foo2 = current.versions("key foo".as_bytes()).await?.last().unwrap().timestamp;
        assert_eq!(current.last_updated().await?.unwrap(), t_foo2);
        assert_ne!(t_foo1, t_foo2);

        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;
        let current = store.current().await;
        assert_eq!(current.last_updated().await?.unwrap(), t_foo2);
    }
}

test! {
    async fn use_same_timestamp_for_whole_transaction(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let mut t = store.current().await;
        t.insert(vec![1], "foo".into())?;
        sleep(1).await;

        t.insert(vec![2], "bar".into())?;
        sleep(1).await;

        t.insert(vec![3], "baz".into())?;
        sleep(1).await;

        t.commit().await?;

        let current = store.current().await;
        let t1 = current.versions(&[1]).await?.last().unwrap().timestamp;
        let t2 = current.versions(&[2]).await?.last().unwrap().timestamp;
        let t3 = current.versions(&[3]).await?.last().unwrap().timestamp;

        assert!(t1 > 0);
        assert_eq!(t1, t2);
        assert_eq!(t2, t3);
    }
}

test! {
    async fn most_recent_timestamp(storage) -> Result<()> {
        let store = KvStore::open(storage).await?;
        let key1 = vec![1, 2, 3];
        let value1 = vec![6, 7];

        let current = store.current().await;
        assert_eq!(current.last_updated().await?, None);

        let mut t = store.current().await;
        t.insert(key1.clone(), value1.clone())?;
        assert_eq!(
            t.last_updated().await?.unwrap(),
            t.versions(&key1).await?.last().unwrap().timestamp
        );
        t.commit().await?;

        let current = store.current().await;
        let t1 = current.versions(&key1).await?.last().unwrap().timestamp;
        assert_eq!(current.last_updated().await?.unwrap(), t1);
    }
}

test! {
    async fn monotonically_increasing_timestamps(storage) -> Result<()> {
        let store_name = String::from(storage.name());
        let now_plus_10_minutes = timestamp_now_monotonic(0) + 600_000;
        insert_transaction_manually(&mut storage, now_plus_10_minutes).await?;

        sleep(100).await;

        let store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        assert!(t.last_updated().await?.unwrap() > timestamp_now_monotonic(0));
        assert_eq!(t.last_updated().await?.unwrap(), now_plus_10_minutes);
        t.insert(vec![5], vec![8])?;
        t.commit().await?;

        sleep(100).await;

        let t_after_transaction = timestamp_now_monotonic(0);
        let current = store.current().await;
        assert!(current.last_updated().await?.unwrap() > t_after_transaction);
        assert_eq!(current.last_updated().await?.unwrap(), now_plus_10_minutes);

        let storage = storage::open(&store_name).await?;
        let mut store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        assert_eq!(t.get(&[5]).await?.unwrap(), vec![8]);
        assert!(t.last_updated().await?.unwrap() > timestamp_now_monotonic(0));
        assert_eq!(t.last_updated().await?.unwrap(), now_plus_10_minutes);
        t.insert(vec![5], vec![9])?;
        t.commit().await?;

        sleep(100).await;

        store.merge().await?;
        let storage = storage::open(&store_name).await?;
        let store = KvStore::open(storage).await?;

        let t = store.current().await;
        assert_eq!(t.get(&[5]).await?.unwrap(), vec![9]);
        assert!(t.last_updated().await?.unwrap() > timestamp_now_monotonic(0));
        assert_eq!(t.last_updated().await?.unwrap(), now_plus_10_minutes);
        t.commit().await?;
    }
}

test! {
    async fn timestamped_snapshot_ordering(storage) -> Result<()> {
        // to ensure that all transactions "happen" at the same millisecond
        let now_plus_10_minutes = timestamp_now_monotonic(0) + 600_000;
        insert_transaction_manually(&mut storage, now_plus_10_minutes).await?;

        let store = KvStore::open(storage).await?;
        let snapshot = store.current().await;

        let mut t = store.current().await;
        t.insert(vec![5, 6, 7], vec![8, 9, 10])?;
        t.commit().await?;

        assert_eq!(snapshot.get(&[5, 6, 7]).await?, None);
        let snapshot = store.current().await;
        assert_eq!(snapshot.get(&[5, 6, 7]).await?.unwrap(), vec![8, 9, 10]);
    }
}

test! {
    async fn timestamped_transaction_isolation(storage) -> Result<()> {
        // to ensure that all transactions "happen" at the same millisecond
        let now_plus_10_minutes = timestamp_now_monotonic(0) + 600_000;
        insert_transaction_manually(&mut storage, now_plus_10_minutes).await?;

        let store = KvStore::open(storage).await?;

        let mut t = store.current().await;
        t.insert(vec![5, 6, 7], vec![8, 9, 10])?;

        {
            let mut t = store.current().await;
            assert_eq!(t.get(&[5, 6, 7]).await?, None);
            t.insert(vec![11, 12], vec![13, 14])?;
            t.commit().await?;
        }

        assert_eq!(t.get(&[11, 12]).await?, None);
        match t.commit().await {
            Err(Error::TransactionConflict) => {},
            instead => panic!("Expected a transaction conflict, but found {:?}", instead),
        };
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

const BYTES_TIMESTAMP: usize = 6;
const BYTES_CRC: usize = 4;

async fn insert_transaction_manually<S: Storage>(storage: &mut S, t: u64) -> storage::Result<()> {
    let mut buf_timestamp = [0; BYTES_TIMESTAMP];
    buf_timestamp.copy_from_slice(&t.to_le_bytes()[0..BYTES_TIMESTAMP]);

    let k = 1;
    let v = 2;
    let mut manual_transaction_with_timestamp_in_the_future = [
        0b0000_1001,           // header for kv write, bytes key size = 1, bytes val size = 1
        1,                     // key size
        1,                     // val size
        k,                     // key
        v,                     // value
        0b0000_0001,           // header for commit, bytes key size = 0, bytes val size = 1
        BYTES_TIMESTAMP as u8, // val size
        buf_timestamp[0],
        buf_timestamp[1],
        buf_timestamp[2],
        buf_timestamp[3],
        buf_timestamp[4],
        buf_timestamp[5],
        0, // for crc,
        0, // for crc,
        0, // for crc,
        0, // for crc,
    ];

    let mut buf_crc = [0; BYTES_CRC];
    let mut crc = Hasher::new();
    let len_without_crc = manual_transaction_with_timestamp_in_the_future.len() - BYTES_CRC;
    crc.update(&manual_transaction_with_timestamp_in_the_future[..len_without_crc]);
    buf_crc.copy_from_slice(&crc.finalize().to_le_bytes()[0..BYTES_CRC]);
    manual_transaction_with_timestamp_in_the_future[len_without_crc] = buf_crc[0];
    manual_transaction_with_timestamp_in_the_future[len_without_crc + 1] = buf_crc[1];
    manual_transaction_with_timestamp_in_the_future[len_without_crc + 2] = buf_crc[2];
    manual_transaction_with_timestamp_in_the_future[len_without_crc + 3] = buf_crc[3];

    storage
        .write(&manual_transaction_with_timestamp_in_the_future)
        .await?;
    storage.flush().await?;
    Ok(())
}

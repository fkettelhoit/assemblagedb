# Versioned & Transactional KV Store for Native & Wasm

AssemblageKV is a very simple embedded key-value store implemented in 100% safe
Rust as a log-structured hash table similar to
[Bitcask](https://riak.com/assets/bitcask-intro.pdf). Writes of new or changed
values never overwrite old entries, but are simply appended to the end of the
storage. Old values are kept at earlier offsets in the storage and remain
accessible. An in-memory hash table tracks the storage offsets of all keys and
allows efficient reads directly from the relevant portions of the storage. A
store can be merged, which discards old versions and builds a more compact
representation containing only the latest value of each key.

## Features

  - _simple_: log-structured hash architecture, with all keys in memory
  - _fully versioned:_ old values remain accessible until merged
  - _transactional:_ all reads and writes happen only in isolated transactions
  - _storage-independent:_ supports files on native and IndexedDB on wasm

## Obligatory Warning

AssemblageKV is a personal project that grew out of my need for a simple KV
store that could run on both native and wasm targets. It should go without
saying that it is not a battle-tested production-ready database and could at any
time eat all of your data. **If you need to persist production data, use a real
database such as Postgres, SQLite or perhaps look into
[sled](https://github.com/spacejam/sled) for a KV store in Rust.**

## Example

```rust
let store_name = storage.name().to_string();
let mut store = KvStore::open(storage).await?;
let slot = 0;

{
    let mut current = store.current().await;
    assert_eq!(current.get::<_, u8>(slot, &"key1").await?, None);
    current.insert(slot, &"key1", 1)?;
    current.commit().await?;
}

{
    let mut current = store.current().await;
    assert_eq!(current.get(slot, &"key1").await?, Some(1));
    current.remove(slot, &"key1")?;
    current.commit().await?;
}

{
    let mut current = store.current().await;
    assert_eq!(current.get::<_, u8>(slot, &"key1").await?, None);
    current.insert(slot, &"key1", 3)?;
    current.commit().await?;
}

{
    let current = store.current().await;
    let versions = current.versions(slot, &"key1").await?;
    assert_eq!(versions.len(), 3);
    assert_eq!(current.get_version(slot, &"key1", versions[0]).await?, Some(1));
    assert_eq!(current.get_version::<_, u8>(slot, &"key1", versions[1]).await?, None);
    assert_eq!(current.get_version(slot, &"key1", versions[2]).await?, Some(3));
}

store.merge().await?;
let storage = storage::open(&store_name).await?;
let store = KvStore::open(storage).await?;

{
    let current = store.current().await;
    let versions = current.versions(slot, &"key1").await?;
    assert_eq!(versions.len(), 1);
    assert_eq!(current.get(slot, &"key1").await?, Some(3));
}
```

use assemblage_index::{Db, data::ContentType};
use assemblage_kv::storage::PlatformStorage;
use std::future::Future;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const TEXT_CONTENT: ContentType = ContentType(0);

#[test]
fn index_text() {
    with_storage(file!(), line!(), |_| async {
        let foobar = Db::build_from(TEXT_CONTENT, "foobar".as_bytes()).await?;
        let barbaz = Db::build_from(TEXT_CONTENT, "babaqux".as_bytes()).await?;
        //let foobar = Db::build_from(TEXT_CONTENT, "foobarbaz".as_bytes()).await?;
        //let barbaz = Db::build_from(TEXT_CONTENT, "xybarqux".as_bytes()).await?;
        let mut foobar_snapshot = foobar.current().await;
        let barbaz_snapshot = barbaz.current().await;
        foobar_snapshot.print().await?;
        barbaz_snapshot.print().await?;
        println!("\n\n");
        foobar_snapshot.import(&barbaz_snapshot).await?;
        foobar_snapshot.print().await?;
        foobar_snapshot.check_consistency().await?;
        Ok(())
    })
}

/*#[test]
fn index_text() {
    with_storage(file!(), line!(), |_| async {
        let db = Db::build_from(TEXT_CONTENT, "foobarbar".as_bytes()).await?;
        let current = db.current().await;
        let matches = current.search(TEXT_CONTENT, "foo".as_bytes()).await?;
        assert_eq!(matches.len(), 1);
        Ok(())
    })
}*/

/*#[test]
fn import_text() {
    with_storage(file!(), line!(), |storage| async {
        let db = Db::open(storage).await?;

        let mut t = db.current().await;
        t.add(TEXT_CONTENT, "foobarbaz".as_bytes()).await?;
        t.commit().await?;
        Ok(())
    })
}*/

fn with_storage<T, Fut>(file: &str, line: u32, mut t: T)
where
    T: FnMut(PlatformStorage) -> Fut,
    Fut: Future<Output = assemblage_index::data::Result<()>>,
{
    let _ignored = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info)
        .try_init();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = std::path::Path::new(file)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap();
        let name = format!("{}_{}", file, line);
        assemblage_kv::storage::purge(&name)
            .await
            .expect("Could not purge storage before test");
        let storage = assemblage_kv::storage::open(&name)
            .await
            .expect("Could not open storage for test");

        let result = t(storage).await;
        assert!(result.is_ok());

        assemblage_kv::storage::purge(&name)
            .await
            .expect("Could not purge storage after test");
    })
}

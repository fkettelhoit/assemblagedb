use std::{collections::HashMap, convert::TryInto};

use assemblage_kv::{storage, storage::Result, storage::Storage, test};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

test! {
    async fn initial_state(s) -> Result<()> {
        let name = String::from(s.name());
        assert_eq!(s.len(), 0);
        s.flush().await?;

        let s = storage::open(&name).await?;
        assert_eq!(s.name(), name);
        assert_eq!(s.len(), 0);
    }
}

test! {
    async fn read_and_write(s) -> Result<()> {
        let name = String::from(s.name());
        let ten_kbytes = [0; 10240];
        s.write(&ten_kbytes).await?;
        let five_bytes = [5, 6, 7, 8, 9];
        s.write(&five_bytes).await?;
        assert_eq!(s.read(1000, 10).await?, vec![0; 10]);
        assert_eq!(s.read(10240, 5).await?, five_bytes.to_vec());
        s.truncate(10240 + 3).await?;
        assert_eq!(s.read(10240, 3).await?, five_bytes[..3].to_vec());
        assert_eq!(s.read(10243, 2).await?, vec![0, 0]);
        s.flush().await?;
        let mut s = storage::open(&name).await?;
        assert_eq!(s.read(10240, 3).await?, five_bytes[..3].to_vec());
        assert_eq!(s.read(10243, 2).await?, vec![0, 0]);
    }
}

test! {
    async fn read_and_write_large_values(s) -> Result<()> {
        let mut writes = HashMap::new();
        let chunk_size = 16usize.pow(4) + 1;
        for i in 0..10 {
            let mut chunk = vec![0u8; chunk_size];
            for (j, byte) in chunk.iter_mut().enumerate() {
                *byte = (i * j % 256).try_into().unwrap();
            }
            let len = s.len();
            let offset = s.write(&chunk.clone()).await?;
            assert_eq!(len, offset);
            writes.insert(len, chunk);
        }
        for (offset, chunk) in writes.iter() {
            let read = s.read(*offset, chunk_size.try_into().unwrap()).await?;
            assert_eq!(&read, chunk);
        }
    }
}

test! {
    async fn start_and_stop_merge(s) -> Result<()> {
        let ten_kbytes = [0; 10240];
        s.write(&ten_kbytes).await?;
        let five_bytes = [5, 6, 7, 8, 9];
        s.write(&five_bytes).await?;

        s.start_merge().await?;
        let three_bytes = [2, 3, 4];
        s.write(&three_bytes).await?;
        s.stop_merge().await?;

        assert_eq!(s.len(), 3);
        assert_eq!(s.read(0, 3).await?, three_bytes.to_vec());
    }
}

test! {
    async fn read_beyond_end_of_storage(s) -> Result<()> {
        let buf = s.read(0, 10).await?;
        assert_eq!(buf, vec![0; 10]);

        let five_bytes = [5, 6, 7, 8, 9];
        s.write(&five_bytes).await?;

        let buf = s.read(0, 10).await?;
        assert_eq!(buf, vec![5, 6, 7, 8, 9, 0, 0, 0, 0, 0]);

        s.truncate(2).await?;
        s.write(&[0, 0, 0]).await?;

        let buf = s.read(0, 10).await?;
        assert_eq!(buf, vec![5, 6, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
}

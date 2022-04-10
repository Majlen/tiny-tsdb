use anyhow::Result;
use rocksdb::{DBCompressionType, Options, DB};
use std::str;

pub struct RocksDB {
    db: DB,
}

impl RocksDB {
    pub fn new(path: &str) -> Result<RocksDB> {
        let mut options = Options::default();
        options.set_compression_type(DBCompressionType::Zstd);
        options.create_if_missing(true);
        let db = DB::open(&options, path)?;
        Ok(RocksDB { db: db })
    }
}

impl super::DB for RocksDB {
    fn put(&self, key: &str, val: &[u8]) -> Result<()> {
        self.db.put(key, val)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }
}

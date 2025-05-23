use crate::{Store, error::Error};

#[derive(Clone)]
pub struct Config {
    pub(crate) wal_path: String,
    pub(crate) segment_path: String,
    pub(crate) max_table_size: u64,
    pub(crate) max_wal_size: u64,
    pub(crate) max_tables_count: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            wal_path: "./wal".to_string(),
            segment_path: "./segment".to_string(),
            max_table_size: 128 * 1024 * 1024,
            max_wal_size: 64 * 1024 * 1024,
            max_tables_count: 10,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn wal_path(&mut self, wal_path: &str) -> &mut Self {
        self.wal_path = wal_path.to_string();
        self
    }
    pub fn segment_path(&mut self, segment_path: &str) -> &mut Self {
        self.segment_path = segment_path.to_string();
        self
    }
    pub fn max_table_size(&mut self, max_table_size: u64) -> &mut Self {
        self.max_table_size = max_table_size;
        self
    }
    pub fn max_wal_size(&mut self, max_wal_size: u64) -> &mut Self {
        self.max_wal_size = max_wal_size;
        self
    }
    pub fn max_tables_count(&mut self, max_tables_count: u64) -> &mut Self {
        self.max_tables_count = max_tables_count;
        self
    }
    pub fn wal_path_str(&self) -> &str {
        &self.wal_path
    }

    pub fn open_store(&self) -> Result<Store, Error> {
        let store = Store::reload(self)?;
        Ok(store)
    }
}

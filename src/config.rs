#[derive(Clone)]
pub struct Options {
    pub(crate) wal_path: String,
    pub(crate) segment_path: String,
    pub(crate) max_table_size: u64,
    pub(crate) max_wal_size: u64,
    pub(crate) max_tables_count: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            wal_path: "./wal".to_string(),
            segment_path: "./segment".to_string(),
            max_table_size: 128 * 1024 * 1024,
            max_wal_size: 64 * 1024 * 1024,
            max_tables_count: 10,
        }
    }
}

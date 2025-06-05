use crate::{entry::Entry, table::StreamTable};
use anyhow::Result;
use std::{
    collections::HashMap,
    io,
    sync::{self, Arc, Mutex, atomic::AtomicU64},
};

pub type MemTableArc = Arc<MemTable>;
pub type MemTableWeak = sync::Weak<MemTable>;
pub(crate) type GetStreamOffsetFn = Arc<Box<dyn Fn(u64) -> Result<u64> + Send + Sync>>;
pub struct MemTable {
    stream_tables: Mutex<HashMap<u64, StreamTable>>,
    first_entry: AtomicU64,
    last_entry: AtomicU64,
    size: AtomicU64,
    get_stream_offset_fn: Mutex<GetStreamOffsetFn>,
}

impl MemTable {
    pub fn new(get_stream_offset_fn: &GetStreamOffsetFn) -> Self {
        MemTable {
            stream_tables: Mutex::new(HashMap::new()),
            first_entry: AtomicU64::new(0),
            last_entry: AtomicU64::new(0),
            size: AtomicU64::new(0),
            get_stream_offset_fn: Mutex::new(get_stream_offset_fn.clone()),
        }
    }

    pub fn get_first_entry(&self) -> u64 {
        self.first_entry.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_last_entry(&self) -> u64 {
        self.last_entry.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_size(&self) -> u64 {
        self.size.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_stream_tables(&self) -> std::sync::MutexGuard<HashMap<u64, StreamTable>> {
        self.stream_tables.lock().unwrap()
    }

    pub fn get_stream_range(&self, stream_id: u64) -> Option<(u64, u64)> {
        let guard = self.stream_tables.lock().unwrap();
        if let Some(stream_table) = guard.get(&stream_id) {
            return stream_table.get_stream_range();
        }
        None
    }

    pub fn read_stream(&self, stream_id: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.stream_tables.lock().unwrap();
        if let Some(stream_table) = guard.get(&stream_id) {
            return stream_table.read_stream(offset, buf);
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Stream ID {} not found", stream_id),
        ))
    }

    // return the stream offset
    pub fn append(&self, entry: &Entry) -> Result<u64> {
        assert!(entry.stream_id != 0, "Stream ID cannot be zero");
        assert!(entry.data.len() > 0, "Entry data cannot be empty");
        assert!(entry.id > 0, "Entry ID must be greater than zero");
        assert!(
            entry.id > self.last_entry.load(std::sync::atomic::Ordering::SeqCst),
            "Entry ID must be greater than the last entry ID"
        );

        let data_len = entry.data.len() as u64;

        let mut guard = self.stream_tables.lock().unwrap();

        let res = match guard.get_mut(&entry.stream_id) {
            Some(stream_table) => stream_table,
            None => {
                let offset = match self.get_stream_offset_fn.lock().unwrap()(entry.stream_id) {
                    Ok(offset) => offset,
                    Err(e) => return Err(e),
                };
                guard.insert(entry.stream_id, StreamTable::new(entry.stream_id, offset));
                guard.get_mut(&entry.stream_id).unwrap()
            }
        };

        // Append the data to the stream table
        let offset = res.append(&entry.data)?;

        // Update the stream table
        self.size
            .fetch_add(data_len, std::sync::atomic::Ordering::SeqCst);

        self.last_entry
            .store(entry.id, std::sync::atomic::Ordering::SeqCst);

        if self.first_entry.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            self.first_entry
                .store(entry.id, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(offset)
    }
}

fn assert_send_sync<T: Send + Sync>() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_data() {
        assert_send_sync::<MemTable>();
    }
}

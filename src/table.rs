use core::{hash, sync};
use std::{
    collections::HashMap,
    sync::{Mutex, atomic::AtomicU64},
};

use crate::{error::Error, store::Entry};

const STREAM_DATA_BUFFER_CAP: u64 = 1024 * 128; // 128KB

pub struct StreamData {
    pub stream_id: u64,
    pub offset: u64,
    pub data: Vec<u8>,
}

pub struct StreamTable {
    pub stream_id: u64,
    pub offset: u64,
    pub size: u64,
    pub stream_datas: Vec<StreamData>,
}

type GetStreamOffsetHandler = Box<dyn Fn(u64) -> Result<u64, Error> + Send + Send>;

pub type MemTableArc = std::sync::Arc<MemTable>;
pub type MemTableWeak = std::sync::Weak<MemTable>;
pub struct MemTable {
    stream_tables: Mutex<HashMap<u64, StreamTable>>,
    first_entry: AtomicU64,
    last_entry: AtomicU64,
    size: AtomicU64,
    get_stream_offset: Mutex<Option<GetStreamOffsetHandler>>,
}

impl StreamData {
    pub fn new(stream_id: u64, offset: u64, buffer_cap: u64) -> Self {
        StreamData {
            stream_id,
            offset,
            data: Vec::with_capacity(buffer_cap as usize),
        }
    }

    // Fill the buffer with data
    // If the buffer is full, return the remaining data
    // If the buffer is not full, return None
    pub fn fill<'a>(&mut self, data: &'a [u8]) -> Result<(u64, Option<&'a [u8]>), Error> {
        let available = self.cap_remaining().min(data.len() as u64);
        self.data.extend_from_slice(&data[..available as usize]);

        let remaining_data = if available < data.len() as u64 {
            Some(&data[available as usize..])
        } else {
            None
        };

        Ok((available, remaining_data))
    }

    pub fn get_stream_range(&self) -> Option<(u64, u64)> {
        if self.data.is_empty() {
            return None;
        }
        let start = self.offset;
        let end = self.offset + self.data.len() as u64;
        Some((start, end))
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn cap_remaining(&self) -> u64 {
        self.data.capacity() as u64 - self.data.len() as u64
    }
}

impl StreamTable {
    pub fn new(stream_id: u64, offset: u64) -> Self {
        StreamTable {
            stream_id,
            offset: offset,
            size: 0,
            stream_datas: Vec::new(),
        }
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        if self.stream_datas.is_empty() || self.stream_datas.last().unwrap().cap_remaining() == 0 {
            self.stream_datas.push(StreamData::new(
                self.stream_id,
                self.offset + self.size,
                STREAM_DATA_BUFFER_CAP,
            ));
        }

        let stream_data = self.stream_datas.last_mut().unwrap();
        let (size, remain_buffer) = stream_data.fill(data)?;
        self.size += size;

        // If the buffer is full, we need to create a new buffer
        if let Some(buffer) = remain_buffer {
            return self.append(buffer);
        }

        Ok(())
    }

    pub fn get_stream_range(&self) -> Option<(u64, u64)> {
        if self.stream_datas.is_empty() {
            return None;
        }
        return Some((self.offset, self.offset + self.size));
    }

    pub fn read_stream_data(&self, offset: u64, size: u64) -> Result<Vec<u8>, Error> {
        let mut data = Vec::new();
        let mut offset = offset;
        let mut size = size;

        // find the first stream data that offset <= offset by quick search

        let res = self.stream_datas.binary_search_by(|stream_data| {
            if offset < stream_data.offset {
                std::cmp::Ordering::Greater
            } else if offset >= stream_data.offset + stream_data.size() {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });
        let mut index = match res {
            Ok(index) => index,
            Err(index) => index,
        };

        // read the data from the stream data
        while index < self.stream_datas.len() {
            let stream_data = &self.stream_datas[index];
            let stream_data_offset = stream_data.offset;
            let stream_data_size = stream_data.size();

            if offset >= stream_data_offset + stream_data_size {
                index += 1;
                continue;
            }

            let start = (offset - stream_data_offset) as usize;
            let end = (start + size as usize).min(stream_data_size as usize);
            data.extend_from_slice(&stream_data.data[start..end]);

            size -= (end - start) as u64;
            if size == 0 {
                break;
            }
            offset = stream_data_offset + stream_data_size;
        }

        Ok(data)
    }
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            stream_tables: Mutex::new(HashMap::new()),
            first_entry: AtomicU64::new(0),
            last_entry: AtomicU64::new(0),
            size: AtomicU64::new(0),
            get_stream_offset: Mutex::new(None),
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

    pub fn read_stream_data(
        &self,
        stream_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, Error> {
        let guard = self.stream_tables.lock().unwrap();
        if let Some(stream_table) = guard.get(&stream_id) {
            return stream_table.read_stream_data(offset, size);
        }
        Err(Error::StreamNotFound)
    }

    pub fn append(&self, entry: &Entry) -> Result<(), Error> {
        let data_len = entry.data.len() as u64;

        let mut guard = self.stream_tables.lock().unwrap();

        let res = guard.get_mut(&entry.stream_id);
        if res.is_none() {
            let offset = match self.get_stream_offset.lock().unwrap().as_ref() {
                Some(ref handler) => handler(entry.stream_id)?,
                None => 0,
            };
            guard
                .insert(entry.stream_id, StreamTable::new(entry.stream_id, offset))
                .unwrap();
        } else {
            // Append the data to the stream table
            res.unwrap().append(&entry.data)?;
        }

        // Update the stream table
        self.size
            .fetch_add(data_len, std::sync::atomic::Ordering::SeqCst);

        self.last_entry
            .store(entry.id, std::sync::atomic::Ordering::SeqCst);

        if self.first_entry.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            self.first_entry
                .store(entry.id, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
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

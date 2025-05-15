use core::hash;
use std::collections::HashMap;

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

type GetStreamOffsetHandler = Box<dyn Fn(u64) -> Result<u64, Error>>;

pub struct MemTable {
    pub stream_tables: HashMap<u64, StreamTable>,
    pub first_entry: u64,
    pub last_entry: u64,
    pub size: u64,
    pub get_stream_offset: Option<GetStreamOffsetHandler>,
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
    pub fn fill(&mut self, mut data: Vec<u8>) -> Result<(u64, Option<Vec<u8>>), Error> {
        let data_len = data.len() as u64;
        let mut data_remaining = data.len() as u64 - self.cap_remaining();
        if data_remaining < 0 {
            data_remaining = 0;
        }

        if data_remaining > 0 {
            let mut remain_buffer = Vec::with_capacity(data_remaining as usize);
            remain_buffer.extend_from_slice(&mut data[data_remaining as usize..]);
            self.data
                .extend_from_slice(&data[..data_remaining as usize]);
            return Ok((data_len - data_remaining, Some(remain_buffer)));
        } else {
            self.data.append(&mut data);
            return Ok((data_len, None));
        }
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

    pub fn append(&mut self, data: Vec<u8>) -> Result<(), Error> {
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
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            stream_tables: HashMap::new(),
            first_entry: 0,
            last_entry: 0,
            size: 0,
            get_stream_offset: None,
        }
    }

    pub fn append(&mut self, entry: Entry) -> Result<(), Error> {
        let data_len = entry.data.len() as u64;

        let res = self.stream_tables.get_mut(&entry.stream_id);
        if res.is_none() {
            let offset = match self.get_stream_offset {
                Some(ref handler) => handler(entry.stream_id)?,
                None => 0,
            };
            self.stream_tables
                .insert(entry.stream_id, StreamTable::new(entry.stream_id, offset))
                .unwrap();
        } else {
            // Append the data to the stream table
            res.unwrap().append(entry.data)?;
        }

        // Update the stream table
        self.size += data_len;
        self.last_entry += entry.id;
        if self.first_entry == 0 {
            self.first_entry = entry.id;
        }
        Ok(())
    }
}

use core::hash;
use std::collections::HashMap;

use crate::error::Error;

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

pub struct MemTable {
    pub stream_tables: HashMap<u64, StreamTable>,
    pub first_entry: u64,
    pub last_entry: u64,
    pub size: u64,
}

impl StreamData {
    pub fn new(stream_id: u64, offset: u64, buffer_cap: u64) -> Self {
        StreamData {
            stream_id,
            offset,
            data: Vec::with_capacity(buffer_cap as usize),
        }
    }

    pub fn fill(&mut self, offset: u64, mut data: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        if self.data.is_empty() {
            if self.offset != offset {
                return Err(Error::StreamOffsetInvalid);
            }
        };

        let mut data_remaining = data.len() as i64 - self.cap_remaining() as i64;
        if data_remaining < 0 {
            data_remaining = 0;
        }

        if data_remaining > 0 {
            let mut remain_buffer = Vec::with_capacity(data_remaining as usize);
            remain_buffer.extend_from_slice(&mut data[data_remaining as usize..]);
            self.data
                .extend_from_slice(&data[..data_remaining as usize]);
            return Ok(Some(remain_buffer));
        } else {
            self.data.append(&mut data);
            return Ok(None);
        }
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

    pub fn append(&mut self, offset: u64, data: Vec<u8>) -> Result<(), Error> {
        if self.stream_datas.is_empty() || self.stream_datas.last().unwrap().cap_remaining() == 0 {
            self.stream_datas.push(StreamData::new(
                self.stream_id,
                offset,
                STREAM_DATA_BUFFER_CAP,
            ));
        }

        let stream_data = self.stream_datas.last_mut().unwrap();
        let remain_buffer = stream_data.fill(offset, data)?;
        if let Some(buffer) = remain_buffer {
            return self.append(offset, buffer);
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
        }
    }

    pub fn append(&mut self, stream_id: u64, offset: u64, data: Vec<u8>) {}
}

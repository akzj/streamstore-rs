use anyhow::Result;

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
    pub fn fill<'a>(&mut self, data: &'a [u8]) -> Result<(u64, Option<&'a [u8]>)> {
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

    pub fn append(&mut self, data: &[u8]) -> Result<u64> {
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

        Ok(self.offset + self.size)
    }

    pub fn get_stream_range(&self) -> Option<(u64, u64)> {
        if self.stream_datas.is_empty() {
            return None;
        }
        return Some((self.offset, self.offset + self.size));
    }

    pub fn read_stream_data(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
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

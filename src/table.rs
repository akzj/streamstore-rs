use std::io;

use anyhow::Result;

const STREAM_DATA_BUFFER_CAP: u64 = 1024; // 128KB

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
            log::debug!(
                "Creating new StreamData for stream {} at offset {}",
                self.stream_id,
                self.offset + self.size
            );
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

    pub fn print_stream_meta(&self) {
        for (i, stream_data) in self.stream_datas.iter().enumerate() {
            log::debug!(
                "Stream Data {}: ID: {}, Offset: {}, Size: {}, Capacity: {}",
                i,
                stream_data.stream_id,
                stream_data.offset,
                stream_data.size(),
                stream_data.data.capacity()
            );
        }
    }

    pub fn read_stream(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.print_stream_meta();

        let mut offset = offset;
        let mut size = buf.len() as u64;
        let mut copied_size = 0;

        // find the first stream data that offset <= offset by quick search
        let res = self.stream_datas.binary_search_by(|stream_data| {
            if stream_data.offset < offset {
                std::cmp::Ordering::Less
            } else if stream_data.offset + stream_data.size() > offset {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        let mut index = match res {
            Ok(index) => index,
            Err(index) => index,
        };

        log::debug!(
            "Find Index stream {} from offset {} with size {} index {}",
            self.stream_id,
            offset,
            size,
            index
        );

        // read the data from the stream data
        while index < self.stream_datas.len() {
            let stream_data = &self.stream_datas[index];
            let stream_data_offset = stream_data.offset;
            let stream_data_size = stream_data.size();

            log::debug!(
                "Reading stream data {} at index {} with offset {} and size {} offset {}",
                stream_data.stream_id,
                index,
                stream_data_offset,
                stream_data_size,
                offset
            );

            if stream_data_offset + stream_data_size <= offset {
                log::debug!(
                    "Skip Reading stream data {} at index {} with offset {} and size {}",
                    stream_data.stream_id,
                    index,
                    stream_data_offset,
                    stream_data_size
                );
                index += 1;
                continue;
            }

            if offset >= stream_data_offset + stream_data_size {
                log::debug!(
                    "reach stream end {} at index {} with offset {} and size {}",
                    stream_data.stream_id,
                    index,
                    stream_data_offset,
                    stream_data_size
                );
                break;
            }

            if stream_data_offset <= offset && offset < stream_data_offset + stream_data_size {
                // we can read the data from this stream data
                log::debug!(
                    "Reading stream data {} at index {} with offset {} and size {} offset {}",
                    stream_data.stream_id,
                    index,
                    stream_data_offset,
                    stream_data_size,
                    offset
                );

                let start = (offset - stream_data_offset) as usize;
                let end = (start + size as usize).min(stream_data_size as usize);

                // copy the data to the buffer
                let data_to_copy = &stream_data.data[start..end];
                let bytes_to_copy = data_to_copy.len();
                if bytes_to_copy == 0 {
                    index += 1;
                    continue;
                }
                buf[copied_size as usize..(copied_size as u64 + bytes_to_copy as u64) as usize]
                    .copy_from_slice(data_to_copy);

                copied_size += bytes_to_copy;
                size -= (bytes_to_copy) as u64;
                if size == 0 {
                    break;
                }
                offset += bytes_to_copy as u64;
                index += 1;
            } else {
                // skip this stream data
                index += 1;
                continue;
            }
        }

        Ok(copied_size)
    }
}
